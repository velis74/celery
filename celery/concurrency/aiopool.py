"""Parent side of the async pool.

:class:`AsyncAsynPool` is an :class:`~celery.concurrency.asynpool.AsynPool`
whose processes (:class:`~celery.concurrency.aioworker.AsyncWorker`) each run
up to ``capacity`` jobs at the same time.  A process counts as busy only
when it runs ``capacity`` jobs.

Each process also has a control pipe (its synqueue), used to cancel a single
job: on a soft time limit and on ``revoke(terminate=True)``.

The semaphore given as ``semaphore`` holds one token per job, released when
the job is set ready, whatever ends it.
"""
import errno
import os
import pickle
from collections import deque
from functools import partial
from struct import pack

from billiard.pool import TimeoutHandler
from billiard.queues import _SimpleQueue

from celery.utils.log import get_logger

from .aioworker import CANCEL, CANCEL_SOFT, CANCEL_TERMINATE, AsyncWorker
from .asynpool import SCHED_STRATEGY_FAIR, AsynPool, ResultHandler

__all__ = ('AsyncAsynPool',)

logger = get_logger(__name__)


class AsyncResultHandler(ResultHandler):
    """Result handler that does not release the semaphore on ``READY``."""

    def _make_methods(self):
        # AsyncAsynPool releases a token when the job is set ready.
        self.putlock = None
        super()._make_methods()


class AsyncTimeoutHandler(TimeoutHandler):
    """Time limits of jobs running in async pool processes."""

    #: Set by :class:`AsyncAsynPool`.
    pool = None

    def on_soft_timeout(self, job):
        proc = self.pool.process_of(job._job) if self.pool else None
        if proc is None:
            return
        job.handle_timeout(soft=True)
        self.pool.send_cancel(proc, job._job, CANCEL_SOFT)


class AsyncAsynPool(AsynPool):
    """Pool of processes running ``async def`` tasks.

    Arguments:
        capacity (int): Number of jobs each process runs at the same time.
        block_warning (float): Seconds after which a process logs that its
            event loop is blocked; 0 disables it.
    """

    ResultHandler = AsyncResultHandler
    TimeoutHandler = AsyncTimeoutHandler

    _hub = None

    def __init__(self, processes=1, capacity=1, block_warning=1.0,
                 *args, **kwargs):
        self.capacity = capacity
        self.Worker = partial(AsyncWorker, capacity=capacity,
                              block_warning=block_warning)
        # job id -> process the job was written to
        self._job_procs = {}
        # ids of the jobs holding a semaphore token
        self._tokens = set()
        # process -> deque of control messages waiting to be written
        self._control_backlog = {}
        # The processes stop after these limits themselves only when they
        # run one job at a time.
        self.max_tasks_per_child = kwargs.pop('maxtasksperchild', None)
        self.max_memory_per_child = kwargs.pop('max_memory_per_child', None)
        kwargs['sched_strategy'] = SCHED_STRATEGY_FAIR
        super().__init__(processes, *args, **kwargs)
        if self._timeout_handler is not None:
            self._timeout_handler.pool = self

    def WorkerProcess(self, worker):
        proc = super().WorkerProcess(worker)
        proc._aio_jobs = set()
        return proc

    def create_process_queues(self):
        inq, outq, _ = super().create_process_queues()
        # Control messages from the parent.
        synq = _SimpleQueue(wnonblock=True)
        return inq, outq, synq

    def register_with_event_loop(self, hub):
        self._hub = hub
        super().register_with_event_loop(hub)

    def apply_async(self, *args, **kwargs):
        result = super().apply_async(*args, **kwargs)
        if result is not None:
            self._tokens.add(result._job)
        return result

    def _release_token(self, job_id):
        try:
            self._tokens.remove(job_id)
        except KeyError:
            return
        if self._putlock is not None:
            self._putlock.release()

    def _create_timelimit_handlers(self, hub):
        super()._create_timelimit_handlers(hub)
        on_timeout_cancel = self.on_timeout_cancel

        def on_job_set(R):
            # Called by ApplyResult._set() for every job set ready.
            on_timeout_cancel(R)
            self._release_token(R._job)
        self.on_timeout_cancel = on_job_set

    def _create_process_handlers(self, hub):
        super()._create_process_handlers(hub)
        on_process_down = self.on_process_down

        def _on_process_down(proc):
            self._forget_process(proc)
            return on_process_down(proc)
        self.on_process_down = _on_process_down

    def _maintain_pool(self):
        # Tokens are released per job (see _create_timelimit_handlers),
        # not per exited process.
        joined = self._join_exited_workers()
        self._repopulate_pool(joined)

    def process_of(self, job_id):
        """Return the process running the job, or :const:`None`."""
        return self._job_procs.get(job_id)

    def owns_pid(self, pid):
        return any(proc.pid == pid for proc in self._pool)

    def order_ready_fds(self, fds):
        """Sort writable inqueue fds by the load of their processes.

        The write scheduler gives a job to the first fd that is not busy.
        """
        fileno_to_inq = self._fileno_to_inq

        def load(fd):
            proc = fileno_to_inq.get(fd)
            return len(proc._aio_jobs) if proc is not None else 0
        return sorted(fds, key=load)

    def in_flight(self, proc):
        """Number of jobs written to ``proc`` and not ready yet."""
        return len(proc._aio_jobs)

    def _on_job_scheduled(self, proc, fd, job):
        job_id = job._job
        previous = self._job_procs.get(job_id)
        if previous is not None and previous is not proc:
            previous._aio_jobs.discard(job_id)
            self._update_busy(previous)
        self._job_procs[job_id] = proc
        proc._aio_jobs.add(job_id)
        if len(proc._aio_jobs) >= self.capacity:
            self._busy_workers.add(fd)

    def on_job_ready(self, job, i, obj, inqW_fd):
        proc = self._job_procs.pop(job, None)
        if proc is not None:
            proc._aio_jobs.discard(job)
            self._update_busy(proc)

    def _update_busy(self, proc):
        fd = proc.inqW_fd
        if self._fileno_to_inq.get(fd) is not proc:
            return
        if len(proc._aio_jobs) >= self.capacity:
            self._busy_workers.add(fd)
        else:
            self._busy_workers.discard(fd)

    def _forget_process(self, proc):
        for job_id in proc._aio_jobs:
            if self._job_procs.get(job_id) is proc:
                del self._job_procs[job_id]
        proc._aio_jobs.clear()
        if self._control_backlog.pop(proc, None) is not None:
            self._hub.remove_writer(proc.synqW_fd)

    def flush(self):
        # Jobs whose write has not started are discarded by flush and never
        # reach the process.
        unstarted = {
            job_id for job_id in self._job_procs
            if getattr(self._cache.get(job_id), '_write_to', True) is None
        }
        super().flush()
        for job_id in unstarted:
            if job_id not in self._cache:
                proc = self._job_procs.pop(job_id, None)
                if proc is not None:
                    proc._aio_jobs.discard(job_id)
        # Consumer.on_close() has reset the semaphore.
        self._tokens.intersection_update(self._cache)
        for proc in self._pool:
            self._update_busy(proc)

    def terminate_job(self, pid, sig=None):
        """Cancel the jobs of process ``pid`` that are marked as terminated.

        ``Request.terminate()`` marks its job with ``ApplyResult.terminate()``
        right after this call, so the cancel is sent from the event loop.
        """
        proc, _ = self._process_by_pid(pid)
        if proc is None:
            return
        if self._hub is None:
            self._cancel_terminated(proc)
        else:
            self._hub.call_soon(self._cancel_terminated, proc)

    def _cancel_terminated(self, proc):
        for job_id in tuple(proc._aio_jobs):
            job = self._cache.get(job_id)
            if (job is None or job.ready() or job._terminated is None or
                    getattr(job, '_aio_cancel_sent', False)):
                continue
            job._aio_cancel_sent = True
            self.send_cancel(proc, job_id, CANCEL_TERMINATE, job._terminated)

    def send_cancel(self, proc, job_id, reason, signum=None):
        """Ask ``proc`` to cancel the job."""
        self.send_control(proc, (CANCEL, job_id, reason, signum))

    def send_control(self, proc, message):
        """Write a control message to the synqueue of ``proc``."""
        if proc.synq is None or not proc._is_alive():
            return
        body = pickle.dumps(message, protocol=pickle.HIGHEST_PROTOCOL)
        data = pack('>I', len(body)) + body
        backlog = self._control_backlog.get(proc)
        if backlog is not None:
            backlog.append(data)
            return
        backlog = deque([data])
        if self._write_control(proc, backlog):
            return
        self._control_backlog[proc] = backlog
        self._hub.add_writer(proc.synqW_fd, self._flush_control, proc)

    def _write_control(self, proc, backlog):
        """Write ``backlog``; return true when nothing is left."""
        while backlog:
            data = backlog[0]
            try:
                n = os.write(proc.synqW_fd, data)
            except BlockingIOError:
                return False
            except InterruptedError:
                continue
            except OSError as exc:
                if exc.errno not in (errno.EPIPE, errno.EBADF):
                    raise
                backlog.clear()  # process gone
                return True
            if n < len(data):
                backlog[0] = data[n:]
                return False
            backlog.popleft()
        return True

    def _flush_control(self, proc):
        backlog = self._control_backlog.get(proc)
        if backlog is None or self._write_control(proc, backlog):
            self._control_backlog.pop(proc, None)
            self._hub.remove_writer(proc.synqW_fd)

    def process_info(self):
        """In-flight jobs of each process."""
        return [
            {'pid': proc.pid, 'in-flight': len(proc._aio_jobs)}
            for proc in self._pool
        ]
