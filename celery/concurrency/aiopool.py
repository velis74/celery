"""Parent side of the async pool.

:class:`AsyncAsynPool` is an :class:`~celery.concurrency.asynpool.AsynPool`
whose processes (:class:`~celery.concurrency.aioworker.AsyncWorker`) each run
up to ``capacity`` jobs at the same time.  A process counts as busy only
when it runs ``capacity`` jobs.

Each process also has a control pipe (its synqueue), used to cancel a single
job (soft and hard time limits, ``revoke(terminate=True)``) and to stop a
process that is recycled.

A process is recycled when one of its jobs exceeds the hard time limit, once
it has been given ``max_tasks_per_child`` jobs, and when it reports that its
memory use exceeds ``max_memory_per_child``.  A recycled process gets no new
jobs, and a replacement starts at once.  The process exits when its jobs are done; it is
killed when only jobs over the hard time limit remain, or ``recycle_timeout``
seconds after the recycling started.

The semaphore given as ``semaphore`` holds one token per job, released when
the job is set ready, whatever ends it.
"""
import errno
import os
import pickle
import signal
from collections import deque
from functools import partial
from struct import pack
from weakref import ref

from billiard.einfo import ExceptionInfo
from billiard.exceptions import TimeLimitExceeded
from billiard.pool import RUN, TimeoutHandler
from billiard.queues import _SimpleQueue

from celery.utils.log import get_logger

from .aioworker import CANCEL, CANCEL_HARD, CANCEL_SOFT, CANCEL_TERMINATE, RECYCLE_REQUEST, STOP, AsyncWorker
from .asynpool import SCHED_STRATEGY_FAIR, AsynPool, ResultHandler

__all__ = ('AsyncAsynPool',)

logger = get_logger(__name__)


#: Reasons for recycling a process.
RECYCLE_TIME_LIMIT = 'time limit'
RECYCLE_MAX_TASKS = 'max tasks'
RECYCLE_MAX_MEMORY = 'max memory'


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

    def on_hard_timeout(self, job):
        if job.ready():
            return
        # Reported as by billiard, without killing the process.
        try:
            raise TimeLimitExceeded(job._timeout)
        except TimeLimitExceeded:
            job._set(job._job, (False, ExceptionInfo()))
        job.handle_timeout(soft=False)
        if self.pool is not None:
            self.pool.on_job_overdue(job._job)


class AsyncAsynPool(AsynPool):
    """Pool of processes running ``async def`` tasks.

    Arguments:
        capacity (int): Number of jobs each process runs at the same time.
        block_warning (float): Seconds after which a process logs that its
            event loop is blocked; 0 disables it.
        recycle_timeout (float): Seconds a recycled process may take to
            finish its jobs before it is killed; 0 or :const:`None` waits
            until only jobs over the hard time limit remain.
        maxtasksperchild (int): Number of jobs a process gets before it is
            recycled.
        max_memory_per_child (int): Resident memory in KiB above which a
            process is recycled.
    """

    ResultHandler = AsyncResultHandler
    TimeoutHandler = AsyncTimeoutHandler

    #: Seconds a recycled process with only jobs over the hard time limit
    #: left gets to finish their cancellation before it is killed.
    overdue_grace = 1.0

    #: Seconds between attempts to reap a process whose sentinel closed,
    #: and the number of attempts.
    reap_interval = 0.05
    reap_attempts = 20

    _hub = None

    def __init__(self, processes=1, capacity=1, block_warning=1.0,
                 recycle_timeout=30.0, *args, **kwargs):
        self.capacity = capacity
        self.recycle_timeout = recycle_timeout
        # job id -> process the job was written to
        self._job_procs = {}
        # ids of the jobs holding a semaphore token
        self._tokens = set()
        # process -> deque of control messages waiting to be written
        self._control_backlog = {}
        # exit codes of recycled processes joined by _join_exited_workers
        self._recycled_exitcodes = []
        # The parent enforces both limits: the processes run many jobs at
        # the same time and never exit on their own.
        self.max_tasks_per_child = kwargs.pop('maxtasksperchild', None)
        self.max_memory_per_child = kwargs.pop('max_memory_per_child', None)
        self.Worker = partial(AsyncWorker, capacity=capacity,
                              block_warning=block_warning,
                              memory_limit=self.max_memory_per_child)
        kwargs['sched_strategy'] = SCHED_STRATEGY_FAIR
        super().__init__(processes, *args, **kwargs)
        if self._timeout_handler is not None:
            self._timeout_handler.pool = self

    def WorkerProcess(self, worker):
        proc = super().WorkerProcess(worker)
        proc._aio_jobs = set()
        proc._aio_overdue = set()
        proc._aio_assigned = 0
        proc._aio_done = 0
        proc._aio_recycling = None
        proc._aio_replaced = False
        proc._aio_stop_sent = False
        proc._aio_kill_armed = False
        proc._aio_timers = []
        return proc

    def create_result_handler(self):
        handler = super().create_result_handler()
        handler.state_handlers[RECYCLE_REQUEST] = self.on_recycle_request
        return handler

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

    def _event_process_exit(self, hub, proc):
        super()._event_process_exit(hub, proc)
        if proc in self._pool:
            # The sentinel closes before the process can be reaped.
            hub.call_later(self.reap_interval, self._reap, hub, ref(proc), 1)

    def _reap(self, hub, proc_ref, attempt):
        proc = proc_ref()
        if proc is None or proc not in self._pool or self._state != RUN:
            return
        self.maintain_pool()
        if proc in self._pool and attempt < self.reap_attempts:
            hub.call_later(self.reap_interval, self._reap, hub, proc_ref,
                           attempt + 1)

    def _maintain_pool(self):
        # Tokens are released per job (see _create_timelimit_handlers),
        # not per exited process.
        joined = self._join_exited_workers()
        self._repopulate_pool(joined)

    def _join_exited_workers(self, shutdown=False):
        """Join exited processes; return the exit codes of those not recycled.

        Recycled processes already have a replacement, and their exits do
        not count toward the restart frequency limit.
        """
        self._recycled_exitcodes = []
        exitcodes = super()._join_exited_workers(shutdown=shutdown)
        for exitcode in self._recycled_exitcodes:
            try:
                exitcodes.remove(exitcode)
            except ValueError:
                pass
        self._recycled_exitcodes = []
        return exitcodes

    def on_process_alive(self, pid):
        super().on_process_alive(pid)
        proc, _ = self._process_by_pid(pid)
        if proc is not None and getattr(proc, '_aio_recycling', None):
            self._all_inqueues.discard(proc.inqW_fd)

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
            self._maybe_stop(previous)
        self._job_procs[job_id] = proc
        proc._aio_jobs.add(job_id)
        proc._aio_assigned += 1
        if (self.max_tasks_per_child and
                proc._aio_assigned >= self.max_tasks_per_child):
            self.recycle(proc, RECYCLE_MAX_TASKS)
        elif len(proc._aio_jobs) >= self.capacity:
            self._busy_workers.add(fd)

    def on_job_ready(self, job, i, obj, inqW_fd):
        proc = self._job_procs.pop(job, None)
        if proc is None:
            return
        proc._aio_jobs.discard(job)
        proc._aio_overdue.discard(job)
        proc._aio_done += 1
        self._update_busy(proc)
        self._maybe_stop(proc)

    def _update_busy(self, proc):
        fd = proc.inqW_fd
        if self._fileno_to_inq.get(fd) is not proc:
            return
        if proc._aio_recycling:
            # Not in _all_inqueues: a busy fd outside it stops all writes.
            self._busy_workers.discard(fd)
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
        proc._aio_overdue.clear()
        for tref in proc._aio_timers:
            tref.cancel()
        proc._aio_timers = []
        if self._control_backlog.pop(proc, None) is not None:
            self._hub.remove_writer(proc.synqW_fd)
        if proc._aio_recycling:
            self._recycled_exitcodes.append(proc.exitcode)
            if proc._aio_replaced:
                proc._aio_replaced = False
                self._processes -= 1
        # Control messages the process did not read.
        if proc.synq is not None:
            _drain(proc.synq._reader)

    def on_job_overdue(self, job_id):
        """Cancel a job over its hard time limit and recycle its process."""
        proc = self.process_of(job_id)
        if proc is None:
            return
        proc._aio_overdue.add(job_id)
        self.send_cancel(proc, job_id, CANCEL_HARD)
        self.recycle(proc, RECYCLE_TIME_LIMIT)
        self._maybe_stop(proc)

    def on_recycle_request(self, pid, reason, detail=None):
        """Handle a request of process ``pid`` to be recycled."""
        proc, _ = self._process_by_pid(pid)
        if proc is None:
            return
        if reason == RECYCLE_MAX_MEMORY:
            logger.warning(
                'Async pool process %s exceeds the memory limit '
                '(%sKiB / %sKiB) and is recycled',
                pid, detail, self.max_memory_per_child)
        self.recycle(proc, reason)
        self._maybe_stop(proc)

    def recycle(self, proc, reason):
        """Stop giving jobs to ``proc`` and start a replacement for it.

        ``proc`` exits once its jobs are done (see :meth:`_maybe_stop`).
        """
        if proc._aio_recycling or proc not in self._pool:
            return
        proc._aio_recycling = reason
        logger.info('Recycling async pool process %s (%s), %d job(s) left',
                    proc.pid, reason, len(proc._aio_jobs))
        fd = proc.inqW_fd
        if self._fileno_to_inq.get(fd) is proc:
            self._all_inqueues.discard(fd)
            self._busy_workers.discard(fd)
        if self.recycle_timeout and self._hub is not None:
            proc._aio_timers.append(self._hub.call_later(
                self.recycle_timeout, self._on_recycle_timeout, ref(proc),
            ))
        if self._state == RUN:
            # One process more until the recycled one exits.
            self._processes += 1
            proc._aio_replaced = True
            self.on_grow(1)
            self._create_worker_process(self._avail_index())

    def _maybe_stop(self, proc):
        """Stop or kill a recycled process whose remaining jobs allow it."""
        if not proc._aio_recycling:
            return
        jobs = proc._aio_jobs
        if not jobs:
            if not proc._aio_stop_sent:
                proc._aio_stop_sent = True
                self.send_control(proc, (STOP,))
        elif jobs <= proc._aio_overdue:
            if self._hub is None or self._state != RUN:
                self._kill(proc, 'only jobs over the time limit remain')
            elif not proc._aio_kill_armed:
                proc._aio_kill_armed = True
                proc._aio_timers.append(self._hub.call_later(
                    self.overdue_grace, self._on_overdue_grace, ref(proc),
                ))

    def _on_overdue_grace(self, proc_ref):
        proc = proc_ref()
        if proc is None:
            return
        proc._aio_kill_armed = False
        jobs = proc._aio_jobs
        if jobs and jobs <= proc._aio_overdue:
            self._kill(proc, 'only jobs over the time limit remain')

    def _on_recycle_timeout(self, proc_ref):
        proc = proc_ref()
        if proc is None or proc not in self._pool:
            return
        self._kill(proc, f'its jobs did not end within {self.recycle_timeout}s')

    def _kill(self, proc, reason):
        if proc not in self._pool or not proc._is_alive():
            return
        logger.warning('Killing recycled async pool process %s: %s; '
                       '%d job(s) within the time limit are lost',
                       proc.pid, reason,
                       len(proc._aio_jobs - proc._aio_overdue))
        proc._controlled_termination = True
        try:
            os.kill(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass

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
            self._maybe_stop(proc)

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
            {'pid': proc.pid, 'in-flight': len(proc._aio_jobs),
             'done': proc._aio_done, 'recycling': proc._aio_recycling}
            for proc in self._pool
        ]


def _drain(conn):
    """Read and drop what is buffered in the pipe of ``conn``."""
    try:
        fd = conn.fileno()
        blocking = os.get_blocking(fd)
        os.set_blocking(fd, False)
    except (OSError, ValueError):
        return
    try:
        while os.read(fd, 65536):
            pass
    except OSError:
        pass
    finally:
        try:
            os.set_blocking(fd, blocking)
        except OSError:
            pass
