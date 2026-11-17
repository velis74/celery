"""Child process of the async pool.

Runs ``async def`` tasks on an :mod:`asyncio` event loop, many at a time.
The parent (:class:`celery.concurrency.aiopool.AsyncAsynPool`) writes jobs to
the inqueue and control messages to the synqueue of the process; the process
answers with the usual ``ACK`` and ``READY`` messages on its outqueue.

The tracer of each job runs in a thread of a thread pool.  It calls the task,
and the awaiter installed with :func:`celery.app.trace.set_task_awaiter`
runs the returned coroutine as an :class:`asyncio.Task` on the event loop and
waits for its result.
"""
import asyncio
import concurrent.futures
import contextvars
import errno
import os
import pickle
import queue
import signal
import sys
import threading
import traceback
from functools import partial
from struct import unpack_from
from time import monotonic

from billiard.compat import mem_rss
from billiard.einfo import ExceptionInfo
from billiard.exceptions import SoftTimeLimitExceeded, Terminated, TimeLimitExceeded
from billiard.pool import ACK, EX_OK, EX_RECYCLE, READY, TASK, MaybeEncodingError

from celery.app.trace import get_task_awaiter, set_task_awaiter
from celery.utils.log import get_logger

from .asynpool import Worker

__all__ = (
    'AsyncWorker', 'AsyncWorkloop', 'CANCEL', 'STOP', 'RECYCLE_REQUEST',
    'add_task_wrapper',
)

logger = get_logger(__name__)

#: Control message ``(CANCEL, job_id, reason, signum)`` sent by the parent.
CANCEL = 'CANCEL'

#: Cancel reasons.
CANCEL_SOFT = 'soft'
CANCEL_HARD = 'hard'
CANCEL_TERMINATE = 'terminate'

#: Control message ``(STOP,)``: finish the running jobs and exit.
STOP = 'STOP'

#: Message ``(RECYCLE_REQUEST, (pid, reason, detail))`` to the parent.
RECYCLE_REQUEST = 16

#: Smallest :setting:`worker_async_block_warning` threshold, in seconds.
MIN_BLOCK_WARNING = 0.04

#: Size of a single read from a pipe.
READ_SIZE = 65536

_STOP = object()

#: The job run by the tracer in the current thread.
_current_job = contextvars.ContextVar('celery.concurrency.aioworker.job')

#: Wrappers added with :func:`add_task_wrapper`.
_task_wrappers = []


def add_task_wrapper(wrapper):
    """Run each task coroutine of the async pool processes as ``wrapper(coroutine)``.

    ``wrapper`` returns an awaitable that awaits ``coroutine`` and returns
    its result.  It runs on the event loop, in the context of the task.  The
    wrapper added last is the outermost.  Wrappers added in the worker before
    the pool starts apply in all its async processes.
    """
    _task_wrappers.append(wrapper)


def _wrap(awaitable):
    for wrapper in _task_wrappers:
        awaitable = wrapper(awaitable)
    return awaitable


class _JobAborted(BaseException):
    """Raised in the tracer thread of a job the parent has already ended.

    Used for ``revoke(terminate=True)`` and the hard time limit.  Derives
    from :exc:`BaseException`, so the tracer does not store a state for the
    job; ``exc`` is the job's result sent to the parent.
    """

    def __init__(self, exc):
        super().__init__(exc)
        self.exc = exc


class FrameReader:
    """Reads length-prefixed pickled messages from a non-blocking pipe."""

    def __init__(self, fd, loads=pickle.loads):
        self.fd = fd
        self.loads = loads
        self._buf = bytearray()

    def read(self):
        """Return the messages that are complete.

        Raises:
            EOFError: when the writing end of the pipe is closed.
        """
        while True:
            try:
                data = os.read(self.fd, READ_SIZE)
            except BlockingIOError:
                break
            except InterruptedError:
                continue
            if not data:
                raise EOFError()
            self._buf += data
        return self._parse()

    def _parse(self):
        buf, messages = self._buf, []
        while len(buf) >= 4:
            size = unpack_from('>I', buf)[0]
            if len(buf) < 4 + size:
                break
            body = bytes(buf[4:4 + size])
            del buf[:4 + size]
            messages.append(self.loads(body))
        return messages


class OutqueueWriter(threading.Thread):
    """Writes messages to the outqueue from a single thread.

    The outqueue has no lock, and messages written by several threads at
    once would interleave.
    """

    def __init__(self, outq):
        super().__init__(name='celery-async-writer', daemon=True)
        self.outq = outq
        self._queue = queue.SimpleQueue()

    def put(self, message):
        self._queue.put(message)

    def stop(self, timeout=None):
        """Write the queued messages and stop."""
        self._queue.put(_STOP)
        if self.is_alive():
            self.join(timeout)

    def run(self):
        get = self._queue.get
        while True:
            message = get()
            if message is _STOP:
                return
            self.write(message)

    def write(self, message):
        try:
            self.outq.put(message)
        except Exception as exc:  # pylint: disable=broad-except
            if message[0] != READY:
                logger.error('Cannot send %r to the parent: %r',
                             message[0], exc, exc_info=True)
                return
            # The result cannot be pickled: report that instead.
            job, i, result, inqW_fd = message[1]
            _, _, tb = sys.exc_info()
            try:
                wrapped = MaybeEncodingError(exc, result[1])
                einfo = ExceptionInfo((MaybeEncodingError, wrapped, tb))
                self.outq.put((READY, (job, i, (False, einfo), inqW_fd)))
            except Exception:  # pylint: disable=broad-except
                logger.error('Cannot send result of job %r to the parent',
                             job, exc_info=True)
            finally:
                del tb


class LoopWatchdog(threading.Thread):
    """Logs a warning when the event loop does not run for a while."""

    #: Number of innermost frames of the loop thread in the warning.
    stack_limit = 20

    def __init__(self, workloop, threshold):
        super().__init__(name='celery-async-watchdog', daemon=True)
        self.workloop = workloop
        self.threshold = threshold
        self.interval = min(threshold / 4, 0.25)
        self._stopped = threading.Event()

    def stop(self):
        self._stopped.set()

    def run(self):
        warned = False
        while not self._stopped.wait(self.interval):
            lag = monotonic() - self.workloop.last_beat
            if lag <= self.threshold:
                warned = False
            elif not warned:
                warned = True
                self.warn(lag)

    def warn(self, lag):
        frame = sys._current_frames().get(self.workloop.loop_thread)
        stack = (''.join(traceback.format_stack(frame, self.stack_limit))
                 if frame else '')
        logger.warning(
            'The event loop of process %s has not run for %.2f seconds; '
            'a task blocks it.\nEvent loop thread stack:\n%s',
            self.workloop.pid, lag, stack,
        )


class _Job:
    """A job running in the process."""

    __slots__ = ('id', 'index', 'task', 'cancel_reason', 'signum')

    def __init__(self, job_id, index):
        self.id = job_id
        self.index = index
        self.task = None
        self.cancel_reason = None
        self.signum = None


class AsyncWorkloop:
    """Event loop of an async pool process."""

    #: Seconds between two runs of the heartbeat callback.
    heartbeat_interval = 0.1

    #: Seconds to wait for the writer thread when the loop ends abnormally.
    writer_stop_timeout = 1.0

    def __init__(self, worker, pid):
        self.worker = worker
        self.pid = pid
        self.capacity = worker.capacity
        self.inqW_fd = worker.inqW_fd
        self.synqW_fd = worker.synqW_fd
        self.jobs = {}
        self.accepting = True
        self.exitcode = EX_OK
        self.memory_limit = getattr(worker, 'memory_limit', None)
        self.memory_reported = False
        self.loop = None
        self.loop_thread = None
        self.executor = None
        self.last_beat = monotonic()
        self.writer = OutqueueWriter(worker.outq)
        self._readers = []

    def run(self):
        """Run jobs until the parent sends the shutdown sentinel."""
        loop = self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self.loop_thread = threading.get_ident()
        self.executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.capacity,
            thread_name_prefix='celery-async-tracer',
        )
        threshold = self.worker.block_warning
        watchdog = None
        if threshold:
            threshold = max(threshold, MIN_BLOCK_WARNING)
            # An idle loop must beat well within the threshold.
            self.heartbeat_interval = min(self.heartbeat_interval,
                                          threshold / 4)
            watchdog = LoopWatchdog(self, threshold)
        previous_awaiter = get_task_awaiter()
        set_task_awaiter(self.await_in_loop)
        self.writer.start()
        if watchdog is not None:
            watchdog.start()
        clean = False
        try:
            self._add_reader(self.worker.inq._reader.fileno(),
                             self.on_task_messages)
            if self.worker.synq is not None:
                self._add_reader(self.worker.synq._reader.fileno(),
                                 self.on_control_messages)
            loop.call_soon(self._heartbeat)
            loop.run_forever()
            while self.accepting or self.jobs:
                logger.error('A task stopped the event loop of process %s; '
                             'restarting it.', self.pid)
                loop.run_forever()
            clean = True
        finally:
            set_task_awaiter(previous_awaiter)
            if watchdog is not None:
                watchdog.stop()
            self._remove_readers()
            self.executor.shutdown(wait=clean, cancel_futures=not clean)
            self.writer.stop(None if clean else self.writer_stop_timeout)
            loop.close()
        return self.exitcode

    def _add_reader(self, fd, callback):
        os.set_blocking(fd, False)
        reader = FrameReader(fd)
        self.loop.add_reader(fd, self._on_readable, reader, callback)
        self._readers.append(fd)

    def _remove_reader(self, fd):
        if fd in self._readers:
            self._readers.remove(fd)
            self.loop.remove_reader(fd)

    def _remove_readers(self):
        for fd in list(self._readers):
            self._remove_reader(fd)

    def _on_readable(self, reader, callback):
        try:
            messages = reader.read()
        except EOFError:
            self._remove_reader(reader.fd)
            self.stop_accepting()
            return
        except OSError as exc:
            if exc.errno != errno.EBADF:
                raise
            self._remove_reader(reader.fd)
            self.stop_accepting()
            return
        if messages:
            callback(reader, messages)

    def _heartbeat(self):
        self.last_beat = monotonic()
        sentinel = self.worker._shutdown
        if sentinel is not None and sentinel.is_set():
            self.stop_accepting()
        self.loop.call_later(self.heartbeat_interval, self._heartbeat)

    def send(self, message):
        self.writer.put(message)

    def on_task_messages(self, reader, messages):
        for message in messages:
            if message is None:
                self._remove_reader(reader.fd)
                self.stop_accepting()
                continue
            type_, args = message
            if type_ == TASK:
                self.on_task(*args)
            else:
                logger.error('Unknown message from the parent: %r', type_)

    def on_control_messages(self, reader, messages):
        for message in messages:
            if message and message[0] == CANCEL:
                self.on_cancel(*message[1:])
            elif message and message[0] == STOP:
                self.exitcode = EX_RECYCLE
                self.stop_accepting()
            else:
                logger.error('Unknown control message from the parent: %r',
                             message)

    def stop_accepting(self):
        """Stop reading jobs, and stop the loop once the running jobs end."""
        if self.accepting:
            self.accepting = False
            self._remove_reader(self.worker.inq._reader.fileno())
        if not self.jobs:
            self.loop.stop()

    def on_task(self, job_id, index, fun, args, kwargs):
        entry = self.jobs[job_id] = _Job(job_id, index)
        self.send((ACK, (job_id, index, monotonic(), self.pid, self.synqW_fd)))
        # Each job starts with an empty context: the threads of the executor
        # run many jobs.
        self.executor.submit(
            contextvars.Context().run, self.run_job, entry, fun, args, kwargs,
        )

    def run_job(self, entry, fun, args, kwargs):
        """Run the tracer of a job (in a thread of the executor)."""
        _current_job.set(entry)
        try:
            result = (True, self.worker.prepare_result(fun(*args, **kwargs)))
        except _JobAborted as exc:
            result = _error_result(exc.exc)
        except BaseException:  # pylint: disable=broad-except
            result = (False, ExceptionInfo())
        self.send((READY, (entry.id, entry.index, result, self.inqW_fd)))
        try:
            self.loop.call_soon_threadsafe(self.on_job_done, entry)
        except RuntimeError:  # loop closed
            pass

    def on_job_done(self, entry):
        self.jobs.pop(entry.id, None)
        if self.memory_limit and not self.memory_reported:
            self.check_memory()
        if not self.accepting and not self.jobs:
            self.loop.stop()

    def check_memory(self):
        """Ask the parent for recycling when memory use exceeds the limit."""
        used_kb = mem_rss()
        if used_kb > self.memory_limit:
            self.memory_reported = True
            self.send((RECYCLE_REQUEST, (self.pid, 'max memory', used_kb)))

    def on_cancel(self, job_id, reason, signum=None):
        entry = self.jobs.get(job_id)
        if entry is None:
            return
        entry.cancel_reason = reason
        entry.signum = signum
        if entry.task is not None:
            entry.task.cancel()

    def await_in_loop(self, awaitable):
        """Task awaiter: run ``awaitable`` on the loop and return its result.

        Called in a tracer thread.  The :class:`asyncio.Task` runs in a copy
        of the context of the calling thread.
        """
        if threading.get_ident() == self.loop_thread:
            _close(awaitable)
            raise RuntimeError(
                'An async task cannot be run and waited for in the thread '
                'of the event loop: await its run() method instead.')
        entry = _current_job.get(None)
        done = concurrent.futures.Future()
        self.loop.call_soon_threadsafe(self._start, awaitable, entry, done)
        return done.result()

    def _start(self, awaitable, entry, done):
        if not done.set_running_or_notify_cancel():
            _close(awaitable)
            return
        if entry is not None and entry.cancel_reason is not None:
            _close(awaitable)
            done.set_exception(_cancel_exception(entry))
            return
        try:
            task = asyncio.ensure_future(_wrap(awaitable))
        except BaseException as exc:  # pylint: disable=broad-except
            done.set_exception(exc)
            return
        if entry is not None:
            entry.task = task
        task.add_done_callback(partial(self._finish, entry, done))

    @staticmethod
    def _finish(entry, done, task):
        if entry is not None:
            entry.task = None
        aborted = (entry is not None
                   and entry.cancel_reason in (CANCEL_HARD, CANCEL_TERMINATE))
        if task.cancelled() or aborted:
            # The parent has ended an aborted job: an outcome of a coroutine
            # that handled the cancellation is discarded.
            if not task.cancelled() and task.exception() is not None:
                logger.debug('Discarding %r of aborted job %r',
                             task.exception(), entry.id)
            done.set_exception(_cancel_exception(entry))
            return
        exc = task.exception()
        if exc is not None:
            done.set_exception(exc)
        else:
            done.set_result(task.result())


def _cancel_exception(entry):
    reason = entry.cancel_reason if entry is not None else None
    if reason == CANCEL_SOFT:
        return SoftTimeLimitExceeded()
    if reason == CANCEL_HARD:
        return _JobAborted(TimeLimitExceeded())
    if reason == CANCEL_TERMINATE:
        return _JobAborted(Terminated(entry.signum or 0))
    return concurrent.futures.CancelledError('The task coroutine was cancelled.')


def _error_result(exc):
    try:
        raise exc
    except BaseException:  # pylint: disable=broad-except
        return False, ExceptionInfo()


def _close(awaitable):
    close = getattr(awaitable, 'close', None)
    if close is not None:
        close()


class AsyncWorker(Worker):
    """Async pool process: runs up to ``capacity`` jobs at the same time."""

    Workloop = AsyncWorkloop

    def __init__(self, *args, capacity=1, block_warning=1.0,
                 memory_limit=None, **kwargs):
        self.capacity = capacity
        self.block_warning = block_warning
        self.memory_limit = memory_limit
        super().__init__(*args, **kwargs)

    def __reduce__(self):
        cls, args = super().__reduce__()
        return partial(cls, capacity=self.capacity,
                       block_warning=self.block_warning,
                       memory_limit=self.memory_limit), args

    def after_fork(self):
        super().after_fork()
        # The parent cancels the job instead of sending SIGUSR1.
        soft_timeout_signal = getattr(signal, 'SIGUSR1', None)
        if soft_timeout_signal is not None:
            signal.signal(soft_timeout_signal, signal.SIG_IGN)
        if self.synq is not None:
            self.synq._writer.close()

    def workloop(self, debug=None, now=None, pid=None):
        return self.Workloop(self, pid or os.getpid()).run()
