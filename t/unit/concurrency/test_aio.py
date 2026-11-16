import asyncio
import os
import pickle
import signal
import threading
import time
from struct import pack
from unittest.mock import Mock, patch

import pytest
from billiard.exceptions import SoftTimeLimitExceeded, Terminated, TimeLimitExceeded
from billiard.pool import ACK, EX_OK, READY, TASK, ApplyResult, MaybeEncodingError
from kombu.asynchronous import Hub
from kombu.asynchronous.semaphore import LaxBoundedSemaphore

import t.skip
from celery.app.trace import get_task_awaiter, set_task_awaiter

pytestmark = t.skip.if_win32

aio = pytest.importorskip('celery.concurrency.aio')
aiopool = pytest.importorskip('celery.concurrency.aiopool')
aioworker = pytest.importorskip('celery.concurrency.aioworker')


def frame(message):
    body = pickle.dumps(message)
    return pack('>I', len(body)) + body


class Pipe:

    def __init__(self):
        self.r, self.w = os.pipe()

    def close(self):
        for fd in (self.r, self.w):
            try:
                os.close(fd)
            except OSError:
                pass


class RecordingQueue:

    def __init__(self):
        self.messages = []
        self.cond = threading.Condition()

    def put(self, message):
        with self.cond:
            self.messages.append(message)
            self.cond.notify_all()

    def wait_for(self, predicate, timeout=5):
        with self.cond:
            assert self.cond.wait_for(lambda: predicate(self.messages), timeout)


JOBS = {}


def run_registered(job_id):
    fun, args = JOBS.pop(job_id)
    return fun(*args)


class FakeWorker:
    """Attributes of AsyncWorker used by AsyncWorkloop."""

    def __init__(self, capacity=4, block_warning=0):
        self.inq_pipe = Pipe()
        self.synq_pipe = Pipe()
        self.inq = Mock(_reader=Mock(fileno=Mock(return_value=self.inq_pipe.r)))
        self.synq = Mock(_reader=Mock(fileno=Mock(return_value=self.synq_pipe.r)))
        self.outq = RecordingQueue()
        self.capacity = capacity
        self.block_warning = block_warning
        self.inqW_fd = 101
        self.synqW_fd = 102
        self._shutdown = None

    def prepare_result(self, result):
        return result

    def send_task(self, job_id, fun, *args):
        # The job function and its arguments stay in this process.
        JOBS[job_id] = (fun, args)
        os.write(self.inq_pipe.w,
                 frame((TASK, (job_id, None, run_registered, (job_id,), {}))))

    def send_control(self, *message):
        os.write(self.synq_pipe.w, frame(message))

    def send_sentinel(self):
        os.write(self.inq_pipe.w, frame(None))

    def close(self):
        self.inq_pipe.close()
        self.synq_pipe.close()


def run_awaited(coro_fun, *args):
    """Job function: call the task awaiter as the tracer does."""
    return get_task_awaiter()(coro_fun(*args))


async def sleep_and_return(seconds, value):
    await asyncio.sleep(seconds)
    return value


async def wait_forever():
    await asyncio.sleep(60)


async def raise_value_error():
    raise ValueError('boom')


def raise_key_error():
    raise KeyError('outside the coroutine')


def readies(messages):
    # Pickled as on the way to the parent.
    return {m[1][0]: pickle.loads(pickle.dumps(m[1][2]))
            for m in messages if m[0] == READY}


@pytest.fixture
def worker():
    w = FakeWorker()
    yield w
    w.close()


def start_loop(w):
    workloop = aioworker.AsyncWorkloop(w, pid=1234)
    result = {}
    thread = threading.Thread(
        target=lambda: result.setdefault('exit', workloop.run()), daemon=True)
    thread.start()
    return workloop, thread, result


class test_FrameReader:

    def test_read_messages_split_across_writes(self):
        pipe = Pipe()
        try:
            os.set_blocking(pipe.r, False)
            reader = aioworker.FrameReader(pipe.r)
            data = frame(('A', 1)) + frame(('B', 'x' * 100))
            os.write(pipe.w, data[:7])
            assert reader.read() == []
            os.write(pipe.w, data[7:])
            assert reader.read() == [('A', 1), ('B', 'x' * 100)]
            assert reader.read() == []
            os.close(pipe.w)
            with pytest.raises(EOFError):
                reader.read()
        finally:
            pipe.close()


class test_OutqueueWriter:

    def test_writes_in_order_and_stops(self):
        outq = RecordingQueue()
        writer = aioworker.OutqueueWriter(outq)
        writer.start()
        for i in range(10):
            writer.put((ACK, (i,)))
        writer.stop()
        assert [m[1][0] for m in outq.messages] == list(range(10))
        assert not writer.is_alive()

    def test_unpicklable_result_is_reported(self):
        outq = Mock()
        outq.put.side_effect = [TypeError('cannot pickle'), None]
        writer = aioworker.OutqueueWriter(outq)
        writer.write((READY, (1, None, (True, object()), 5)))
        message = outq.put.call_args[0][0]
        assert message[0] == READY
        job, i, (ok, einfo), fd = message[1]
        assert (job, ok, fd) == (1, False, 5)
        assert einfo.type is MaybeEncodingError

    def test_other_errors_are_logged(self):
        outq = Mock()
        outq.put.side_effect = OSError('gone')
        with patch.object(aioworker, 'logger') as logger:
            aioworker.OutqueueWriter(outq).write((ACK, (1,)))
        logger.error.assert_called_once()


class test_AsyncWorkloop:

    def test_runs_jobs_concurrently(self, worker):
        workloop, thread, result = start_loop(worker)
        t0 = time.monotonic()
        for job_id in range(4):
            worker.send_task(job_id, run_awaited, sleep_and_return, 0.3, job_id)
        worker.send_sentinel()
        thread.join(5)
        assert not thread.is_alive()
        assert time.monotonic() - t0 < 1.0
        assert result['exit'] == EX_OK
        messages = worker.outq.messages
        assert readies(messages) == {i: (True, i) for i in range(4)}
        for job_id in range(4):
            kinds = [m[0] for m in messages if m[1][0] == job_id]
            assert kinds == [ACK, READY]
        ack = next(m for m in messages if m[0] == ACK)
        assert ack[1][3:] == (1234, 102)
        assert next(m for m in messages if m[0] == READY)[1][3] == 101
        assert get_task_awaiter() is None

    def test_job_exceptions(self, worker):
        workloop, thread, _ = start_loop(worker)
        worker.send_task(1, run_awaited, raise_value_error)
        worker.send_task(2, raise_key_error)
        worker.send_sentinel()
        thread.join(5)
        results = readies(worker.outq.messages)
        assert results[1][0] is False
        assert isinstance(results[1][1].exception, ValueError)
        assert results[2][0] is False
        assert isinstance(results[2][1].exception, KeyError)

    @pytest.mark.parametrize('reason,exc', [
        ('soft', SoftTimeLimitExceeded),
        ('hard', TimeLimitExceeded),
        ('terminate', Terminated),
    ])
    def test_cancel(self, worker, reason, exc):
        workloop, thread, _ = start_loop(worker)
        worker.send_task(1, run_awaited, wait_forever)
        worker.send_task(2, run_awaited, sleep_and_return, 0.3, 'other')
        worker.outq.wait_for(lambda m: sum(x[0] == ACK for x in m) == 2)
        time.sleep(0.1)
        worker.send_control(aioworker.CANCEL, 1, reason, signal.SIGTERM)
        worker.send_sentinel()
        thread.join(5)
        assert not thread.is_alive()
        results = readies(worker.outq.messages)
        assert results[2] == (True, 'other')
        assert results[1][0] is False
        assert isinstance(results[1][1].exception, exc)
        if reason == 'terminate':
            assert results[1][1].exception.args == (signal.SIGTERM,)

    @pytest.mark.parametrize('outcome', ['return', 'raise'])
    @pytest.mark.parametrize('reason,exc', [
        ('terminate', Terminated),
    ])
    def test_aborted_job_ignores_handled_cancel(self, worker, reason, exc,
                                                outcome):
        seen = []

        async def handle_cancel():
            try:
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                if outcome == 'raise':
                    raise ValueError('cleanup failed')
                return 'handled'

        def tracer():
            try:
                seen.append(run_awaited(handle_cancel))
            except Exception as exc:
                seen.append(exc)
                raise

        workloop, thread, _ = start_loop(worker)
        worker.send_task(1, tracer)
        worker.outq.wait_for(lambda m: m)
        time.sleep(0.1)
        worker.send_control(aioworker.CANCEL, 1, reason, signal.SIGTERM)
        worker.send_sentinel()
        thread.join(5)
        assert not seen
        result = readies(worker.outq.messages)[1]
        assert result[0] is False
        assert isinstance(result[1].exception, exc)

    def test_cancel_before_awaiter_starts(self, worker):
        workloop, thread, _ = start_loop(worker)
        started = threading.Event()

        def slow_job():
            started.wait(5)
            return run_awaited(wait_forever)

        worker.send_task(1, slow_job)
        worker.outq.wait_for(lambda m: m)
        worker.send_control(aioworker.CANCEL, 1, 'soft', None)
        time.sleep(0.2)
        started.set()
        worker.send_sentinel()
        thread.join(5)
        assert isinstance(readies(worker.outq.messages)[1][1].exception,
                          SoftTimeLimitExceeded)

    def test_coroutine_cancelling_itself(self, worker):
        async def cancel_self():
            asyncio.current_task().cancel()
            await asyncio.sleep(1)

        workloop, thread, _ = start_loop(worker)
        worker.send_task(1, run_awaited, cancel_self)
        worker.send_sentinel()
        thread.join(5)
        exc = readies(worker.outq.messages)[1][1].exception
        assert type(exc).__name__ == 'CancelledError'
        assert isinstance(exc, Exception)

    def test_coroutine_stopping_the_loop(self, worker):
        async def stop_loop():
            asyncio.get_running_loop().stop()
            await asyncio.sleep(0.1)
            return 'stopped'

        with patch.object(aioworker, 'logger') as logger:
            workloop, thread, result = start_loop(worker)
            worker.send_task(1, run_awaited, stop_loop)
            worker.outq.wait_for(lambda m: any(x[0] == READY for x in m))
            worker.send_task(2, run_awaited, sleep_and_return, 0, 'next')
            worker.send_sentinel()
            thread.join(5)
        assert not thread.is_alive()
        assert result['exit'] == EX_OK
        assert readies(worker.outq.messages) == {
            1: (True, 'stopped'), 2: (True, 'next')}
        logger.error.assert_called()

    def test_unknown_messages_and_cancel_of_unknown_job(self, worker):
        workloop, thread, _ = start_loop(worker)
        with patch.object(aioworker, 'logger') as logger:
            os.write(worker.inq_pipe.w, frame(('OTHER', ())))
            worker.send_control('OTHER')
            worker.send_control(aioworker.CANCEL, 99, 'soft', None)
            time.sleep(0.2)
            worker.send_sentinel()
            thread.join(5)
        assert logger.error.call_count == 2
        assert not thread.is_alive()

    def test_eof_stops_loop(self, worker):
        workloop, thread, _ = start_loop(worker)
        worker.send_task(1, run_awaited, sleep_and_return, 0.2, 'x')
        worker.outq.wait_for(lambda m: m)
        os.close(worker.inq_pipe.w)
        thread.join(5)
        assert not thread.is_alive()
        assert readies(worker.outq.messages) == {1: (True, 'x')}

    def test_shutdown_sentinel_event(self, worker):
        worker._shutdown = threading.Event()
        workloop, thread, _ = start_loop(worker)
        worker._shutdown.set()
        thread.join(5)
        assert not thread.is_alive()

    def test_awaiter_in_loop_thread_raises(self, worker):
        async def nested():
            coro = sleep_and_return(0, 1)
            with pytest.raises(RuntimeError):
                get_task_awaiter()(coro)
            return 'ok'

        workloop, thread, _ = start_loop(worker)
        worker.send_task(1, run_awaited, nested)
        worker.send_sentinel()
        thread.join(5)
        assert readies(worker.outq.messages) == {1: (True, 'ok')}

    def test_request_context_reaches_coroutine(self, worker):
        import contextvars
        var = contextvars.ContextVar('var', default=None)

        async def read_var():
            await asyncio.sleep(0.05)
            return var.get()

        def job(value):
            var.set(value)
            return run_awaited(read_var)

        workloop, thread, _ = start_loop(worker)
        for i in range(3):
            worker.send_task(i, job, i)
        worker.send_sentinel()
        thread.join(5)
        assert readies(worker.outq.messages) == {i: (True, i) for i in range(3)}

    def test_restores_previous_awaiter(self, worker):
        previous = Mock(name='awaiter')
        set_task_awaiter(previous)
        try:
            workloop, thread, _ = start_loop(worker)
            worker.send_sentinel()
            thread.join(5)
            assert get_task_awaiter() is previous
        finally:
            set_task_awaiter(None)


class test_LoopWatchdog:

    def test_warns_once_per_blocking_period(self):
        workloop = Mock(pid=1, loop_thread=threading.get_ident())
        workloop.last_beat = time.monotonic() - 10
        watchdog = aioworker.LoopWatchdog(workloop, 0.05)
        with patch.object(aioworker, 'logger') as logger:
            watchdog.start()
            time.sleep(0.2)
            assert logger.warning.call_count == 1
            assert 'test_warns_once' in logger.warning.call_args[0][-1]
            workloop.last_beat = time.monotonic() + 10
            time.sleep(0.1)
            workloop.last_beat = time.monotonic() - 10
            time.sleep(0.1)
            watchdog.stop()
            watchdog.join(1)
        assert logger.warning.call_count == 2

    @pytest.mark.parametrize('threshold', [0.01, 0.05])
    def test_idle_loop_does_not_warn(self, worker, threshold):
        worker.block_warning = threshold
        with patch.object(aioworker, 'logger') as logger:
            workloop, thread, _ = start_loop(worker)
            time.sleep(0.6)
            worker.send_sentinel()
            thread.join(5)
        assert not logger.warning.called
        assert workloop.heartbeat_interval <= max(
            threshold, aioworker.MIN_BLOCK_WARNING) / 4

    def test_workloop_starts_watchdog(self, worker):
        worker.block_warning = 0.05
        with patch.object(aioworker, 'LoopWatchdog') as watchdog:
            workloop, thread, _ = start_loop(worker)
            worker.send_sentinel()
            thread.join(5)
        watchdog.assert_called_once()
        watchdog.return_value.start.assert_called_once_with()
        watchdog.return_value.stop.assert_called_once_with()


class test_AsyncWorker:

    def make_worker(self):
        inq, outq, synq = Mock(), Mock(), Mock()
        for q in (inq, outq, synq):
            q._reader.fileno.return_value = 10
            q._writer.fileno.return_value = 11
        return aioworker.AsyncWorker(inq, outq, synq, capacity=7,
                                     block_warning=0.5)

    def test_reduce_keeps_options(self):
        w = self.make_worker()
        fun, args = w.__reduce__()
        assert fun.keywords == {'capacity': 7, 'block_warning': 0.5}
        assert fun.func is aioworker.AsyncWorker

    def test_after_fork_ignores_soft_timeout_signal(self):
        w = self.make_worker()
        with patch('billiard.pool.Worker.after_fork') as after_fork, \
                patch.object(aioworker.signal, 'signal') as set_signal:
            w.after_fork()
        after_fork.assert_called_once_with()
        set_signal.assert_called_once_with(signal.SIGUSR1, signal.SIG_IGN)
        w.synq._writer.close.assert_called_once_with()

    def test_workloop(self):
        w = self.make_worker()
        with patch.object(w, 'Workloop') as Workloop:
            assert w.workloop(pid=5) is Workloop.return_value.run.return_value
        Workloop.assert_called_once_with(w, 5)


def get_hub():
    hub = Hub()
    hub.readers, hub.writers = {}, {}
    hub.timer = Mock(name='hub.timer')
    hub.poller = Mock(name='hub.poller')
    return hub


class FakeProc:

    def __init__(self, fd, pid=1000):
        self.inqW_fd = fd
        self.pid = pid
        self._aio_jobs = set()
        self.synq = Mock()
        self.synqW_fd = None
        self.alive = True

    def _is_alive(self):
        return self.alive


@pytest.fixture
def apool():
    with patch('billiard.pool.Pool._create_worker_process'):
        pool = aiopool.AsyncAsynPool(processes=1, capacity=2, threads=False,
                                     enable_timeouts=True,
                                     semaphore=LaxBoundedSemaphore(3))
    yield pool


class test_AsyncAsynPool:

    def test_init(self, apool):
        assert apool.capacity == 2
        assert apool.sched_strategy == aiopool.SCHED_STRATEGY_FAIR
        assert apool.Worker.keywords == {'capacity': 2, 'block_warning': 1.0}
        assert apool._timeout_handler.pool is apool
        assert all(q[2] is not None for q in apool._queues)

    def test_init_takes_child_limits(self):
        with patch('billiard.pool.Pool._create_worker_process'):
            pool = aiopool.AsyncAsynPool(processes=1, threads=False,
                                         maxtasksperchild=10,
                                         max_memory_per_child=1000)
        assert pool.max_tasks_per_child == 10
        assert pool._maxtasksperchild is None
        assert pool._max_memory_per_child is None

    def test_capacity_accounting(self, apool):
        proc = FakeProc(5)
        apool._fileno_to_inq[5] = proc
        jobs = [Mock(_job=i) for i in range(3)]
        apool._on_job_scheduled(proc, 5, jobs[0])
        assert 5 not in apool._busy_workers
        apool._on_job_scheduled(proc, 5, jobs[1])
        assert 5 in apool._busy_workers
        assert apool.in_flight(proc) == 2
        assert apool.process_of(1) is proc
        apool.on_job_ready(0, None, None, 5)
        assert 5 not in apool._busy_workers
        apool.on_job_ready(0, None, None, 5)  # unknown job
        assert apool.in_flight(proc) == 1

    def test_rescheduled_job_moves(self, apool):
        a, b = FakeProc(5), FakeProc(6)
        apool._fileno_to_inq.update({5: a, 6: b})
        job = Mock(_job=1)
        apool._on_job_scheduled(a, 5, job)
        apool._on_job_scheduled(a, 5, Mock(_job=2))
        assert 5 in apool._busy_workers
        apool._on_job_scheduled(b, 6, job)
        assert a._aio_jobs == {2}
        assert 5 not in apool._busy_workers
        assert b._aio_jobs == {1}

    def test_busy_only_for_indexed_process(self, apool):
        proc = FakeProc(5)
        proc._aio_jobs = {1, 2}
        apool._update_busy(proc)
        assert 5 not in apool._busy_workers

    def test_order_ready_fds(self, apool):
        a, b = FakeProc(5), FakeProc(6)
        a._aio_jobs = {1, 2, 3}
        apool._fileno_to_inq.update({5: a, 6: b})
        assert apool.order_ready_fds([5, 6, 7]) == [6, 7, 5]

    def test_tokens_released_once_per_job(self, apool):
        hub = get_hub()
        apool._create_timelimit_handlers(hub)
        semaphore = apool._putlock
        for _ in range(3):
            semaphore.acquire(Mock())
        with patch('billiard.pool.Pool.apply_async') as apply_async:
            apply_async.side_effect = lambda *a, **kw: ApplyResult(
                apool._cache, None, on_timeout_cancel=apool.on_timeout_cancel)
            results = [apool.apply_async(Mock()) for _ in range(3)]
        assert semaphore.value == 0
        results[0]._set(0, (True, 1))
        results[0]._set(0, (True, 1))
        assert semaphore.value == 1
        results[1]._set_terminated(15)
        assert semaphore.value == 2

    def test_apply_async_not_running(self, apool):
        with patch('billiard.pool.Pool.apply_async', return_value=None):
            assert apool.apply_async(Mock()) is None
        assert not apool._tokens

    def test_result_handler_does_not_release(self, apool):
        assert apool._result_handler.putlock is None

    def test_maintain_pool_does_not_release_per_process(self, apool):
        apool._putlock = Mock()
        apool._join_exited_workers = Mock(return_value=[0, 0])
        apool._repopulate_pool = Mock()
        apool._maintain_pool()
        apool._repopulate_pool.assert_called_once_with([0, 0])
        apool._putlock.release.assert_not_called()

    def test_process_down_forgets_jobs(self, apool):
        hub = get_hub()
        apool._hub = hub
        apool._create_process_handlers(hub)
        proc = FakeProc(5)
        proc.dead = True
        proc._aio_jobs = {1, 2}
        apool._job_procs = {1: proc, 2: FakeProc(6)}
        apool._control_backlog[proc] = ['x']
        hub.remove_writer = Mock()
        apool.on_process_down(proc)
        assert apool._job_procs == {2: apool._job_procs[2]}
        assert not proc._aio_jobs
        assert proc not in apool._control_backlog
        hub.remove_writer.assert_called_once_with(proc.synqW_fd)

    def test_flush_recounts(self, apool):
        apool.maintain_pool = Mock()
        proc = FakeProc(5)
        apool._pool = [proc]
        apool._fileno_to_inq[5] = proc
        written = ApplyResult(apool._cache, None)
        written._write_to = proc
        unwritten = ApplyResult(apool._cache, None)
        for job in (written, unwritten):
            apool._on_job_scheduled(proc, 5, job)
            apool._tokens.add(job._job)
        apool._busy_workers.discard(5)
        apool.flush()
        assert proc._aio_jobs == {written._job}
        assert apool._tokens == set()
        assert 5 not in apool._busy_workers

    def test_terminate_job_cancels_terminated_jobs(self, apool):
        hub = Mock(name='hub')
        apool._hub = hub
        proc = FakeProc(5, pid=4242)
        apool._pool = [proc]
        jobs = [ApplyResult(apool._cache, None) for _ in range(3)]
        for job in jobs:
            proc._aio_jobs.add(job._job)
        apool.send_cancel = Mock()
        apool.terminate_job(4242, 15)
        hub.call_soon.assert_called_once_with(apool._cancel_terminated, proc)
        jobs[1].terminate(15)
        apool._cancel_terminated(proc)
        apool._cancel_terminated(proc)
        apool.send_cancel.assert_called_once_with(
            proc, jobs[1]._job, aioworker.CANCEL_TERMINATE, 15)
        apool.terminate_job(1, 15)  # unknown pid
        assert apool.owns_pid(4242) and not apool.owns_pid(1)

    def test_terminate_job_without_hub(self, apool):
        proc = FakeProc(5, pid=4242)
        apool._pool = [proc]
        apool._cancel_terminated = Mock()
        apool.terminate_job(4242, 9)
        apool._cancel_terminated.assert_called_once_with(proc)

    def test_send_control_writes_frames(self, apool):
        pipe = Pipe()
        try:
            proc = FakeProc(5)
            proc.synqW_fd = pipe.w
            apool.send_cancel(proc, 7, 'soft')
            os.set_blocking(pipe.r, False)
            assert aioworker.FrameReader(pipe.r).read() == [
                (aioworker.CANCEL, 7, 'soft', None)]
            proc.alive = False
            apool.send_cancel(proc, 8, 'soft')
            with pytest.raises(BlockingIOError):
                os.read(pipe.r, 10)
        finally:
            pipe.close()

    def test_send_control_backlog(self, apool):
        pipe = Pipe()
        try:
            os.set_blocking(pipe.w, False)
            hub = apool._hub = Mock(name='hub')
            proc = FakeProc(5)
            proc.synqW_fd = pipe.w
            filler = b'x' * 4096
            while True:
                try:
                    os.write(pipe.w, filler)
                except BlockingIOError:
                    break
            apool.send_cancel(proc, 1, 'soft')
            apool.send_cancel(proc, 2, 'soft')
            hub.add_writer.assert_called_once_with(
                pipe.w, apool._flush_control, proc)
            assert len(apool._control_backlog[proc]) == 2
            apool._flush_control(proc)
            assert len(apool._control_backlog[proc]) == 2
            os.set_blocking(pipe.r, False)
            while True:
                try:
                    if not os.read(pipe.r, 65536):
                        break
                except BlockingIOError:
                    break
            apool._flush_control(proc)
            assert proc not in apool._control_backlog
            hub.remove_writer.assert_called_once_with(pipe.w)
            assert aioworker.FrameReader(pipe.r).read() == [
                (aioworker.CANCEL, 1, 'soft', None),
                (aioworker.CANCEL, 2, 'soft', None),
            ]
        finally:
            pipe.close()

    def test_write_control_partial_and_closed(self, apool):
        proc = FakeProc(5)
        proc.synqW_fd = 99
        backlog = aiopool.deque([b'abcdef'])
        with patch.object(aiopool.os, 'write', return_value=2):
            assert not apool._write_control(proc, backlog)
        assert backlog[0] == b'cdef'
        with patch.object(aiopool.os, 'write',
                          side_effect=OSError(aiopool.errno.EPIPE, 'gone')):
            assert apool._write_control(proc, backlog)
        assert not backlog
        backlog.append(b'x')
        with patch.object(aiopool.os, 'write',
                          side_effect=OSError(aiopool.errno.EIO, 'io')):
            with pytest.raises(OSError):
                apool._write_control(proc, backlog)

    def test_soft_timeout_sends_cancel(self, apool):
        handler = apool._timeout_handler
        proc = FakeProc(5)
        job = Mock(_job=3)
        apool.send_cancel = Mock()
        handler.on_soft_timeout(job)
        job.handle_timeout.assert_not_called()
        apool._job_procs[3] = proc
        handler.on_soft_timeout(job)
        job.handle_timeout.assert_called_once_with(soft=True)
        apool.send_cancel.assert_called_once_with(proc, 3, 'soft')

    def test_process_info(self, apool):
        proc = FakeProc(5, pid=10)
        proc._aio_jobs = {1}
        apool._pool = [proc]
        assert apool.process_info() == [{'pid': 10, 'in-flight': 1}]

    def test_worker_process_tracks_jobs(self, apool):
        with patch('celery.concurrency.asynpool.AsynPool.WorkerProcess',
                   return_value=Mock()):
            proc = apool.WorkerProcess(Mock())
        assert proc._aio_jobs == set()

    def test_register_with_event_loop_keeps_hub(self, apool):
        hub = get_hub()
        with patch('celery.concurrency.asynpool.AsynPool.register_with_event_loop'):
            apool.register_with_event_loop(hub)
        assert apool._hub is hub


@pytest.fixture
def composite(app):
    @app.task(shared=False)
    async def async_task():
        pass

    @app.task(shared=False)
    def sync_task():
        pass

    pool = aio.TaskPool(2, async_concurrency=5, async_processes=2, app=app,
                        semaphore=LaxBoundedSemaphore(2))
    pool.async_name, pool.sync_name = async_task.name, sync_task.name
    return pool


class test_TaskPool:

    def test_init(self, composite):
        assert composite.async_capacity == 3
        assert composite.supports_async_concurrency
        assert composite.uses_child_processes
        assert isinstance(composite.task_semaphore, aio.TaskTypeSemaphore)

    def test_init_without_semaphore(self, app):
        pool = aio.TaskPool(2, async_concurrency=3, async_processes=5, app=app)
        assert pool.task_semaphore is None
        assert pool.async_processes == 3
        assert pool.async_capacity == 1

    def test_init_rejects_zero(self, app):
        with pytest.raises(ValueError):
            aio.TaskPool(2, async_concurrency=0, app=app)

    def test_is_async_task_name(self, composite):
        assert composite.is_async_task_name(composite.async_name)
        assert composite.is_async_task_name(composite.async_name)
        assert not composite.is_async_task_name(composite.sync_name)
        assert not composite.is_async_task_name('unknown')
        assert not composite.is_async_task_name(['unhashable'])
        composite.app = None
        assert not composite.is_async_task_name('other')

    def test_task_semaphore(self, composite):
        sem = composite.task_semaphore
        callback = Mock()
        sync_request, async_request = Mock(), Mock()
        sync_request.type = composite.sync_name
        async_request.type = composite.async_name
        for _ in range(3):
            sem.acquire(callback, sync_request)
        assert callback.call_count == 2
        for _ in range(6):
            sem.acquire(callback, async_request)
        assert callback.call_count == 7
        sem.clear()
        assert sem.sync_semaphore.value == 2
        assert sem.async_semaphore.value == 5
        assert 'TaskTypeSemaphore' in repr(sem)

    def test_task_semaphore_release_in_callback(self, composite):
        # A revoked request gives its token back from within its callback.
        sem = composite.task_semaphore
        async_request = Mock()
        async_request.type = composite.async_name
        sem.acquire(lambda req: sem.release(), async_request)
        assert sem.async_semaphore.value == 5
        assert sem.sync_semaphore.value == 2
        sem.acquire(Mock(), async_request)
        assert sem.async_semaphore.value == 4
        assert sem._running is None
        # Outside a callback the sync semaphore is released.
        sem.sync_semaphore.acquire(Mock())
        sem.release()
        assert sem.sync_semaphore.value == 2

    @pytest.mark.parametrize('kind', ['async', 'sync'])
    def test_task_semaphore_ignores_shadow_name(self, composite, kind):
        # The pool part is chosen by the registered task name, so the token
        # comes from the semaphore that the part releases.
        sem = composite.task_semaphore
        other = 'sync' if kind == 'async' else 'async'
        request = Mock()
        request.type = getattr(composite, f'{kind}_name')
        request.name = getattr(composite, f'{other}_name')
        sem.acquire(Mock(), request)
        assert getattr(sem, f'{kind}_semaphore').value == \
            getattr(sem, f'{kind}_semaphore').initial_value - 1
        assert getattr(sem, f'{other}_semaphore').value == \
            getattr(sem, f'{other}_semaphore').initial_value

    def started(self, composite):
        with patch('celery.concurrency.prefork.TaskPool.on_start') as on_start, \
                patch.object(composite, 'AsyncPool') as AsyncPool:
            def set_pool():
                composite._pool = Mock(name='sync', _processes=2)
            on_start.side_effect = set_pool
            composite.on_start()
        return AsyncPool

    def test_on_start(self, composite):
        AsyncPool = self.started(composite)
        kwargs = AsyncPool.call_args[1]
        assert kwargs['processes'] == 2
        assert kwargs['capacity'] == 3
        assert kwargs['semaphore'] is composite._async_semaphore
        assert kwargs['block_warning'] == 1.0
        assert composite.num_processes == 7

    def test_dispatch(self, composite):
        self.started(composite)
        sync, async_ = composite._pool, composite._async
        composite.on_apply(Mock(), (composite.async_name,), {})
        async_.apply_async.assert_called_once()
        composite.on_apply(Mock(), (composite.sync_name,), {})
        composite.on_apply(Mock(), (), {})
        assert sync.apply_async.call_count == 2

    def test_proxies(self, composite):
        self.started(composite)
        sync, async_ = composite._pool, composite._async
        composite.maintain_pool()
        sync.maintain_pool.assert_called_once_with()
        async_.maintain_pool.assert_called_once_with()
        composite.flush()
        sync.flush.assert_called_once_with()
        async_.flush.assert_called_once_with()
        async_.owns_pid.return_value = True
        composite.terminate_job(10, 15)
        async_.terminate_job.assert_called_once_with(10, 15)
        async_.owns_pid.return_value = False
        composite.terminate_job(11, 15)
        sync.terminate_job.assert_called_once_with(11, 15)
        assert composite.did_start_ok()
        composite.restart()
        sync.restart.assert_called_once_with()
        async_.restart.assert_called_once_with()
        sync.apply_async.assert_called_once()

    def test_register_with_event_loop_multiplexes_writes(self, composite):
        self.started(composite)
        sync, async_ = composite._pool, composite._async
        hub = Mock(name='hub')
        sync_writes, async_writes = Mock(), Mock()
        sync.register_with_event_loop.side_effect = (
            lambda h: setattr(h, 'consolidate_callback', sync_writes))
        async_.register_with_event_loop.side_effect = (
            lambda h: setattr(h, 'consolidate_callback', async_writes))
        async_._fileno_to_inq = {7: Mock(), 8: Mock()}
        async_.order_ready_fds.side_effect = lambda fds: list(reversed(fds))
        composite.register_with_event_loop(hub)
        hub.consolidate_callback([3, 7, 4, 8])
        sync_writes.assert_called_once_with([3, 4])
        async_writes.assert_called_once_with([8, 7])
        hub.consolidate_callback([3])
        async_writes.assert_called_once()

    def test_close_join_stop_terminate(self, composite):
        self.started(composite)
        sync, async_ = composite._pool, composite._async
        async_._state = aio.prefork.RUN
        sync._state = aio.prefork.RUN
        composite.on_close()
        sync.close.assert_called_once_with()
        async_.close.assert_called_once_with()
        composite._close_pools()
        composite._join_pools()
        sync.join.assert_called_once_with()
        async_.join.assert_called_once_with()
        with patch('celery.concurrency.prefork.get_event_loop',
                   return_value=None):
            composite.on_stop()
        assert composite._async is None
        assert composite._pool is None
        composite._pool = sync
        composite._async = async_
        composite.on_terminate()
        async_.terminate.assert_called_once_with()
        assert composite._async is None
        composite.on_close()
        composite._close_pools = lambda: None
        composite._pool = Mock()
        composite._join_pools()

    def test_info(self, composite):
        self.started(composite)
        composite._pool._pool = []
        composite._pool._maxtasksperchild = None
        composite._pool.human_write_stats.return_value = {}
        composite._async.process_info.return_value = [{'pid': 1}]
        info = composite._get_info()
        assert info['async-concurrency'] == 5
        assert info['async-processes'] == [{'pid': 1}]
        assert info['max-concurrency'] == 2
        composite._async = None
        assert composite._get_info()['async-processes'] == []
