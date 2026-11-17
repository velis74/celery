"""A real worker with ``--aconcurrency`` running ``async def`` tasks.

The worker runs in a subprocess against the broker in
``ASYNC_TEST_BROKER`` (default ``redis://127.0.0.1:6390/0``) and the result
backend in ``ASYNC_TEST_BACKEND`` (default ``redis://127.0.0.1:6390/1``).
The tests are skipped when either is unreachable.
"""
import os
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest
from billiard.exceptions import SoftTimeLimitExceeded, TimeLimitExceeded, WorkerLostError
from kombu import Connection

import t.skip
from t.integration import async_tasks as tasks

pytestmark = t.skip.if_win32

ROOT = Path(__file__).resolve().parents[2]
WORKER_START_TIMEOUT = 60
WORKER_STOP_TIMEOUT = 30


def _reachable():
    try:
        with Connection(tasks.BROKER, connect_timeout=2) as conn:
            conn.ensure_connection(max_retries=1, timeout=2)
        client = getattr(tasks.app.backend, 'client', None)
        if client is not None:
            client.ping()
    except Exception as exc:  # pylint: disable=broad-except
        return repr(exc)
    return None


@pytest.fixture(scope='module', autouse=True)
def broker_available():
    error = _reachable()
    if error is not None:
        pytest.skip(f'Broker or result backend unreachable: {error}')


class Worker:
    """A worker subprocess consuming its own queue."""

    def __init__(self, tmp_path, *args):
        self.name = f'asyncit-{uuid.uuid4().hex[:10]}'
        self.queue = self.name
        self.log = tmp_path / f'{self.name}.log'
        self.args = args
        self.proc = None
        self.node = None
        self.leftover = False

    def start(self):
        env = dict(os.environ, PYTHONPATH=str(ROOT))
        env.setdefault('ASYNC_TEST_BROKER', tasks.BROKER)
        env.setdefault('ASYNC_TEST_BACKEND', tasks.BACKEND)
        with open(self.log, 'wb') as log:
            self.proc = subprocess.Popen(
                [sys.executable, '-m', 'celery', '-A', 't.integration.async_tasks',
                 'worker', '-n', f'{self.name}@%h', '-Q', self.queue, '-l', 'info',
                 '--without-gossip', '--without-mingle', '--without-heartbeat',
                 *self.args],
                cwd=ROOT, env=env, stdin=subprocess.DEVNULL,
                stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
            )
        deadline = time.monotonic() + WORKER_START_TIMEOUT
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                break
            if b' ready.' in self.log.read_bytes():
                replies = tasks.app.control.inspect(timeout=1).ping() or {}
                self.node = next(
                    (n for n in replies if n.startswith(self.name + '@')), None)
                if self.node:
                    return self
            time.sleep(0.2)
        log = self.read_log()
        self.stop()
        pytest.fail(f'Worker did not start:\n{log}')

    def send(self, task, *args, **options):
        return task.apply_async(args, queue=self.queue, **options)

    def pool_stats(self):
        replies = tasks.app.control.inspect(
            destination=[self.node], timeout=3).stats() or {}
        return replies[self.node]['pool']

    def read_log(self):
        return self.log.read_text(errors='replace')

    def wait_for_log(self, text, timeout=10):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if text in self.read_log():
                return True
            time.sleep(0.1)
        return False

    def stop(self, timeout=WORKER_STOP_TIMEOUT):
        """Warm shutdown; kill the process group if it does not exit."""
        if self.proc is None:
            return None
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout)
            except subprocess.TimeoutExpired:
                pass
        if self.proc.poll() is not None:
            # Children of a worker that exited must not outlive it.
            deadline = time.monotonic() + 2
            while group_alive(self.proc.pid) and time.monotonic() < deadline:
                time.sleep(0.05)
            self.leftover = group_alive(self.proc.pid)
        try:
            os.killpg(self.proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        self.proc.wait()
        self._delete_queue()
        return self.proc.returncode

    def _delete_queue(self):
        try:
            with Connection(tasks.BROKER) as conn:
                conn.default_channel.queue_delete(self.queue)
        except Exception:  # pylint: disable=broad-except
            pass


def group_alive(pgid):
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    return True


@pytest.fixture
def make_worker(tmp_path):
    workers = []

    def make(*args):
        worker = Worker(tmp_path, *args)
        workers.append(worker)
        return worker.start()

    yield make
    for worker in workers:
        worker.stop()


@pytest.fixture(scope='module')
def worker(tmp_path_factory):
    w = Worker(tmp_path_factory.mktemp('async-worker'),
               '-c', '2', '--aconcurrency=200', '--aprocesses=2')
    w.start()
    yield w
    w.stop()


def results(rs, timeout=60):
    """Wait for ``rs``; return ``(state, value)`` for each."""
    deadline = time.monotonic() + timeout
    while not all(r.ready() for r in rs):
        if time.monotonic() > deadline:
            pending = sum(not r.ready() for r in rs)
            pytest.fail(f'{pending} of {len(rs)} results not ready')
        time.sleep(0.05)
    return [(r.state, r.get(timeout=5, propagate=False)) for r in rs]


def async_pids(worker):
    return {p['pid'] for p in worker.pool_stats()['async-processes']}


def assert_slots_free(worker, timeout=10):
    deadline = time.monotonic() + timeout
    while True:
        slots = worker.pool_stats()['free-slots']
        if slots == {'sync': 2, 'async': 200} or time.monotonic() > deadline:
            break
        time.sleep(0.2)
    assert slots == {'sync': 2, 'async': 200}


def test_concurrency(worker):
    pids = async_pids(worker)
    assert len(pids) == 2
    start = time.monotonic()
    rs = [worker.send(tasks.asleep, 2) for _ in range(200)]
    out = results(rs)
    elapsed = time.monotonic() - start
    assert all(state == 'SUCCESS' for state, _ in out)
    # Each task sees its own request.
    assert [value[0] for _, value in out] == [r.id for r in rs]
    assert {value[1] for _, value in out} == pids
    # One round: 200 slots for 200 tasks.
    assert elapsed < 3.8, elapsed


def test_sync_tasks_run_in_prefork_processes(worker):
    sync_pids = set(worker.pool_stats()['processes'])
    out = results([worker.send(tasks.add, i, i) for i in range(10)]
                  + [worker.send(tasks.asleep, 0.1) for _ in range(10)])
    assert [value[0] for _, value in out[:10]] == [2 * i for i in range(10)]
    assert {value[1] for _, value in out[:10]} <= sync_pids
    assert not {value[1] for _, value in out[10:]} & sync_pids


def test_exception(worker):
    [(state, value)] = results([worker.send(tasks.araise)])
    assert state == 'FAILURE'
    assert isinstance(value, ValueError)
    assert str(value) == 'boom'


def test_retry(worker):
    r = worker.send(tasks.aretry)
    [(state, value)] = results([r])
    assert state == 'SUCCESS'
    assert value == [r.id, 1]


def test_soft_time_limit(worker):
    rs = [worker.send(tasks.asoft, 5), worker.send(tasks.asoft_catch, 5)]
    rs += [worker.send(tasks.asleep, 1) for _ in range(6)]
    out = results(rs)
    assert out[0][0] == 'FAILURE'
    assert isinstance(out[0][1], SoftTimeLimitExceeded)
    assert out[1] == ('SUCCESS', 'cancelled')
    assert all(state == 'SUCCESS' for state, _ in out[2:])


def test_hard_time_limit_recycles_process(worker):
    before = async_pids(worker)
    start = time.monotonic()
    rs = [worker.send(tasks.ahard, 30)]
    rs += [worker.send(tasks.asleep, 2.5) for _ in range(6)]
    [(state, value)] = results(rs[:1])
    assert time.monotonic() - start < 3
    assert state == 'FAILURE'
    assert isinstance(value, TimeLimitExceeded)
    # Jobs of the recycled process run to completion.
    assert all(state == 'SUCCESS' for state, _ in results(rs[1:]))
    assert worker.wait_for_log('Recycling async pool process')
    deadline = time.monotonic() + 10
    while async_pids(worker) & before == before and time.monotonic() < deadline:
        time.sleep(0.2)
    after = async_pids(worker)
    assert len(after) == 2
    assert after != before
    out = results([worker.send(tasks.asleep, 0.1) for _ in range(20)])
    assert all(state == 'SUCCESS' for state, _ in out)


def test_hard_time_limit_kills_stubborn_coroutine(worker):
    rs = [worker.send(tasks.ahard_stubborn, 30)]
    rs += [worker.send(tasks.asleep, 1) for _ in range(4)]
    out = results(rs)
    assert out[0][0] == 'FAILURE'
    assert isinstance(out[0][1], TimeLimitExceeded)
    assert all(state == 'SUCCESS' for state, _ in out[1:])
    assert worker.wait_for_log('only jobs over the time limit remain')


def test_blocked_event_loop(worker):
    rs = [worker.send(tasks.ablock, 30)]
    out = results(rs, timeout=30)
    assert out[0][0] == 'FAILURE'
    assert isinstance(out[0][1], TimeLimitExceeded)
    assert worker.wait_for_log('has not run for')
    assert worker.wait_for_log('Killing recycled async pool process')


def test_revoke_terminate(worker):
    target = worker.send(tasks.asleep, 30)
    others = [worker.send(tasks.asleep, 1.5) for _ in range(4)]
    time.sleep(0.5)
    start = time.monotonic()
    target.revoke(terminate=True)
    assert results([target], timeout=10)[0][0] == 'REVOKED'
    assert time.monotonic() - start < 3
    assert all(state == 'SUCCESS' for state, _ in results(others))


def test_crash_fails_in_flight_jobs(worker):
    out = results([worker.send(tasks.acrash)], timeout=30)
    assert out[0][0] == 'FAILURE'
    assert isinstance(out[0][1], WorkerLostError)
    deadline = time.monotonic() + 10
    while len(async_pids(worker)) < 2 and time.monotonic() < deadline:
        time.sleep(0.2)
    assert len(async_pids(worker)) == 2
    assert all(state == 'SUCCESS' for state, _ in
               results([worker.send(tasks.asleep, 0.1) for _ in range(10)]))


def test_crash_redelivers_acks_late_task(worker, tmp_path):
    flag = tmp_path / 'crashed'
    [(state, value)] = results([worker.send(tasks.acrash_once, str(flag))],
                               timeout=30)
    assert flag.exists()
    assert state == 'SUCCESS'
    assert value[0] == 'survived'


def test_no_slot_leak_under_stress(worker):
    rs = [worker.send(tasks.asleep, 0.5) for _ in range(300)]
    rs += [worker.send(tasks.ahard, 30) for _ in range(2)]
    rs += [worker.send(tasks.asoft, 30) for _ in range(2)]
    revoked = [worker.send(tasks.asleep, 30, countdown=1) for _ in range(3)]
    for r in revoked:
        r.revoke()
    rs.append(worker.send(tasks.acrash))
    out = results(rs + revoked, timeout=90)
    states = [state for state, _ in out]
    assert states.count('REVOKED') == 3
    assert {'SUCCESS', 'FAILURE'} >= set(states[:-3])
    assert_slots_free(worker)
    # The full capacity is available again: one round.
    start = time.monotonic()
    assert all(state == 'SUCCESS' for state, _ in
               results([worker.send(tasks.asleep, 2) for _ in range(200)]))
    assert time.monotonic() - start < 3.8


def test_max_tasks_per_child(make_worker):
    w = make_worker('-c', '1', '--aconcurrency=10', '--aprocesses=2',
                    '--max-tasks-per-child=3')
    out = results([w.send(tasks.asleep, 0.2) for _ in range(30)])
    assert all(state == 'SUCCESS' for state, _ in out)
    per_pid = {}
    for _, (_, pid) in out:
        per_pid[pid] = per_pid.get(pid, 0) + 1
    assert max(per_pid.values()) <= 3
    assert len(per_pid) >= 10
    assert 'RestartFreqExceeded' not in w.read_log()


def test_warm_shutdown_finishes_in_flight_jobs(make_worker):
    w = make_worker('-c', '1', '--aconcurrency=20')
    rs = [w.send(tasks.asleep, 3) for _ in range(20)]
    deadline = time.monotonic() + 10
    while w.pool_stats()['free-slots']['async'] > 0:
        assert time.monotonic() < deadline
        time.sleep(0.1)
    start = time.monotonic()
    assert w.stop() == 0
    assert time.monotonic() - start < 15
    assert not w.leftover
    assert all(state == 'SUCCESS' for state, _ in results(rs, timeout=10))


def test_worker_without_aconcurrency(make_worker):
    w = make_worker('-c', '1')
    assert '1 (prefork)' in w.read_log()
    assert 'async:' not in w.read_log()
    assert 'async-processes' not in w.pool_stats()
    assert results([w.send(tasks.add, 1, 2)])[0][1][0] == 3
