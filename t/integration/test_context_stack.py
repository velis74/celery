"""The task and request a task body sees in a real worker, for every pool.

The worker runs in a subprocess against the broker in
``CONTEXT_STACK_TEST_BROKER`` (default ``redis://127.0.0.1:6390/0``) and
the result backend in ``CONTEXT_STACK_TEST_BACKEND`` (default
``redis://127.0.0.1:6390/1``).  The tests are skipped when either is
unreachable.
"""
import os
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest
from kombu import Connection

import t.skip
from t.integration import context_stack_tasks as tasks

pytestmark = t.skip.if_win32

ROOT = Path(__file__).resolve().parents[2]
WORKER_START_TIMEOUT = 60
WORKER_STOP_TIMEOUT = 30
RESULT_TIMEOUT = 30


@pytest.fixture(scope='module', autouse=True)
def broker_available():
    try:
        with Connection(tasks.BROKER, connect_timeout=2) as conn:
            conn.ensure_connection(max_retries=1, timeout=2)
        tasks.app.backend.client.ping()
    except Exception as exc:  # pylint: disable=broad-except
        pytest.skip(f'Broker or result backend unreachable: {exc!r}')


class Worker:
    """A worker subprocess consuming its own queue."""

    def __init__(self, log_dir, *args):
        self.name = f'ctxstack-{uuid.uuid4().hex[:10]}'
        self.queue = self.name
        self.log = log_dir / f'{self.name}.log'
        self.args = args
        self.proc = None

    def start(self):
        pythonpath = os.pathsep.join(filter(None, [str(ROOT), os.environ.get('PYTHONPATH')]))
        env = dict(os.environ, PYTHONPATH=pythonpath)
        env.setdefault('CONTEXT_STACK_TEST_BROKER', tasks.BROKER)
        env.setdefault('CONTEXT_STACK_TEST_BACKEND', tasks.BACKEND)
        with open(self.log, 'wb') as log:
            self.proc = subprocess.Popen(
                [sys.executable, '-m', 'celery', '-A', 't.integration.context_stack_tasks',
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
                return self
            time.sleep(0.2)
        log = self.log.read_text(errors='replace')
        self.stop()
        pytest.fail(f'Worker did not start:\n{log}')

    def send(self, task, *args):
        return task.apply_async(args, queue=self.queue)

    def run(self, task, *args):
        return self.send(task, *args).get(timeout=RESULT_TIMEOUT)

    def run_concurrently(self, task, n, *args):
        results = [self.send(task, *args) for _ in range(n)]
        return [(r.id, r.get(timeout=RESULT_TIMEOUT)) for r in results]

    def stop(self):
        if self.proc is None:
            return
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(WORKER_STOP_TIMEOUT)
            except subprocess.TimeoutExpired:
                pass
        try:
            os.killpg(self.proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        self.proc.wait()
        try:
            with Connection(tasks.BROKER) as conn:
                conn.default_channel.queue_delete(self.queue)
        except Exception:  # pylint: disable=broad-except
            pass


@pytest.fixture(scope='module')
def start_worker(tmp_path_factory):
    workers = []

    def start(*args):
        worker = Worker(tmp_path_factory.mktemp('context-stack'), *args).start()
        workers.append(worker)
        return worker

    yield start
    for worker in workers:
        worker.stop()


def sees(task, task_id, worker_task=None):
    """What a task running as ``task_id`` reports through :func:`tasks.seen`."""
    return [task.name, task_id, (worker_task or task).name, task_id]


def assert_overlap(results):
    """The tasks ran at the same time, so they shared the process."""
    assert max(r['start'] for _, r in results) < min(r['end'] for _, r in results)
    assert len({r['pid'] for _, r in results}) == 1


SEQUENTIAL_POOLS = {
    'prefork': ('-P', 'prefork', '-c', '2'),
    'solo': ('-P', 'solo'),
}


@pytest.fixture(scope='module', params=sorted(SEQUENTIAL_POOLS))
def sequential_worker(request, start_worker):
    return start_worker(*SEQUENTIAL_POOLS[request.param])


class test_prefork_and_solo:
    """Scenario 1."""

    def test_task_sees_itself(self, sequential_worker):
        res = sequential_worker.send(tasks.whoami)
        out = res.get(timeout=RESULT_TIMEOUT)
        assert out['before'] == out['after'] == sees(tasks.whoami, res.id)

    def test_previous_task_request_is_gone(self, sequential_worker):
        # Every process of the pool runs whoami, then other_request.
        for _ in range(4):
            sequential_worker.run(tasks.whoami)
        res = sequential_worker.send(tasks.other_request, tasks.whoami.name)
        out = res.get(timeout=RESULT_TIMEOUT)
        assert out['other'] is None
        assert out['seen'] == sees(tasks.other_request, res.id)

    def test_nested_apply_and_call(self, sequential_worker):
        res = sequential_worker.send(tasks.nested)
        out = res.get(timeout=RESULT_TIMEOUT)
        outer = sees(tasks.nested, res.id)
        assert out['before'] == out['after'] == outer
        inner_id = out['inner']['before'][1]
        assert inner_id not in (None, res.id)
        assert out['inner']['before'] == out['inner']['after'] == sees(tasks.whoami, inner_id)
        assert out['direct']['before'] == [tasks.whoami.name, None, tasks.nested.name, None]


CONCURRENT_POOLS = {
    'threads': ('-P', 'threads', '-c', '4'),
    'gevent': ('-P', 'gevent', '-c', '10'),
    'eventlet': ('-P', 'eventlet', '-c', '10'),
}


@pytest.fixture(scope='module', params=sorted(CONCURRENT_POOLS))
def concurrent_worker(request, start_worker):
    pool = request.param
    if pool != 'threads':
        pytest.importorskip(pool)
    return start_worker(*CONCURRENT_POOLS[pool])


class test_concurrent_pools:
    """Scenarios 2 and 3: threads, gevent and eventlet pools."""

    def test_concurrent_tasks_see_their_own_request(self, concurrent_worker):
        results = concurrent_worker.run_concurrently(tasks.whoami, 4, 1.0)
        assert_overlap(results)
        for task_id, out in results:
            assert out['before'] == out['after'] == sees(tasks.whoami, task_id)

    def test_previous_task_request_is_gone(self, concurrent_worker):
        concurrent_worker.run_concurrently(tasks.whoami, 4)
        res = concurrent_worker.send(tasks.other_request, tasks.whoami.name)
        out = res.get(timeout=RESULT_TIMEOUT)
        assert out['other'] is None
        assert out['seen'] == sees(tasks.other_request, res.id)


@pytest.fixture(scope='module')
def async_worker(start_worker):
    return start_worker('-c', '1', '--aconcurrency=50', '--aprocesses=1')


class test_async_worker:
    """Scenario 4: async tasks interleaved on one event loop."""

    def test_concurrent_async_tasks_see_their_own_request(self, async_worker):
        results = async_worker.run_concurrently(tasks.awhoami, 10, 1.0)
        assert_overlap(results)
        for task_id, out in results:
            assert out['before'] == out['after'] == sees(tasks.awhoami, task_id)
