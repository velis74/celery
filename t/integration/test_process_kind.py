"""``current_process_kind()`` and ``current_process_capacity()`` in a real worker.

Uses the worker subprocess and broker of :mod:`t.integration.test_async_worker`.
"""
import pytest

import t.skip
from celery.concurrency import current_process_capacity, current_process_kind
from t.integration import async_tasks as tasks
from t.integration.test_async_worker import Worker, _reachable, results

pytestmark = t.skip.if_win32


@pytest.fixture(scope='module', autouse=True)
def broker_available():
    error = _reachable()
    if error is not None:
        pytest.skip(f'Broker or result backend unreachable: {error}')


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


def values(worker, task, count=1):
    out = results([worker.send(task) for _ in range(count)])
    assert all(state == 'SUCCESS' for state, _ in out), out
    return [value for _, value in out]


def test_prefork_and_async_processes(make_worker):
    w = make_worker('-c', '1', '--aconcurrency=4', '--aprocesses=2')
    assert values(w, tasks.process_kind, 4) == [['prefork', 1]] * 4
    assert values(w, tasks.aprocess_kind, 8) == [['async', 2]] * 8


def test_async_capacity_rounds_up(make_worker):
    w = make_worker('-c', '1', '--aconcurrency=3', '--aprocesses=2')
    assert values(w, tasks.aprocess_kind, 6) == [['async', 2]] * 6


def test_prefork_worker_without_aconcurrency(make_worker):
    w = make_worker('-c', '1')
    assert values(w, tasks.process_kind) == [['prefork', 1]]


def test_process_init_sees_the_task_values(make_worker):
    w = make_worker('-c', '1', '--aconcurrency=4', '--aprocesses=2')
    for recorded, current in values(w, tasks.process_init_values, 4):
        assert recorded == current == ['prefork', 1]
    for recorded, current in values(w, tasks.aprocess_init_values, 8):
        assert recorded == current == ['async', 2]


def test_client_and_eager_execution():
    assert current_process_kind() is None
    assert current_process_capacity() is None
    assert tasks.process_kind.apply().get() == [None, None]


def test_solo_worker(make_worker):
    w = make_worker('--pool=solo')
    assert values(w, tasks.process_kind) == [[None, None]]
