"""Sync tasks with an async implementation in ``__celery_async__``.

Uses the worker subprocess and broker of :mod:`t.integration.test_async_worker`.
"""
import time

import pytest

import t.skip
from celery.exceptions import SoftTimeLimitExceeded, TimeLimitExceeded
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


def values(worker, task, *args, count=1):
    out = results([worker.send(task, *args) for _ in range(count)])
    assert all(state == 'SUCCESS' for state, _ in out), out
    return [value for _, value in out]


def test_async_worker_awaits_async_implementation(make_worker):
    w = make_worker('-c', '1', '--aconcurrency=200', '--aprocesses=2')
    assert values(w, tasks.hybrid) == [['async', 'async']]
    start = time.monotonic()
    out = values(w, tasks.hybrid, 0.5, count=200)
    elapsed = time.monotonic() - start
    assert out == [['async', 'async']] * 200
    assert elapsed < 15, elapsed


def test_worker_without_async_processes_runs_sync_function(make_worker):
    w = make_worker('-c', '1')
    assert values(w, tasks.hybrid) == [['sync', 'prefork']]


def test_solo_worker_runs_sync_function(make_worker):
    w = make_worker('--pool=solo')
    assert values(w, tasks.hybrid) == [['sync', None]]


def test_bound_async_implementation_sees_its_own_request(make_worker):
    w = make_worker('-c', '1', '--aconcurrency=50', '--aprocesses=1')
    rs = [w.send(tasks.hybrid_bound, 0.5) for _ in range(20)]
    out = results(rs)
    assert out == [('SUCCESS', ['async', r.id]) for r in rs]


def test_soft_time_limit(make_worker):
    w = make_worker('-c', '1', '--aconcurrency=10', '--aprocesses=1')
    out = results([w.send(tasks.hybrid_soft, 5),
                   w.send(tasks.hybrid_soft_fail, 5)])
    assert out[0] == ('SUCCESS', 'soft limit')
    assert out[1][0] == 'FAILURE'
    assert isinstance(out[1][1], SoftTimeLimitExceeded)


def test_hard_time_limit(make_worker):
    w = make_worker('-c', '1', '--aconcurrency=10', '--aprocesses=1')
    [(state, value)] = results([w.send(tasks.hybrid_hard, 30)])
    assert state == 'FAILURE'
    assert isinstance(value, TimeLimitExceeded)
    assert values(w, tasks.hybrid) == [['async', 'async']]


def test_apply_and_direct_call_run_sync_function():
    assert tasks.hybrid.apply().get() == ['sync', None]
    assert tasks.hybrid() == ['sync', None]


def test_functools_wraps_wrapper_routes_to_async_implementation(make_worker):
    w = make_worker('-c', '1', '--aconcurrency=10', '--aprocesses=1')
    assert values(w, tasks.wrapped) == [['async', 'async']]


def test_async_body_in_a_sync_wrapper(make_worker):
    w = make_worker('-c', '1', '--aconcurrency=50', '--aprocesses=1')
    rs = [w.send(tasks.async_body, 0.5) for _ in range(20)]
    start = time.monotonic()
    assert results(rs) == [('SUCCESS', [r.id, 'async']) for r in rs]
    assert time.monotonic() - start < 5


def test_async_body_in_a_sync_wrapper_without_async_processes(make_worker):
    w = make_worker('-c', '1')
    r = w.send(tasks.async_body, 0)
    assert results([r]) == [('SUCCESS', [r.id, 'prefork'])]
    assert tasks.async_body.apply((0,), task_id='eager').get() == ['eager', None]
