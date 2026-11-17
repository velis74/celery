"""Tasks run by the worker of :mod:`t.integration.test_async_worker`.

The broker, the result backend and the async pool settings are read from
the environment, so the worker process and the tests use the same app.
"""
import asyncio
import functools
import os
import signal
import time

from celery import Celery
from celery.concurrency import current_process_capacity, current_process_kind
from celery.exceptions import SoftTimeLimitExceeded
from celery.signals import worker_process_init

BROKER = os.environ.get('ASYNC_TEST_BROKER', 'redis://127.0.0.1:6390/0')
BACKEND = os.environ.get('ASYNC_TEST_BACKEND', 'redis://127.0.0.1:6390/1')

app = Celery('async_tasks', broker=BROKER, backend=BACKEND)
app.conf.update(
    worker_async_recycle_timeout=float(
        os.environ.get('ASYNC_TEST_RECYCLE_TIMEOUT', '5')),
    worker_async_block_warning=1.0,
    worker_hijack_root_logger=True,
    broker_connection_retry_on_startup=True,
    result_expires=600,
)


@app.task(bind=True)
async def asleep(self, seconds):
    """Sleep, then return the id the task sees and the process id."""
    await asyncio.sleep(seconds)
    return [self.request.id, os.getpid()]


@app.task
def add(x, y):
    return [x + y, os.getpid()]


@app.task
async def araise():
    await asyncio.sleep(0)
    raise ValueError('boom')


# The retried run starts after the first run has stored RETRY, which
# would otherwise overwrite its result.
@app.task(bind=True, max_retries=3, default_retry_delay=1)
async def aretry(self):
    await asyncio.sleep(0)
    if self.request.retries < 1:
        raise self.retry()
    return [self.request.id, self.request.retries]


@app.task(soft_time_limit=0.5)
async def asoft(seconds):
    await asyncio.sleep(seconds)
    return 'not reached'


@app.task(soft_time_limit=0.5)
async def asoft_catch(seconds):
    try:
        await asyncio.sleep(seconds)
    except (asyncio.CancelledError, SoftTimeLimitExceeded):
        return 'cancelled'
    return 'not reached'


@app.task(time_limit=1)
async def ahard(seconds):
    await asyncio.sleep(seconds)
    return 'not reached'


@app.task(time_limit=1)
async def ahard_stubborn(seconds):
    """Ignore cancellation until ``seconds`` have passed."""
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        try:
            await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            pass
    return 'not reached'


@app.task(time_limit=2)
async def ablock(seconds):
    """Block the event loop."""
    time.sleep(seconds)
    return 'not reached'


@app.task
async def acrash():
    await asyncio.sleep(0.2)
    os.kill(os.getpid(), signal.SIGKILL)


@app.task(bind=True, acks_late=True, reject_on_worker_lost=True)
async def acrash_once(self, flag_path):
    """Kill the process on the first delivery only."""
    await asyncio.sleep(0.2)
    try:
        fd = os.open(flag_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        return ['survived', os.getpid()]
    os.close(fd)
    os.kill(os.getpid(), signal.SIGKILL)


# Process kind and capacity seen by ``worker_process_init``, by process id.
PROCESS_INIT_VALUES = {}


@worker_process_init.connect
def record_process_kind(**kwargs):
    PROCESS_INIT_VALUES[os.getpid()] = [
        current_process_kind(), current_process_capacity()]


@app.task
def process_kind():
    return [current_process_kind(), current_process_capacity()]


@app.task
async def aprocess_kind():
    await asyncio.sleep(0)
    return [current_process_kind(), current_process_capacity()]


@app.task
def process_init_values():
    """Values recorded at process init and current values, in this process."""
    return [PROCESS_INIT_VALUES.get(os.getpid()),
            [current_process_kind(), current_process_capacity()]]


@app.task
async def aprocess_init_values():
    await asyncio.sleep(0)
    return [PROCESS_INIT_VALUES.get(os.getpid()),
            [current_process_kind(), current_process_capacity()]]


# Sync tasks with an async implementation in ``__celery_async__``.

def _hybrid(seconds=0):
    time.sleep(seconds)
    return ['sync', current_process_kind()]


async def _ahybrid(seconds=0):
    await asyncio.sleep(seconds)
    return ['async', current_process_kind()]


_hybrid.__celery_async__ = _ahybrid
hybrid = app.task(name='async_tasks.hybrid')(_hybrid)


def _hybrid_bound(self, seconds):
    time.sleep(seconds)
    return ['sync', self.request.id]


async def _ahybrid_bound(self, seconds):
    await asyncio.sleep(seconds)
    return ['async', self.request.id]


_hybrid_bound.__celery_async__ = _ahybrid_bound
hybrid_bound = app.task(bind=True, name='async_tasks.hybrid_bound')(
    _hybrid_bound)


def _hybrid_soft(seconds):
    return 'sync'


async def _ahybrid_soft(seconds):
    try:
        await asyncio.sleep(seconds)
    except asyncio.CancelledError:
        return 'soft limit'
    return 'not reached'


_hybrid_soft.__celery_async__ = _ahybrid_soft
hybrid_soft = app.task(soft_time_limit=0.5, name='async_tasks.hybrid_soft')(
    _hybrid_soft)


def _hybrid_soft_fail(seconds):
    return 'sync'


async def _ahybrid_soft_fail(seconds):
    await asyncio.sleep(seconds)
    return 'not reached'


_hybrid_soft_fail.__celery_async__ = _ahybrid_soft_fail
hybrid_soft_fail = app.task(
    soft_time_limit=0.5, name='async_tasks.hybrid_soft_fail')(
    _hybrid_soft_fail)


def _hybrid_hard(seconds):
    return 'sync'


async def _ahybrid_hard(seconds):
    """Ignore cancellation until ``seconds`` have passed."""
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        try:
            await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            pass
    return 'not reached'


_hybrid_hard.__celery_async__ = _ahybrid_hard
hybrid_hard = app.task(time_limit=1, name='async_tasks.hybrid_hard')(
    _hybrid_hard)


def _wrapped_target():
    return ['sync', current_process_kind()]


async def _awrapped_target():
    await asyncio.sleep(0)
    return ['async', current_process_kind()]


_wrapped_target.__celery_async__ = _awrapped_target


@app.task(name='async_tasks.wrapped')
@functools.wraps(_wrapped_target)
def wrapped():
    return _wrapped_target()
