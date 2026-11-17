"""Tasks run by the worker of :mod:`t.integration.test_async_worker`.

The broker, the result backend and the async pool settings are read from
the environment, so the worker process and the tests use the same app.
"""
import asyncio
import os
import signal
import time

from celery import Celery
from celery.exceptions import SoftTimeLimitExceeded

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
