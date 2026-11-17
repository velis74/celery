"""Tasks of the async worker demo.

Every scenario exists as a sync task (run by the prefork processes) and as
an ``async def`` task (run by the async processes when the worker is started
with ``--aconcurrency``).

The broker and the result backend are read from ``DEMO_BROKER`` (default
``redis://127.0.0.1:6390/2``) and ``DEMO_BACKEND`` (default
``redis://127.0.0.1:6390/3``).
"""
import asyncio
import os
import time

from celery import Celery
from celery.exceptions import SoftTimeLimitExceeded

BROKER = os.environ.get('DEMO_BROKER', 'redis://127.0.0.1:6390/2')
BACKEND = os.environ.get('DEMO_BACKEND', 'redis://127.0.0.1:6390/3')

app = Celery('tasks', broker=BROKER, backend=BACKEND)
app.conf.update(
    # Redis redelivers a message that is not acknowledged within
    # visibility_timeout seconds, checked every second by a running worker.
    # Tasks with acks_late must finish well within it.  RabbitMQ ignores
    # these options and redelivers when the connection closes.
    broker_transport_options={
        'visibility_timeout': 10,
        'unacked_restore_interval': 1,
        'unacked_restore_throttle': 1,
    },
    broker_connection_retry_on_startup=True,
    result_expires=3600,
    worker_async_recycle_timeout=5.0,
    worker_async_block_warning=1.0,
)


def record_start(task, marker_dir):
    """Count the starts of the current task and publish its process id.

    Returns the number of times the task has started, redeliveries included.
    """
    path = os.path.join(marker_dir, task.request.id)
    with open(path, 'a') as fh:
        fh.write(f'{os.getpid()}\n')
    with open(path) as fh:
        starts = len(fh.readlines())
    task.update_state(state='PROGRESS', meta={'pid': os.getpid(), 'starts': starts})
    return starts


# I/O-bound work.

@app.task
def io_sleep(seconds):
    time.sleep(seconds)
    return os.getpid()


@app.task
async def aio_sleep(seconds):
    await asyncio.sleep(seconds)
    return os.getpid()


# CPU-bound sync work; always runs in a prefork process.

@app.task
def cpu_work(n):
    total = 0
    for i in range(n):
        total += i * i
    return [total, os.getpid()]


# Task context: each task sees its own request.

@app.task(bind=True)
def echo_id(self, seconds):
    time.sleep(seconds)
    return self.request.id


@app.task(bind=True)
async def aecho_id(self, seconds):
    await asyncio.sleep(seconds)
    return self.request.id


@app.task(bind=True)
async def anested(self):
    """Await another async task; the own request is restored afterwards."""
    before = self.request.id
    inner = await aecho_id(0)
    return [before, inner, self.request.id]


# Failures and retries.

@app.task
def fail():
    raise ValueError('sync task failed')


@app.task
async def afail():
    await asyncio.sleep(0)
    raise ValueError('async task failed')


# The retried run starts after the first run has stored RETRY, which
# would otherwise overwrite its result.
@app.task(bind=True, max_retries=3, default_retry_delay=1)
def retry_once(self):
    if self.request.retries < 1:
        raise self.retry()
    return self.request.retries


# The retried run starts after the first run has stored RETRY, which
# would otherwise overwrite its result.
@app.task(bind=True, max_retries=3, default_retry_delay=1)
async def aretry_once(self):
    await asyncio.sleep(0)
    if self.request.retries < 1:
        raise self.retry()
    return self.request.retries


# Time limits.

@app.task(soft_time_limit=0.5)
def soft_limited(seconds):
    time.sleep(seconds)
    return 'not reached'


@app.task(soft_time_limit=0.5)
async def asoft_limited(seconds):
    await asyncio.sleep(seconds)
    return 'not reached'


@app.task(soft_time_limit=0.5)
async def asoft_cleanup(seconds):
    """Catch the cancellation of the soft time limit and return a value."""
    try:
        await asyncio.sleep(seconds)
    except (asyncio.CancelledError, SoftTimeLimitExceeded):
        return 'cleaned up'
    return 'not reached'


@app.task(time_limit=1)
def hard_limited(seconds):
    time.sleep(seconds)
    return 'not reached'


@app.task(time_limit=1)
async def ahard_limited(seconds):
    await asyncio.sleep(seconds)
    return 'not reached'


# Blocking call inside a coroutine: stops every task of its process.

@app.task
async def ablock_loop(seconds):
    time.sleep(seconds)
    return os.getpid()


# Slow tasks for killing processes and for shutdown.

@app.task(bind=True)
def slow(self, seconds, marker_dir):
    starts = record_start(self, marker_dir)
    time.sleep(seconds)
    return {'pid': os.getpid(), 'starts': starts}


@app.task(bind=True)
async def aslow(self, seconds, marker_dir):
    starts = record_start(self, marker_dir)
    await asyncio.sleep(seconds)
    return {'pid': os.getpid(), 'starts': starts}


@app.task(bind=True, acks_late=True, reject_on_worker_lost=True)
def slow_late(self, seconds, marker_dir):
    starts = record_start(self, marker_dir)
    time.sleep(seconds)
    return {'pid': os.getpid(), 'starts': starts}


@app.task(bind=True, acks_late=True, reject_on_worker_lost=True)
async def aslow_late(self, seconds, marker_dir):
    starts = record_start(self, marker_dir)
    await asyncio.sleep(seconds)
    return {'pid': os.getpid(), 'starts': starts}
