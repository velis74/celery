"""Tasks run by the workers of :mod:`t.integration.test_context_stack`.

Each task reports the task and request it sees through the public API.
The broker and the result backend are read from the environment, so the
worker process and the tests use the same app.
"""
import asyncio
import os
import time

from celery import Celery, current_task

BROKER = os.environ.get('CONTEXT_STACK_TEST_BROKER', 'redis://127.0.0.1:6390/0')
BACKEND = os.environ.get('CONTEXT_STACK_TEST_BACKEND', 'redis://127.0.0.1:6390/1')

app = Celery('context_stack_tasks', broker=BROKER, backend=BACKEND)
app.conf.update(
    broker_connection_retry_on_startup=True,
    result_expires=600,
    worker_prefetch_multiplier=1,
)


def name(task):
    return None if task is None else task.name


def seen(task):
    """Return what task code sees: current task, its request, worker task, own request."""
    current = current_task._get_current_object()
    return [name(current), current.request.id if current is not None else None,
            name(app.current_worker_task), task.request.id]


@app.task(bind=True)
def whoami(self, sleep=0):
    """Report what the task sees before and after sleeping ``sleep`` seconds."""
    start = time.monotonic()
    before = seen(self)
    if sleep:
        time.sleep(sleep)
    after = seen(self)
    return {'pid': os.getpid(), 'before': before, 'after': after,
            'start': start, 'end': time.monotonic()}


@app.task(bind=True)
def nested(self):
    """Report what the task sees around a nested ``apply()`` and direct call."""
    before = seen(self)
    inner = whoami.apply().result
    direct = whoami()
    return {'pid': os.getpid(), 'before': before, 'inner': inner,
            'direct': direct, 'after': seen(self)}


@app.task(bind=True)
def other_request(self, task_name):
    """Return the request id of another task, and what this task sees."""
    return {'pid': os.getpid(), 'other': app.tasks[task_name].request.id,
            'seen': seen(self)}


@app.task(bind=True)
async def awhoami(self, sleep=0):
    """Report what the async task sees before and after sleeping ``sleep`` seconds."""
    start = time.monotonic()
    before = seen(self)
    await asyncio.sleep(sleep)
    after = seen(self)
    return {'pid': os.getpid(), 'before': before, 'after': after,
            'start': start, 'end': time.monotonic()}
