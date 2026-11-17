.. _guide-async:

=============
 Async Tasks
=============

:Release: |version|
:Date: |today|

.. contents::
    :local:
    :depth: 2

.. _async-installation:

Installation
============

Async tasks are available in the ``async-worker`` branch of the
``velis74/celery`` fork, version ``5.7.0+velis1``, which is based on Celery
5.7.0. It is installed from git:

uv, in ``pyproject.toml``:

.. code-block:: toml

    [project]
    dependencies = ["celery[redis]"]

    [tool.uv.sources]
    celery = { git = "https://github.com/velis74/celery", branch = "async-worker" }

pip, with a PEP 508 direct reference (also usable in a requirements file):

.. code-block:: console

    $ pip install "celery[redis] @ git+https://github.com/velis74/celery@async-worker"

Poetry, in ``pyproject.toml``:

.. code-block:: toml

    [tool.poetry.dependencies]
    celery = { git = "https://github.com/velis74/celery", branch = "async-worker", extras = ["redis"] }

Use ``rev = "<commit>"`` (uv, Poetry) or ``@<commit>`` (pip) in place of the
branch to pin a commit.

.. _async-overview:

Overview
========

A task whose body is an ``async def`` function runs on an :mod:`asyncio`
event loop when the worker is started with
:option:`--aconcurrency <celery worker --aconcurrency>`:

.. code-block:: console

    $ celery -A proj worker --concurrency=4 --aconcurrency=200

This worker has two kinds of child processes:

- ``--concurrency`` prefork processes, one task each. They run every task
  that is not an ``async def`` task, as without ``--aconcurrency``.
- ``--aprocesses`` async processes (default 1). Each runs an event loop and
  up to ``ceil(aconcurrency / aprocesses)`` ``async def`` tasks at the same
  time. In total, at most ``--aconcurrency`` async tasks run at once.

Sync tasks never run in async processes, and async tasks never run in
prefork processes. The parent process consumes messages, applies the time
limits and sends each task to a process of its kind, as with the prefork pool.

Async tasks suit work that waits on I/O: HTTP calls, database queries
through an async driver, calls to other services. A single process runs
hundreds of such tasks while they wait. CPU-bound work belongs in sync tasks.

Without ``--aconcurrency``, the worker behaves as a plain prefork worker.

.. _async-defining:

Defining async tasks
====================

Decorate an ``async def`` function with :meth:`@task`:

.. code-block:: python

    import httpx

    from proj.celery import app


    @app.task(bind=True, soft_time_limit=10, time_limit=30)
    async def fetch(self, url):
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
        return {'id': self.request.id, 'status': response.status_code}


    @app.task
    def resize(path):
        ...  # CPU-bound: runs in a prefork process

A task is an async task when its ``run`` method, or a custom ``__call__``,
is a coroutine function, also behind decorators that set ``__wrapped__``
(:func:`functools.wraps`). Clients call async tasks as any other task:
``fetch.delay(url)``, ``fetch.apply_async(...)``, in canvas primitives, and
with :meth:`~@Task.retry`.

Inside an async task:

- ``self.request`` is the request of that task, also while hundreds of other
  tasks run in the same process: the request stack is held in
  :mod:`contextvars`. :meth:`~@Task.retry`, :meth:`~@Task.update_state` and
  the task id in log records refer to the running task.
- ``await other_task(*args)`` runs the body of another async task in the
  current coroutine, with its own request pushed.
- :meth:`~@Task.apply` of an async task fails with :exc:`RuntimeError` in
  the event loop thread, as it cannot wait there. Await the task instead.
- Exceptions behave as in sync tasks: a raised exception records the task as
  failed, and :exc:`~celery.exceptions.Retry`,
  :exc:`~celery.exceptions.Reject` and :exc:`~celery.exceptions.Ignore` are
  handled as usual, as are ``autoretry_for`` and the pydantic argument
  options.

Calling an async task returns a coroutine. A worker without
``--aconcurrency`` runs it as a sync task and stores the coroutine object as
the result, which fails with the JSON serializer
(:exc:`~kombu.exceptions.EncodeError`). :meth:`~@Task.apply` and
:setting:`task_always_eager` also return the coroutine object as the result,
unless an awaiter is installed with
:func:`celery.app.trace.set_task_awaiter`.

.. _async-starting:

Starting the worker
===================

.. list-table::
    :header-rows: 1

    * - Command-line option
      - Setting
      - Default
    * - ``--aconcurrency``
      - :setting:`worker_async_concurrency`
      - 0 (disabled)
    * - ``--aprocesses``
      - :setting:`worker_async_processes`
      - 1
    * -
      - :setting:`worker_async_recycle_timeout`
      - 30.0
    * -
      - :setting:`worker_async_block_warning`
      - 1.0

Requirements:

- Python 3.10 or later.
- The ``prefork`` pool (the default). The worker replaces it with
  ``celery.concurrency.aio:TaskPool``. Any other pool raises
  :exc:`~celery.exceptions.ImproperlyConfigured`, unless its class sets
  ``supports_async_concurrency = True``.
- A broker transport that supports the worker's event loop, such as RabbitMQ
  (``amqp``) and Redis.
- No :option:`--autoscale <celery worker --autoscale>`.

The banner shows both kinds of concurrency:

.. code-block:: text

    .> concurrency: 4 (aio), async: 200 in 1 process(es)

:program:`celery inspect stats` lists the async processes in the ``pool``
section:

.. code-block:: javascript

    "async-concurrency": 200,
    "async-processes": [
        {"pid": 4211, "in-flight": 37, "done": 1840, "recycling": null}
    ],
    "free-slots": {"sync": 4, "async": 163}

``recycling`` is ``null``, or the reason a process is being
:ref:`recycled <async-recycling>`: ``"time limit"``, ``"max tasks"`` or
``"max memory"``.

.. _async-execution:

How async tasks run
===================

The tracer of each async task runs in a thread of a thread pool in the async
process, with ``ceil(aconcurrency / aprocesses)`` threads. The tracer sends
the task signals, calls the task and runs the returned coroutine as an
:class:`asyncio.Task` on the event loop of the process, then stores the
result. So:

- :signal:`task_prerun`, :signal:`task_postrun`, :signal:`task_success`,
  :signal:`task_failure` and :signal:`task_retry` handlers run in a tracer
  thread, not in the event loop thread, and run concurrently for different
  tasks. They must be thread-safe. A slow handler delays its own task only.
- The result backend is called from the tracer thread and does not block the
  event loop.
- Each task starts in a new :class:`contextvars.Context`. Context variables
  set by one task are not visible to another. :class:`threading.local`
  values set in a coroutine are shared by all tasks of the process, as they
  all run in the event loop thread.
- :signal:`worker_process_init` and :signal:`worker_process_shutdown` are
  sent in async processes too.

.. _async-blocking:

Blocking the event loop
-----------------------

All async tasks of a process share one event loop. A blocking call in a
coroutine (:func:`time.sleep`, a synchronous HTTP client, a CPU-bound loop)
stops every other task of that process until it returns, including the
cancellation of tasks over their time limits.

Run blocking code in a thread:

.. code-block:: python

    @app.task
    async def report(path):
        data = await asyncio.to_thread(parse_large_file, path)
        ...

When the event loop has not run for :setting:`worker_async_block_warning`
seconds, the process logs a warning with the stack of the event loop thread,
once per blocking period:

.. code-block:: text

    [WARNING/ForkPoolWorker-3] The event loop of process 4211 has not run for 1.05 seconds; a task blocks it.
    Event loop thread stack:
      ...
      File "proj/tasks.py", line 153, in report
        time.sleep(seconds)

.. _async-limits:

Time limits and revocation
==========================

Time limits are set as for any task: :setting:`task_soft_time_limit`,
:setting:`task_time_limit`, the ``soft_time_limit`` and ``time_limit``
task options, and the ``--soft-time-limit`` and ``--time-limit`` worker
options. The parent process applies them per task.

Soft time limit
    The coroutine of the task is cancelled: it gets
    :exc:`asyncio.CancelledError` at its current ``await``. The task fails
    with :exc:`~celery.exceptions.SoftTimeLimitExceeded`. A coroutine that
    catches :exc:`asyncio.CancelledError` and returns a value succeeds with
    that value. Other tasks of the process are not affected.

Hard time limit
    The task fails with :exc:`~celery.exceptions.TimeLimitExceeded` at the
    limit, its coroutine is cancelled, and its process is
    :ref:`recycled <async-recycling>`. Other tasks of that process keep
    running.

``revoke(terminate=True)``
    The coroutine of the task is cancelled and the task is recorded as
    ``REVOKED``. No signal is sent to the process; the ``signal`` argument
    is only reported as ``signum`` in the ``task-revoked`` event. Other tasks
    of the process are not affected.

A coroutine that suppresses cancellation keeps running and keeps its slot
until it returns or exceeds the hard time limit. After the hard time limit
and ``revoke(terminate=True)``, what such a coroutine returns or raises is
discarded: the task keeps its ``FAILURE`` or ``REVOKED`` state, and no task
signals are sent for it. What it does before it ends still takes effect: a
:meth:`~@Task.retry`, also one made by ``autoretry_for`` for an exception
raised while handling the cancellation, publishes a new message.

.. _async-recycling:

Recycling processes
===================

An async process is recycled:

- when one of its tasks exceeds the hard time limit;
- after :setting:`worker_max_tasks_per_child` tasks
  (:option:`--max-tasks-per-child <celery worker --max-tasks-per-child>`),
  counted by the parent;
- when its resident memory exceeds :setting:`worker_max_memory_per_child`
  (:option:`--max-memory-per-child <celery worker --max-memory-per-child>`).
  The process checks its memory after each task, so its use can exceed the
  limit by what the tasks running at that moment allocate.

A recycled process gets no new tasks, and a replacement process starts at
once, so the async capacity stays the same. The recycled process exits when
its running tasks have finished. It is killed:

- 1 second after only tasks over the hard time limit remain in it, or
- :setting:`worker_async_recycle_timeout` seconds after the recycling
  started, for example when a blocking call stops its event loop.

Tasks still running in a killed process fail with
:exc:`~celery.exceptions.WorkerLostError`. Exits of recycled processes do
not count toward the restart frequency limit of the pool.

The worker logs each step:

.. code-block:: text

    [INFO/MainProcess] Recycling async pool process 4211 (time limit), 4 job(s) left
    [WARNING/MainProcess] Killing recycled async pool process 4211: its jobs did not end within 30.0s; 2 job(s) within the time limit are lost

.. _async-lost:

Lost processes and redelivery
=============================

When an async process exits unexpectedly (killed by the OOM killer, a
segmentation fault in an extension, :data:`signal.SIGKILL`), every task
running in it fails with :exc:`~celery.exceptions.WorkerLostError`: up to
``ceil(aconcurrency / aprocesses)`` tasks, against one task for a prefork
process. A replacement process starts.

The messages of these tasks are acknowledged when the tasks start, unless
:setting:`task_acks_late` is enabled. To run them again, enable both
:setting:`task_acks_late` and :setting:`task_reject_on_worker_lost` (or the
``acks_late`` and ``reject_on_worker_lost`` task options): the worker then
returns the message to the queue, and the task runs again.

When the whole worker is killed, the broker redelivers the unacknowledged
messages of :setting:`task_acks_late` tasks: RabbitMQ when the connection
closes, Redis after the ``visibility_timeout`` of
:setting:`broker_transport_options` (one hour by default). Tasks that run
again must be idempotent, as with prefork.

.. _async-prefetch:

Prefetch and routing
====================

The initial prefetch count is ``(concurrency + aconcurrency) *``
:setting:`worker_prefetch_multiplier`. Both kinds of tasks share it: a queue
full of one kind fills the prefetch with that kind, and the other kind waits
for a slot while its processes idle.

When one kind of task can outnumber the other, route sync and async tasks to
separate queues (see :ref:`guide-routing`) and consume them with separate
workers:

.. code-block:: console

    $ celery -A proj worker -Q default --concurrency=8 -n sync@%h
    $ celery -A proj worker -Q io --concurrency=1 --aconcurrency=200 -n io@%h

.. _async-shutdown:

Shutdown
========

On warm shutdown (:data:`signal.SIGTERM`), async processes stop taking new
tasks and exit once their running tasks have finished. The worker waits for
both kinds of processes.

.. _async-django:

Django
======

With async concurrency enabled, the Django fixup runs the coroutine of each
async task in an ``asgiref.sync.ThreadSensitiveContext`` of its own. Code
that the task runs with :func:`asgiref.sync.sync_to_async` in thread-sensitive
mode (the default), including the async methods of the ORM, runs in a thread
of the task. When the task ends, the database connections and caches of that
thread are closed and the thread exits; ``CONN_MAX_AGE`` and
``CELERY_DB_REUSE_MAX`` do not apply to them. A task that runs no such code
starts no thread.

The fixup does not close connections in the tracer thread before and after an
async task, and does nothing in a thread that runs an event loop, where
Django raises :exc:`~django.core.exceptions.SynchronousOnlyOperation`.

Other code can wrap the coroutines of async tasks in the same way with
:func:`celery.concurrency.aioworker.add_task_wrapper`, called in the worker
before the pool starts, for example in a :signal:`worker_init` handler.

.. _async-example:

Example
=======

``examples/async_worker`` in the Celery repository runs a classic worker and
a worker with ``--aconcurrency`` through these scenarios: throughput of
I/O-bound tasks, time limits, revocation, a blocked event loop, killed
processes, a killed worker, recycling and warm shutdown.
