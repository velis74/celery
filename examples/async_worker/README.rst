=====================
 Async worker demo
=====================

Runs a classic prefork worker and a worker with ``--aconcurrency`` through
the same scenarios and prints what each one does. See
``docs/userguide/async.rst`` for the feature itself.

Files
=====

``tasks.py``
    The Celery app and its tasks. Every scenario has a sync task, run by the
    prefork processes, and an ``async def`` task, run by the async processes.

``run_demo.py``
    Starts a worker subprocess for each scenario, sends tasks, checks the
    outcome and prints a report table. It exits with code 0 when every check
    passes.

Requirements
============

- Python 3.10 or later, Celery from this repository with the ``redis``
  extra (``pip install -e '.[redis]'`` in the repository root).
- A Redis server or a RabbitMQ broker, and a Redis result backend.

The broker and the result backend are read from the environment:

``DEMO_BROKER``
    Default ``redis://127.0.0.1:6390/2``.

``DEMO_BACKEND``
    Default ``redis://127.0.0.1:6390/3``.

Running
=======

.. code-block:: console

    $ cd examples/async_worker
    $ python run_demo.py                          # both modes, all scenarios
    $ python run_demo.py --mode async --scenario throughput,hard_limit
    $ DEMO_BROKER=amqp://guest:guest@localhost:5672// python run_demo.py

Options:

``--mode classic|async|both``
    ``classic`` starts ``celery worker --concurrency 8`` and runs sync tasks.
    ``async`` starts ``celery worker --concurrency 2 --aconcurrency 200
    --aprocesses 2`` and runs async tasks, and sync tasks where a scenario
    mixes them. Default ``both``.

``--scenario NAME[,NAME...]|all``
    Default ``all``.

``--io-tasks N``, ``--io-seconds S``
    Size of the throughput scenario. Default 500 tasks of 0.5 seconds.

``--keep-logs``
    Keep the worker logs. They are always kept when a check fails; the
    report prints their directory.

Each worker consumes its own queue, which is deleted afterwards, and runs in
its own process group. The script kills all worker processes when it exits,
also on Ctrl+C. A full run of both modes takes about three minutes, most of
it in the classic throughput scenario.

Configuration
=============

``tasks.py`` sets:

- ``visibility_timeout`` of 10 seconds, checked every second
  (``unacked_restore_interval`` and ``unacked_restore_throttle`` of 1), so
  Redis redelivers the messages of a killed worker within seconds. With the
  default of one hour, the ``kill_worker`` scenario would wait an hour.
  Tasks with ``acks_late`` must finish within this time, otherwise Redis
  delivers them again while they run.
- ``worker_async_recycle_timeout`` of 5 seconds.

Scenarios
=========

``basics``
    Request context, failure and retry. Each of 16 (classic) or 50 (async)
    concurrent tasks returns ``self.request.id``, which must be its own id. A
    failing task ends as ``FAILURE ValueError``, a task that retries once
    ends as ``SUCCESS`` with ``retries=1``. In async mode, an async task
    awaits another async task and gets its own request back afterwards, and
    4 CPU-bound sync tasks run next to 20 async tasks in different processes.

``throughput``
    500 tasks that each sleep 0.5 seconds: ``time.sleep`` in a sync task on
    8 prefork processes, ``asyncio.sleep`` in an async task with an async
    concurrency of 200. Expected: about 32 seconds against about 2 seconds.

``soft_limit``
    A task with ``soft_time_limit=0.5`` that sleeps 3 seconds fails with
    ``SoftTimeLimitExceeded`` after about 0.5 seconds; 3 other tasks
    succeed. In async mode, a coroutine that catches the cancellation returns
    its own value and succeeds.

``hard_limit``
    A task with ``time_limit=1`` that sleeps 5 seconds fails with
    ``TimeLimitExceeded`` after about 1 second; the other tasks running at
    the same time succeed, also those in the same async process. Classic
    mode logs ``Hard time limit (1s) exceeded`` and replaces the killed
    process; async mode logs ``Recycling async pool process ... (time
    limit)`` and replaces the recycled process. Afterwards 8 tasks of 0.5
    seconds take about 0.5 seconds: the capacity is unchanged.

``revoke``
    ``revoke(terminate=True)`` of one running task: it ends as ``REVOKED``
    within about 0.05 seconds; the other tasks succeed, also those in the
    same async process.

``watchdog`` (async only)
    An async task calls ``time.sleep(1.5)``, which blocks the event loop. The
    process logs ``The event loop of process ... has not run for ...
    seconds`` with the stack of the event loop thread; the report shows the
    innermost frame, the blocking call in ``tasks.py``.

``kill_child``
    Tasks of 3 seconds run (4 in classic mode, 10 over 2 async processes);
    the process running the most of them is killed with ``SIGKILL``.
    Without ``acks_late`` the tasks of that process fail with
    ``WorkerLostError``: 1 in classic mode, about 5 in async mode. With
    ``acks_late=True`` and ``reject_on_worker_lost=True`` they run again and
    succeed; the report counts the tasks that started twice.

``kill_worker``
    Tasks with ``acks_late=True`` run (6 classic, 20 async) when the whole
    worker, main process and children, is killed with ``SIGKILL``. The
    worker is started again; the broker redelivers the messages and all
    tasks succeed, each having started twice. With Redis this takes the
    visibility timeout plus the task time, about 14 seconds.

``max_tasks``
    A worker with ``--max-tasks-per-child 3``. Classic: 12 sync tasks on 2
    processes. Async: 24 async tasks with ``--aconcurrency 10
    --aprocesses 2``. No process runs more than 3 tasks.

``shutdown``
    ``SIGTERM`` while tasks of 3 seconds run (8 classic, 50 async). The
    worker waits for them: all succeed, and the worker exits with code 0
    after about 3 to 5 seconds.

Expected output
===============

A run of both modes ends with a table like this (process ids, ids and
times vary):

.. code-block:: text

    ====================================================================================================
    Scenario     Mode     Check                        Result  Detail
    ----------------------------------------------------------------------------------------------------
    throughput   classic  wall time                    PASS    500 x sleep(0.5s), concurrency 8: 31.67s (16 tasks/s), ...
    kill_child   classic  SIGKILL child                PASS    1 of 4 tasks on killed pid 128578: 1 FAILURE WorkerLostError; ...
    ...
    throughput   async    wall time                    PASS    500 x sleep(0.5s), aconcurrency 200: 1.76s (285 tasks/s), ...
    kill_child   async    SIGKILL child                PASS    5 of 10 tasks on killed pid 130664: 5 FAILURE WorkerLostError; ...
    kill_child   async    ... with acks_late           PASS    5 of 10 tasks on killed pid 130702: 5 SUCCESS, 5 ran twice; ...
    ...
    ====================================================================================================
    Throughput: classic 31.67s, async 1.76s (18.0x faster)
    33 of 33 checks passed or n/a, 0 failed.
