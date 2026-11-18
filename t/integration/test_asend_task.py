"""Tasks published with :meth:`Celery.asend_task` and run by a real worker.

The worker runs :mod:`t.integration.async_tasks` in a subprocess.  The
Redis tests use the broker in ``ASYNC_TEST_BROKER`` (default
``redis://127.0.0.1:6390/0``), the RabbitMQ tests the broker in
``ASEND_TEST_AMQP_BROKER`` (default ``amqp://guest:guest@127.0.0.1:5673//``);
both use the result backend in ``ASYNC_TEST_BACKEND`` (default
``redis://127.0.0.1:6390/1``).  A test is skipped when its broker or the
result backend is unreachable.
"""
import asyncio
import os
import signal
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

import pytest
from kombu import Connection, Exchange, Queue
from kombu.exceptions import OperationalError

import t.skip
from celery import Celery
from celery.exceptions import TimeoutError
from celery.result import AsyncResult
from t.integration import async_tasks as tasks

pytestmark = t.skip.if_win32

ROOT = Path(__file__).resolve().parents[2]
REDIS_BROKER = tasks.BROKER
AMQP_BROKER = os.environ.get(
    'ASEND_TEST_AMQP_BROKER', 'amqp://guest:guest@127.0.0.1:5673//')
BACKEND = tasks.BACKEND
WORKER_START_TIMEOUT = 60
WORKER_STOP_TIMEOUT = 30
RESULT_TIMEOUT = 60


def _unreachable(broker):
    try:
        with Connection(broker, connect_timeout=2) as conn:
            conn.ensure_connection(max_retries=1, timeout=2)
        tasks.app.backend.client.ping()
    except Exception as exc:  # pylint: disable=broad-except
        return repr(exc)
    return None


def make_client(broker, **conf):
    """A client app publishing to ``broker``, separate from the worker's."""
    app = Celery(f'asend-client-{uuid.uuid4().hex[:6]}',
                 broker=broker, backend=BACKEND, set_as_current=False)
    app.conf.update(broker_connection_retry_on_startup=True, **conf)
    return app


@pytest.fixture
def redis_app():
    error = _unreachable(REDIS_BROKER)
    if error is not None:
        pytest.skip(f'Redis broker or result backend unreachable: {error}')
    app = make_client(REDIS_BROKER)
    yield app
    app.close()


@pytest.fixture
def amqp_app():
    error = _unreachable(AMQP_BROKER)
    if error is not None:
        pytest.skip(f'AMQP broker or result backend unreachable: {error}')
    app = make_client(AMQP_BROKER)
    yield app
    app.close()


class Worker:
    """A worker subprocess consuming its own queue on ``app``'s broker."""

    def __init__(self, tmp_path, app, *args, queue=None):
        self.app = app
        self.name = f'asendit-{uuid.uuid4().hex[:10]}'
        if isinstance(queue, Queue):
            self.queues = [Queue(queue.name), queue]
            self.queue = queue.name
        else:
            self.queue = queue or self.name
            self.queues = [Queue(self.queue)]
        self.log = tmp_path / f'{self.name}.log'
        self.args = args
        self.proc = None

    def start(self):
        env = dict(os.environ, PYTHONPATH=str(ROOT),
                   ASYNC_TEST_BROKER=self.app.conf.broker_url,
                   ASYNC_TEST_BACKEND=BACKEND)
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
                replies = self.app.control.inspect(timeout=1).ping() or {}
                if any(n.startswith(self.name + '@') for n in replies):
                    return self
            time.sleep(0.2)
        log = self.log.read_text(errors='replace')
        self.stop()
        pytest.fail(f'Worker did not start:\n{log}')

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
        # A virtual transport deletes only the bindings its channel knows,
        # so each binding is declared before the queue is deleted.
        try:
            with Connection(self.app.conf.broker_url) as conn:
                for queue in self.queues:
                    bound = queue(conn.default_channel)
                    bound.declare()
                    bound.delete()
        except Exception:  # pylint: disable=broad-except
            pass
        if self.app.conf.broker_url.startswith('redis'):
            delete_redis_keys(f'*{self.queue}*')


def delete_redis_keys(pattern):
    with Connection(REDIS_BROKER) as conn:
        client = conn.default_channel.client
        for key in client.scan_iter(pattern):
            client.delete(key)


@pytest.fixture
def make_worker(tmp_path):
    workers = []

    def make(app, *args, queue=None, start=True):
        worker = Worker(tmp_path, app, *args, queue=queue)
        workers.append(worker)
        return worker.start() if start else worker

    yield make
    for worker in workers:
        worker.stop()


def wait_ready(results, timeout=RESULT_TIMEOUT):
    deadline = time.monotonic() + timeout
    while not all(r.ready() for r in results):
        if time.monotonic() > deadline:
            pending = sum(not r.ready() for r in results)
            pytest.fail(f'{pending} of {len(results)} results not ready')
        time.sleep(0.05)


# -- Redis broker ------------------------------------------------------------


def test_asend_task_runs_on_worker(redis_app, make_worker):
    worker = make_worker(redis_app, '-c', '2')

    async def publish():
        return await redis_app.asend_task(
            tasks.add.name, args=(2, 3), queue=worker.queue)

    result = asyncio.run(publish())
    assert isinstance(result, AsyncResult)
    assert result.app is redis_app
    value, _pid = result.get(timeout=RESULT_TIMEOUT)
    assert value == 5
    assert result.state == 'SUCCESS'
    assert result.successful()


def test_asend_task_result_matches_send_task(redis_app, make_worker):
    worker = make_worker(redis_app, '-c', '2')
    sent = redis_app.send_task(tasks.add.name, args=(4, 5), queue=worker.queue)

    async def publish():
        return await redis_app.asend_task(
            tasks.add.name, args=(4, 5), queue=worker.queue)

    asent = asyncio.run(publish())
    assert type(asent) is type(sent)
    assert type(asent.backend) is type(sent.backend)
    assert asent.id != sent.id
    assert asent.get(timeout=RESULT_TIMEOUT)[0] == sent.get(timeout=RESULT_TIMEOUT)[0] == 9
    assert asent.state == sent.state == 'SUCCESS'
    assert asent.ready() and sent.ready()
    # A fresh AsyncResult for the same id sees the same outcome.
    again = redis_app.AsyncResult(asent.id)
    assert again.state == 'SUCCESS'
    assert again.result[0] == 9


def test_asend_task_honours_task_id_and_kwargs(redis_app, make_worker):
    worker = make_worker(redis_app, '-c', '2')
    task_id = uuid.uuid4().hex

    async def publish():
        return await redis_app.asend_task(
            tasks.add.name, kwargs={'x': 1, 'y': 6},
            task_id=task_id, queue=worker.queue)

    result = asyncio.run(publish())
    assert result.id == task_id
    assert result.get(timeout=RESULT_TIMEOUT)[0] == 7


def test_asend_task_failure_matches_send_task(redis_app, make_worker):
    worker = make_worker(redis_app, '-c', '2', '--aconcurrency=10', '--aprocesses=1')
    sent = redis_app.send_task(tasks.araise.name, queue=worker.queue)

    async def publish():
        return await redis_app.asend_task(tasks.araise.name, queue=worker.queue)

    asent = asyncio.run(publish())
    wait_ready([sent, asent])
    assert asent.state == sent.state == 'FAILURE'
    assert asent.failed()
    with pytest.raises(ValueError, match='boom'):
        asent.get(timeout=5)
    assert type(asent.get(timeout=5, propagate=False)) is type(
        sent.get(timeout=5, propagate=False))


def test_asend_task_countdown(redis_app, make_worker):
    worker = make_worker(redis_app, '-c', '2')

    async def publish():
        return await redis_app.asend_task(
            tasks.add.name, args=(1, 1), countdown=2, queue=worker.queue)

    result = asyncio.run(publish())
    with pytest.raises(TimeoutError):
        result.get(timeout=1)
    assert result.get(timeout=RESULT_TIMEOUT)[0] == 2


def test_asend_task_link_runs_callback(redis_app, make_worker):
    worker = make_worker(redis_app, '-c', '2')
    callback_id = uuid.uuid4().hex
    callback = redis_app.signature(
        tasks.add.name, args=(10,), queue=worker.queue).set(task_id=callback_id)

    async def publish():
        return await redis_app.asend_task(
            tasks.add.name, args=(1, 2), link=callback, queue=worker.queue)

    result = asyncio.run(publish())
    assert result.get(timeout=RESULT_TIMEOUT)[0] == 3
    # The callback receives the parent's return value as its first argument;
    # ``add([3, pid], 10)`` raises, so only its having run is checked.
    linked = redis_app.AsyncResult(callback_id)
    wait_ready([linked])
    assert linked.state in ('SUCCESS', 'FAILURE')


def test_many_concurrent_asend_task_calls_all_run(redis_app, make_worker):
    worker = make_worker(redis_app, '-c', '4')
    count = 300

    async def publish():
        return await asyncio.gather(*(
            redis_app.asend_task(tasks.add.name, args=(i, 1000), queue=worker.queue)
            for i in range(count)))

    results = asyncio.run(publish())
    assert len({r.id for r in results}) == count
    wait_ready(results)
    assert [r.get(timeout=5)[0] for r in results] == [i + 1000 for i in range(count)]


def test_concurrent_asend_task_calls_interleave_with_other_work(redis_app, make_worker):
    worker = make_worker(redis_app, '-c', '2')
    ticks = []

    async def ticker(stop):
        while not stop.is_set():
            ticks.append(time.monotonic())
            await asyncio.sleep(0)

    async def publish():
        stop = asyncio.Event()
        tick_task = asyncio.ensure_future(ticker(stop))
        await asyncio.sleep(0)
        results = []
        for batch in range(10):
            results += await asyncio.gather(*(
                redis_app.asend_task(tasks.add.name, args=(batch, i), queue=worker.queue)
                for i in range(20)))
        stop.set()
        await tick_task
        return results

    results = asyncio.run(publish())
    wait_ready(results)
    assert all(r.successful() for r in results)
    # The loop kept running other coroutines while the publishes were awaited.
    assert len(ticks) > 10


def test_asend_task_from_successive_loops(redis_app, make_worker):
    worker = make_worker(redis_app, '-c', '2')
    results = []
    for i in range(3):
        async def publish(i=i):
            return await redis_app.asend_task(
                tasks.add.name, args=(i, i), queue=worker.queue)
        results.append(asyncio.run(publish()))
    wait_ready(results)
    assert [r.get(timeout=5)[0] for r in results] == [0, 2, 4]


def test_asend_task_from_loop_in_another_thread(redis_app, make_worker):
    worker = make_worker(redis_app, '-c', '2')
    out = {}

    def run():
        async def publish():
            return await redis_app.asend_task(
                tasks.add.name, args=(20, 22), queue=worker.queue)
        try:
            out['result'] = asyncio.run(publish())
        except BaseException as exc:  # pylint: disable=broad-except
            out['error'] = exc

    thread = threading.Thread(target=run)
    thread.start()
    thread.join(RESULT_TIMEOUT)
    assert 'error' not in out, out.get('error')
    assert out['result'].get(timeout=RESULT_TIMEOUT)[0] == 42


def test_asend_task_priority_order(redis_app, make_worker):
    # Lower numbers are consumed first on Redis; the queue is filled before
    # the worker starts, so the high-priority task runs first.
    worker = make_worker(
        redis_app, '-c', '1', '--prefetch-multiplier', '1', start=False)

    async def publish():
        low = await asyncio.gather(*(
            redis_app.asend_task(tasks.add.name, args=(i, 0), priority=9,
                                 queue=worker.queue)
            for i in range(5)))
        high = await redis_app.asend_task(
            tasks.add.name, args=(100, 0), priority=0, queue=worker.queue)
        return low, high

    low, high = asyncio.run(publish())
    worker.start()
    wait_ready([*low, high])
    assert all(r.successful() for r in [*low, high])
    assert high.date_done <= min(r.date_done for r in low)


def test_asend_task_priority_lands_where_send_task_puts_it(redis_app, make_worker):
    # No worker consumes the queue; the messages stay in the broker.
    queue = f'asendit-pri-{uuid.uuid4().hex[:10]}'
    sep = '\x06\x16'
    keys = {pri: queue if pri == 0 else f'{queue}{sep}{pri}' for pri in (0, 3, 6, 9)}
    try:
        for priority in (0, 3, 5, 9):
            redis_app.send_task(tasks.add.name, args=(0, 0), priority=priority, queue=queue)

            async def publish(priority=priority):
                return await redis_app.asend_task(
                    tasks.add.name, args=(0, 0), priority=priority, queue=queue)

            asyncio.run(publish())
            with redis_app.connection_for_write() as conn:
                client = conn.default_channel.client
                lengths = {pri: client.llen(key) for pri, key in keys.items()}
                for key in keys.values():
                    client.delete(key)
            # Both messages are in the same priority list.
            assert sorted(lengths.values()) == [0, 0, 0, 2], (priority, lengths)
    finally:
        delete_redis_keys(f'*{queue}*')


def test_asend_task_routed_by_task_routes(redis_app, make_worker):
    worker = make_worker(redis_app, '-c', '2')
    redis_app.conf.task_routes = {tasks.add.name: {'queue': worker.queue}}

    async def publish():
        return await redis_app.asend_task(tasks.add.name, args=(3, 4))

    result = asyncio.run(publish())
    assert result.get(timeout=RESULT_TIMEOUT)[0] == 7


def test_asend_task_named_queue_with_own_exchange(redis_app, make_worker):
    name = f'asendit-{uuid.uuid4().hex[:10]}'
    queue = Queue(name, Exchange(f'{name}-ex', type='direct'), routing_key=f'{name}-rk')
    redis_app.conf.task_queues = [queue, Queue('celery')]
    worker = make_worker(redis_app, '-c', '2', queue=queue, start=False)

    async def publish():
        return await redis_app.asend_task(
            tasks.add.name, args=(8, 9), queue=name)

    # Published before any worker has declared the queue or its binding.
    result = asyncio.run(publish())
    worker.start()
    assert result.get(timeout=RESULT_TIMEOUT)[0] == 17


def test_asend_task_routing_key_on_exchange(redis_app, make_worker):
    name = f'asendit-{uuid.uuid4().hex[:10]}'
    exchange = Exchange(f'{name}-ex', type='direct')
    queue = Queue(name, exchange, routing_key=f'{name}-rk')
    redis_app.conf.task_queues = [queue, Queue('celery')]
    worker = make_worker(redis_app, '-c', '2', queue=queue)
    # The binding exists before the publish, which names only the exchange
    # and the routing key.
    with Connection(REDIS_BROKER) as conn:
        queue(conn.default_channel).declare()

    async def publish():
        return await redis_app.asend_task(
            tasks.add.name, args=(5, 5),
            exchange=exchange.name, routing_key=f'{name}-rk')

    result = asyncio.run(publish())
    assert result.get(timeout=RESULT_TIMEOUT)[0] == 10
    assert worker.queue == name


def test_asend_task_with_global_keyprefix(make_worker):
    error = _unreachable(REDIS_BROKER)
    if error is not None:
        pytest.skip(f'Redis broker or result backend unreachable: {error}')
    prefix = f'asendit-{uuid.uuid4().hex[:6]}:'
    app = make_client(
        REDIS_BROKER, broker_transport_options={'global_keyprefix': prefix})
    queue = f'asendit-{uuid.uuid4().hex[:10]}'
    try:
        app.send_task(tasks.add.name, args=(1, 1), queue=queue)

        async def publish():
            return await app.asend_task(tasks.add.name, args=(1, 1), queue=queue)

        asyncio.run(publish())
        with Connection(REDIS_BROKER) as conn:
            client = conn.default_channel.client
            assert client.llen(f'{prefix}{queue}') == 2
            assert client.llen(queue) == 0
    finally:
        delete_redis_keys(f'{prefix}*')
        app.close()


def test_asend_task_from_successive_loops_leaves_no_connections(redis_app):
    queue = f'asendit-{uuid.uuid4().hex[:10]}'

    async def publish():
        return await redis_app.asend_task(tasks.add.name, args=(1, 1), queue=queue)

    try:
        with Connection(REDIS_BROKER) as conn:
            client = conn.default_channel.client
            asyncio.run(publish())
            before = len(client.client_list())
            for _ in range(10):
                asyncio.run(publish())
            assert len(client.client_list()) == before
            assert client.llen(queue) == 11
        assert len(redis_app._async_publisher._loops) == 0
    finally:
        delete_redis_keys(f'*{queue}*')


def test_a_failed_queue_declaration_fails_only_its_own_call(redis_app):
    good = f'asendit-{uuid.uuid4().hex[:10]}'
    bad = f'asendit-{uuid.uuid4().hex[:10]}'

    async def publish():
        return await asyncio.gather(
            redis_app.asend_task(tasks.add.name, args=(1, 1), queue=good),
            redis_app.asend_task(tasks.add.name, args=(1, 1), queue=bad),
            redis_app.asend_task(tasks.add.name, args=(1, 1), queue=good),
            return_exceptions=True,
        )

    try:
        with Connection(REDIS_BROKER) as conn:
            client = conn.default_channel.client
            # A key of another type: declaring the queue fails.
            client.set(bad, 'not a list')
            first, failed, last = asyncio.run(publish())
            assert isinstance(first, AsyncResult)
            assert isinstance(last, AsyncResult)
            assert isinstance(failed, OperationalError)
            assert client.llen(good) == 2
    finally:
        delete_redis_keys(f'*{good}*')
        delete_redis_keys(f'*{bad}*')


# -- Fallback ----------------------------------------------------------------


def test_asend_task_on_amqp_broker(amqp_app, make_worker):
    worker = make_worker(amqp_app, '-c', '2')

    async def publish():
        return await amqp_app.asend_task(
            tasks.add.name, args=(6, 7), queue=worker.queue)

    result = asyncio.run(publish())
    assert isinstance(result, AsyncResult)
    assert result.app is amqp_app
    assert result.get(timeout=RESULT_TIMEOUT)[0] == 13


def test_many_concurrent_asend_task_calls_on_amqp_broker(amqp_app, make_worker):
    worker = make_worker(amqp_app, '-c', '4')
    count = 50

    async def publish():
        return await asyncio.gather(*(
            amqp_app.asend_task(tasks.add.name, args=(i, 1), queue=worker.queue)
            for i in range(count)))

    results = asyncio.run(publish())
    assert len({r.id for r in results}) == count
    wait_ready(results)
    assert [r.get(timeout=5)[0] for r in results] == [i + 1 for i in range(count)]
