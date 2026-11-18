import asyncio
import json
import threading
from datetime import datetime, timezone
from unittest.mock import Mock

import pytest
import redis
import redis.client
from kombu import Exchange, Queue
from kombu.common import Broadcast
from kombu.exceptions import OperationalError
from kombu.utils.functional import retry_over_time

from celery import Celery, signals
from celery.app import async_publish
from celery.app.async_publish import RedisPublisher, _retry_intervals
from celery.backends.base import DisabledBackend
from celery.result import AsyncResult

# An unresolvable host: a command that reaches the network fails.
BROKER = 'redis://asend-unit.invalid:6379/3'
QUEUE = 'asend'
ETA = datetime(2100, 1, 1, tzinfo=timezone.utc)

# Fields filled with values generated per message.
GENERATED_HEADERS = ('id', 'root_id')
GENERATED_PROPERTIES = ('correlation_id', 'delivery_tag')


class FakeAsyncPipeline:

    def __init__(self, client):
        self.client = client
        self.commands = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        return None

    def execute_command(self, *args):
        self.commands.append(args)

    async def execute(self, raise_on_error=True):
        assert raise_on_error is False
        self.client.pipelines.append(self.commands)
        if self.client.errors:
            raise self.client.errors.pop(0)
        return [self.client.reply(command) for command in self.commands]


class FakeConnectionPool:

    def __init__(self):
        self.disconnected = False

    async def disconnect(self):
        self.disconnected = True


class FakeAsyncRedis:
    """Records the pipelines a ``redis.asyncio`` client would send."""

    def __init__(self):
        self.pipelines = []
        self.errors = []
        self.wrong_type = set()
        self.closed = False
        self.connection_pool = FakeConnectionPool()

    def pipeline(self, transaction=True):
        assert transaction is False
        return FakeAsyncPipeline(self)

    def reply(self, command):
        if command[1] in self.wrong_type:
            return redis.exceptions.ResponseError('WRONGTYPE Operation against a key holding the wrong kind')
        return 1

    async def aclose(self):
        self.closed = True

    @property
    def commands(self):
        return [command for pipeline in self.pipelines for command in pipeline]


@pytest.fixture
def sync_commands(monkeypatch):
    """The commands kombu's Redis channel runs, recorded instead of sent."""
    commands = []

    def execute_command(self, *args, **options):
        commands.append(args)
        return True if args[0] == 'PING' else 1

    monkeypatch.setattr(redis.Redis, 'execute_command', execute_command)
    monkeypatch.setattr(redis.client.Pipeline, 'execute', lambda self, *args, **kwargs: [])
    return commands


@pytest.fixture
def clients(monkeypatch):
    created = []

    def create_client(self):
        client = FakeAsyncRedis()
        created.append(client)
        return client

    monkeypatch.setattr(RedisPublisher, '_create_client', create_client)
    return created


@pytest.fixture
def make_app(sync_commands, clients):
    apps = []

    def make(broker=BROKER, **conf):
        app = Celery('asend', broker=broker, set_as_current=False)
        app.conf.task_default_queue = QUEUE
        app.conf.update(conf)
        apps.append(app)
        return app

    yield make
    for app in apps:
        app.close()


def pushes(commands):
    return [command for command in commands if command[0] == 'LPUSH']


def comparable(payload):
    message = json.loads(payload)
    for name in GENERATED_HEADERS:
        message['headers'].pop(name)
    for name in GENERATED_PROPERTIES:
        message['properties'].pop(name)
    return message


def run(coroutine_function):
    return asyncio.run(coroutine_function())


@pytest.mark.parametrize(('conf', 'options', 'key'), [
    pytest.param({}, {}, QUEUE, id='defaults'),
    pytest.param({}, {'priority': 6}, f'{QUEUE}\x06\x166', id='priority'),
    pytest.param(
        {'broker_transport_options': {'global_keyprefix': 'prefix:'}}, {}, f'prefix:{QUEUE}',
        id='global_keyprefix'),
    pytest.param({'task_serializer': 'pickle', 'task_compression': 'gzip'}, {}, QUEUE, id='serializer'),
    pytest.param({'task_routes': {'items.*': {'queue': 'routed'}}}, {}, 'routed', id='task_routes'),
    pytest.param({}, {'queue': 'explicit'}, 'explicit', id='queue'),
    pytest.param(
        {'task_queues': [Queue('declared', Exchange('declared'), routing_key='declared')]},
        {'queue': 'declared'}, 'declared', id='declared_queue'),
    pytest.param(
        {}, {'eta': ETA, 'headers': {'tenant': 'a'}, 'time_limit': 5,
             'argsrepr': '(...)', 'shadow': 'items.shadow'},
        QUEUE, id='options'),
])
def test_asend_task_pushes_the_message_send_task_pushes(make_app, sync_commands, clients, conf, options, key):
    app = make_app(**conf)
    app.send_task('items.create', (1, 'two'), {'data': {'name': 'ana'}}, **options)
    (expected,) = pushes(sync_commands)

    result = run(lambda: app.asend_task('items.create', (1, 'two'), {'data': {'name': 'ana'}}, **options))

    (actual,) = pushes(clients[0].commands)
    assert actual[1] == expected[1] == key
    assert comparable(actual[2]) == comparable(expected[2])
    message = json.loads(actual[2])
    assert message['headers']['id'] == message['properties']['correlation_id'] == result.id


def test_asend_task_declares_the_queue_once_in_a_thread(make_app, clients, monkeypatch):
    from kombu.transport.redis import Channel

    app = make_app()
    bound = []
    queue_bind = Channel._queue_bind

    def recording_queue_bind(self, exchange, routing_key, pattern, queue):
        bound.append((queue, threading.current_thread() is threading.main_thread()))
        return queue_bind(self, exchange, routing_key, pattern, queue)

    monkeypatch.setattr(Channel, '_queue_bind', recording_queue_bind)

    async def main():
        await app.asend_task('items.create')
        await app.asend_task('items.create')

    run(main)

    assert bound == [(QUEUE, False)]
    assert len(pushes(clients[0].commands)) == 2


def test_asend_task_returns_the_result_of_send_task(make_app):
    app = make_app()

    class Result(app.AsyncResult):
        pass

    async def main():
        return (
            await app.asend_task('items.create', task_id='fixed-id'),
            await app.asend_task('items.create', result_cls=Result, ignore_result=True),
        )

    plain, custom = run(main)

    assert isinstance(plain, AsyncResult)
    assert plain.id == 'fixed-id'
    assert plain.ignored is None
    assert type(custom) is Result
    assert custom.ignored is True


def test_asend_task_adds_the_result_to_the_parent(make_app, monkeypatch):
    app = make_app()
    parent = Mock(name='parent')
    parent.request.id = 'parent-id'
    parent.request.root_id = 'root-id'
    monkeypatch.setattr(type(app), 'current_worker_task', property(lambda self: parent))

    async def main():
        return await app.asend_task('items.create'), await app.asend_task('items.create', add_to_parent=False)

    added, not_added = run(main)

    parent.add_trail.assert_called_once_with(added)
    assert not_added.id != added.id


def test_asend_task_sets_parent_and_root_ids(make_app, clients, monkeypatch):
    app = make_app()
    parent = Mock(name='parent')
    parent.request.id = 'parent-id'
    parent.request.root_id = 'root-id'
    monkeypatch.setattr(type(app), 'current_worker_task', property(lambda self: parent))

    run(lambda: app.asend_task('items.create'))

    (push,) = pushes(clients[0].commands)
    headers = json.loads(push[2])['headers']
    assert (headers['parent_id'], headers['root_id']) == ('parent-id', 'root-id')


def test_publishes_of_one_loop_iteration_share_one_pipeline(make_app, clients):
    app = make_app()

    async def main():
        # The first call connects the publisher and declares the queue in a thread.
        await app.asend_task('items.create', kwargs={'n': -1})
        return await asyncio.gather(*(app.asend_task('items.create', kwargs={'n': n}) for n in range(20)))

    results = run(main)

    (client,) = clients
    assert [len(pipeline) for pipeline in client.pipelines] == [1, 20]
    pushed = [json.loads(command[2])['headers']['id'] for command in client.pipelines[1]]
    assert pushed == [result.id for result in results]


def test_a_failed_publish_fails_only_its_own_caller(make_app, clients):
    app = make_app(task_publish_retry=False)

    async def main():
        await app.asend_task('items.create')
        await app.asend_task('items.create', queue='broken')
        clients[0].wrong_type.add('broken')
        return await asyncio.gather(
            app.asend_task('items.create'),
            app.asend_task('items.create', queue='broken'),
            app.asend_task('items.create'),
            return_exceptions=True,
        )

    first, broken, last = run(main)

    assert isinstance(first, AsyncResult)
    assert isinstance(broken, OperationalError)
    assert isinstance(broken.__cause__, redis.exceptions.ResponseError)
    assert isinstance(last, AsyncResult)
    assert [len(pipeline) for pipeline in clients[0].pipelines] == [1, 1, 3]


def test_a_failed_pipeline_fails_every_caller_of_it(make_app, clients):
    app = make_app(task_publish_retry=False)

    async def main():
        await app.asend_task('items.create')
        clients[0].errors.append(redis.exceptions.ConnectionError('dropped'))
        return await asyncio.gather(
            app.asend_task('items.create'), app.asend_task('items.create'), return_exceptions=True)

    outcomes = run(main)

    assert [type(outcome) for outcome in outcomes] == [OperationalError, OperationalError]
    assert isinstance(outcomes[0].__cause__, redis.exceptions.ConnectionError)


def test_a_connection_error_is_retried_with_the_publish_retry_policy(make_app, clients, monkeypatch):
    app = make_app(task_publish_retry_policy={
        'max_retries': 2, 'interval_start': 0, 'interval_step': 0.01, 'interval_max': 0.02})
    waits = []
    sleep = asyncio.sleep

    async def recording_sleep(delay, *args, **kwargs):
        waits.append(delay)
        return await sleep(0)

    async def main():
        await app.asend_task('items.create')
        clients[0].errors.extend([
            redis.exceptions.ConnectionError('dropped'), redis.exceptions.TimeoutError('slow')])
        monkeypatch.setattr(asyncio, 'sleep', recording_sleep)
        try:
            return await app.asend_task('items.create')
        finally:
            monkeypatch.setattr(asyncio, 'sleep', sleep)

    run(main)

    assert waits == [0, 0.01]
    assert [len(pipeline) for pipeline in clients[0].pipelines] == [1, 1, 1, 1]


def test_retries_end_after_max_retries(make_app, clients):
    app = make_app(task_publish_retry_policy={
        'max_retries': 1, 'interval_start': 0, 'interval_step': 0, 'interval_max': 0})

    async def main():
        await app.asend_task('items.create')
        clients[0].errors.extend([redis.exceptions.ConnectionError('dropped')] * 3)
        await app.asend_task('items.create')

    with pytest.raises(OperationalError):
        run(main)
    assert len(clients[0].pipelines) == 3


@pytest.mark.parametrize(('conf', 'options'), [
    pytest.param({'task_publish_retry': False}, {}, id='task_publish_retry'),
    pytest.param({}, {'retry': False}, id='retry_option'),
])
def test_without_publish_retry_a_connection_error_fails_the_call(make_app, clients, conf, options):
    app = make_app(**conf)

    async def main():
        await app.asend_task('items.create')
        clients[0].errors.append(redis.exceptions.ConnectionError('dropped'))
        await app.asend_task('items.create', **options)

    with pytest.raises(OperationalError):
        run(main)
    assert len(clients[0].pipelines) == 2


def test_retry_intervals_match_kombu():
    policy = {'interval_start': 0.5, 'interval_step': 0.4, 'interval_max': 1.5}
    waits = []

    def errback(exc, intervals, retries):
        waits.append(next(intervals))
        return 0

    def fail():
        raise KeyError()

    with pytest.raises(KeyError):
        retry_over_time(fail, KeyError, errback=errback, max_retries=6, **policy)
    intervals = _retry_intervals(policy)
    assert [next(intervals) for _ in waits] == waits


def _record_send_task(app):
    calls = []

    def send_task(name, **kwargs):
        calls.append((name, kwargs, threading.current_thread() is threading.main_thread()))
        return AsyncResult('from-send-task', app=app)

    app.send_task = send_task
    return calls


@pytest.mark.parametrize(('broker', 'conf', 'options'), [
    pytest.param('memory://', {}, {}, id='memory_broker'),
    pytest.param('amqp://asend-unit.invalid//', {}, {}, id='amqp_broker'),
    pytest.param('sentinel://asend-unit.invalid:26379', {}, {}, id='sentinel_broker'),
    pytest.param('redis://asend-unit.invalid:6379;redis://asend-unit-2.invalid:6379', {}, {}, id='failover'),
    pytest.param(BROKER, {'task_always_eager': True}, {}, id='task_always_eager'),
    pytest.param(BROKER, {}, {'producer': Mock(name='producer')}, id='producer'),
    pytest.param(BROKER, {}, {'publisher': Mock(name='publisher')}, id='publisher'),
    pytest.param(BROKER, {}, {'connection': Mock(name='connection')}, id='connection'),
])
def test_other_brokers_and_explicit_producers_use_send_task_in_a_thread(make_app, clients, broker, conf, options):
    app = make_app(broker=broker, **conf)
    calls = _record_send_task(app)

    result = run(lambda: app.asend_task('items.create', (1,), {'a': 1}, countdown=5, priority=3, **options))

    assert result.id == 'from-send-task'
    ((name, kwargs, in_main_thread),) = calls
    assert name == 'items.create'
    assert not in_main_thread
    assert (kwargs['args'], kwargs['kwargs'], kwargs['countdown'], kwargs['priority']) == ((1,), {'a': 1}, 5, 3)
    for option, value in options.items():
        assert kwargs[option] is value
    assert clients == []


@pytest.mark.parametrize('options', [
    pytest.param({'queue': Broadcast('everyone')}, id='broadcast'),
    pytest.param({'exchange': Exchange('topics', type='topic'), 'routing_key': 'a.b'}, id='topic'),
    pytest.param({'exchange': 'direct', 'routing_key': 'a'}, id='exchange_and_routing_key'),
    pytest.param({'queue': Queue('topical', Exchange('topical', type='topic'), routing_key='a.#')},
                 id='topic_queue'),
    pytest.param({'queue': Queue('expiring', queue_arguments={'x-expires': 60000})}, id='expiring_queue'),
    pytest.param({'queue': Queue('auto', auto_delete=True)}, id='auto_delete_queue'),
])
def test_destinations_the_async_path_does_not_reproduce_use_a_thread(make_app, clients, monkeypatch, options):
    app = make_app()
    published = []
    monkeypatch.setattr(
        app, '_publish_prepared_task',
        lambda prepared: published.append((prepared, threading.current_thread() is threading.main_thread())))

    result = run(lambda: app.asend_task('items.create', **options))

    ((prepared, in_main_thread),) = published
    assert prepared.task_id == result.id
    assert not in_main_thread
    assert all(client.pipelines == [] for client in clients)


def test_the_publish_signals_surround_the_push(make_app, clients):
    app = make_app()
    seen = []

    def before(sender, headers, routing_key, **kwargs):
        seen.append(('before', sender, routing_key, len(clients[0].pipelines) if clients else 0))
        headers['traced'] = 'yes'

    def after(sender, headers, routing_key, **kwargs):
        seen.append(('after', sender, routing_key, len(clients[0].pipelines), headers['traced']))

    signals.before_task_publish.connect(before)
    signals.after_task_publish.connect(after)
    try:
        run(lambda: app.asend_task('items.create'))
    finally:
        signals.before_task_publish.disconnect(before)
        signals.after_task_publish.disconnect(after)

    assert seen == [('before', 'items.create', QUEUE, 0), ('after', 'items.create', QUEUE, 1, 'yes')]
    (push,) = pushes(clients[0].commands)
    assert json.loads(push[2])['headers']['traced'] == 'yes'


def test_the_task_sent_event_is_published_like_send_task_does(make_app, sync_commands, clients):
    app = make_app(task_send_sent_event=True)
    app.send_task('items.create', (1,))
    (expected,) = [command for command in sync_commands if command[0] == 'PUBLISH']

    result = run(lambda: app.asend_task('items.create', (1,)))

    push, publish = clients[0].commands
    assert push[0] == 'LPUSH'
    assert publish[:2] == expected[:2]
    event = json.loads(_decode_body(publish[2]))
    assert (event['type'], event['uuid'], event['queue'], event['routing_key']) == (
        'task-sent', result.id, QUEUE, QUEUE)


def _decode_body(payload):
    import base64
    return base64.b64decode(json.loads(payload)['body'])


class SubscribingBackend(DisabledBackend):
    """A result backend that records its ``on_task_call`` calls."""

    def __init__(self, app, fail_for=()):
        super().__init__(app)
        self.calls = []
        self.fail_for = fail_for

    def on_task_call(self, producer, task_id):
        self.calls.append((task_id, threading.current_thread().ident))
        if task_id in self.fail_for:
            raise redis.exceptions.ConnectionError('subscribe failed')


def test_the_result_backend_is_told_of_the_call_in_a_thread(make_app, monkeypatch):
    app = make_app()
    backend = SubscribingBackend(app)
    monkeypatch.setattr(type(app), 'backend', property(lambda self: backend))

    async def main():
        return await app.asend_task('items.create'), await app.asend_task('items.create', ignore_result=True)

    subscribed, ignored = run(main)

    ((task_id, thread),) = backend.calls
    assert task_id == subscribed.id
    assert thread != threading.main_thread().ident


def test_result_backend_calls_of_one_loop_iteration_share_one_thread_call(make_app, clients, monkeypatch):
    app = make_app()
    backend = SubscribingBackend(app, fail_for={'fails'})
    monkeypatch.setattr(type(app), 'backend', property(lambda self: backend))

    async def main():
        await app.asend_task('items.create')
        backend.calls.clear()
        return await asyncio.gather(
            app.asend_task('items.create', task_id='first'),
            app.asend_task('items.create', task_id='fails'),
            app.asend_task('items.create', task_id='last'),
            return_exceptions=True,
        )

    first, failed, last = run(main)

    assert [task_id for task_id, _ in backend.calls] == ['first', 'fails', 'last']
    assert len({thread for _, thread in backend.calls}) == 1
    assert (first.id, last.id) == ('first', 'last')
    assert isinstance(failed, OperationalError)
    pushed = [json.loads(command[2])['headers']['id'] for command in pushes(clients[0].commands)[1:]]
    assert pushed == ['first', 'last']


def test_each_event_loop_has_its_own_client_closed_with_the_loop(make_app, clients):
    app = make_app()

    run(lambda: app.asend_task('items.create'))
    run(lambda: app.asend_task('items.create'))

    assert len(clients) == 2
    assert all(client.closed for client in clients)
    assert [len(client.commands) for client in clients] == [1, 1]


def test_a_closed_loop_leaves_neither_its_client_nor_its_state(make_app, clients):
    app = make_app()

    for _ in range(3):
        run(lambda: app.asend_task('items.create'))

    assert len(clients) == 3
    assert all(client.connection_pool.disconnected for client in clients)
    assert len(app._async_publisher._loops) == 0


def test_a_loop_closed_without_shutdown_is_dropped_by_the_next_loop(make_app, clients):
    app = make_app()
    loop = asyncio.new_event_loop()
    loop.run_until_complete(app.asend_task('items.create'))
    loop.close()
    publisher = app._async_publisher
    assert loop in publisher._loops

    run(lambda: app.asend_task('items.create'))

    assert loop not in publisher._loops
    assert len(publisher._loops) == 0


@pytest.fixture
def failing_events(monkeypatch):
    """Fail every pipeline that publishes an event."""
    execute = FakeAsyncPipeline.execute

    async def failing_execute(self, raise_on_error=True):
        if any(command[0] == 'PUBLISH' for command in self.commands):
            raise redis.exceptions.ConnectionError('down')
        return await execute(self, raise_on_error)

    monkeypatch.setattr(FakeAsyncPipeline, 'execute', failing_execute)


def test_a_failed_task_sent_event_is_buffered_as_send_task_does(make_app, clients, failing_events):
    app = make_app(task_send_sent_event=True, task_publish_retry=False)
    dispatcher = app.amqp._event_dispatcher
    assert dispatcher.buffer_while_offline

    result = run(lambda: app.asend_task('items.create'))

    assert len(pushes(clients[0].commands)) == 1
    ((event, routing_key),) = dispatcher._outbound_buffer
    assert (event['type'], event['uuid'], routing_key) == ('task-sent', result.id, 'task.sent')


def test_a_failed_task_sent_event_raises_without_buffering(make_app, clients, failing_events):
    app = make_app(task_send_sent_event=True, task_publish_retry=False)
    dispatcher = app.events.Dispatcher(enabled=False, buffer_while_offline=False)

    with pytest.raises(OperationalError):
        run(lambda: app.asend_task('items.create', event_dispatcher=dispatcher))
    assert len(pushes(clients[0].commands)) == 1


def test_the_result_backend_of_the_loop_thread_is_told_of_the_call(make_app, monkeypatch):
    app = make_app()
    created = []

    def get_backend():
        created.append(SubscribingBackend(app))
        return created[-1]

    monkeypatch.setattr(app, '_get_backend', get_backend)

    result = run(lambda: app.asend_task('items.create'))

    called = [backend for backend in created if backend.calls]
    assert called == [result.backend]
    assert result.backend.calls[0][0] == result.id


def test_a_redis_result_backend_of_one_thread_subscribes_when_the_result_is_waited_for(make_app, monkeypatch):
    from celery.backends.redis import RedisBackend

    app = make_app(result_backend='redis://asend-unit.invalid:6379/4')
    calls = []
    monkeypatch.setattr(RedisBackend, 'on_task_call', lambda self, producer, task_id: calls.append(task_id))

    result = run(lambda: app.asend_task('items.create'))

    assert isinstance(result.backend, RedisBackend)
    assert calls == []


def test_connection_options_redis_asyncio_does_not_accept_use_a_thread(make_app, clients, monkeypatch):
    import ssl

    app = make_app(
        broker='rediss://asend-unit.invalid:6380/0',
        broker_use_ssl={'ssl_cert_reqs': ssl.CERT_REQUIRED, 'ssl_validate_ocsp': True},
    )
    published = []
    monkeypatch.setattr(
        app, '_publish_prepared_task',
        lambda prepared: published.append((prepared, threading.current_thread() is threading.main_thread())))

    result = run(lambda: app.asend_task('items.create'))

    ((prepared, in_main_thread),) = published
    assert prepared.task_id == result.id
    assert not in_main_thread
    assert clients == []


def test_a_forked_process_sets_the_publisher_up_again(make_app, clients, monkeypatch):
    app = make_app()
    run(lambda: app.asend_task('items.create'))
    publisher = app._async_publisher
    connection = publisher._connection
    monkeypatch.setattr(async_publish.os, 'getpid', lambda: -1)

    run(lambda: app.asend_task('items.create'))

    assert publisher._pid == -1
    assert publisher._connection is not connection
    assert len(clients) == 2


@pytest.mark.parametrize(('broker', 'supported'), [
    (BROKER, True),
    ('rediss://asend-unit.invalid:6380/0?ssl_cert_reqs=required', True),
    ('redis+socket:///tmp/asend-unit.sock', True),
    ('memory://', False),
    ('sentinel://asend-unit.invalid:26379', False),
])
def test_for_app_selects_one_redis_server(broker, supported):
    app = Celery('asend', broker=broker, set_as_current=False)
    try:
        assert (RedisPublisher.for_app(app) is not None) is supported
    finally:
        app.close()
