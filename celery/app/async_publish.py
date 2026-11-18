"""Publishing task messages to a Redis broker from an :mod:`asyncio` event loop.

Used by :meth:`celery.Celery.asend_task`. The message is built by Celery's
and kombu's own code, as :meth:`~celery.Celery.send_task` builds it: a kombu
Redis channel prepares the message and routes it, and the Redis commands it
would run are collected instead of run. A ``redis.asyncio`` client then sends
them from the event loop. Messages published in the same iteration of the
event loop are sent in one pipeline.
"""
import asyncio
import functools
import inspect
import os
import threading
from collections import namedtuple
from weakref import WeakKeyDictionary

from kombu.common import Broadcast, maybe_declare
from kombu.utils.functional import fxrange

from celery import signals
from celery.app.amqp import _callback_publish_retry, send_task_sent_signal, task_publish_kwargs, task_sent_event
from celery.utils.log import get_logger

__all__ = ('RedisPublisher',)

logger = get_logger(__name__)

#: Defaults of :meth:`kombu.Connection.ensure` for keys missing from a retry policy.
_ENSURE_DEFAULTS = {'max_retries': None, 'interval_start': 1, 'interval_step': 1, 'interval_max': 1}

_LoopState = namedtuple('_LoopState', ('client', 'pending', 'calls', 'tasks'))


class _CommandCollector:
    """Collects the Redis commands kombu's Redis channel runs for a publish.

    Installed as the active publish batch of a :class:`kombu.Producer`, it
    receives the commands :class:`kombu.transport.redis.PublishBatch` would
    add to its pipeline.
    """

    def __init__(self):
        self.commands = []

    def publish(self, channel, message, **kwargs):
        return channel.basic_publish(message, _publish_batch=self, **kwargs)

    def add(self, name, *args):
        self.commands.append((name.upper(), *args))


def _collect(producer, publish, *args, **kwargs):
    """Call ``publish(*args, **kwargs)`` with the Redis commands of the producer's publishes collected.

    Returns the collected commands. The producer must not be in a publish batch.
    """
    local = producer._batch_local
    if getattr(local, 'current', None) is not None:
        raise RuntimeError('The producer is in a publish batch')
    collector = local.current = _CommandCollector()
    try:
        publish(*args, **kwargs)
    finally:
        del local.current
    return collector.commands


def _calls_on_task_call(backend):
    """Whether :meth:`RedisPublisher.publish` calls ``backend.on_task_call`` before the publish.

    Not when the backend does nothing in it. Not for a Redis result backend
    of one thread: it subscribes to the result with the client the thread
    waits for results with, and :meth:`AsyncResult.get` subscribes and
    reads a result stored before it.
    """
    from celery.backends.base import Backend
    if getattr(type(backend), 'on_task_call', None) is Backend.on_task_call:
        return False
    try:
        from celery.backends.redis import RedisBackend
    except ImportError:  # pragma: no cover
        return True
    return backend.thread_safe or not isinstance(backend, RedisBackend)


def _retry_intervals(policy):
    """The waits between publish retries, as :func:`kombu.utils.functional.retry_over_time` computes them."""
    interval_start = policy['interval_start']
    interval_max = policy['interval_max']
    for value in fxrange(interval_start, interval_max + interval_start, policy['interval_step'], repeatlast=True):
        yield min(value, float(interval_max))


def _accepted_params(connection_class):
    """The keyword arguments ``connection_class`` accepts, from the ``__init__`` signatures of its classes."""
    names = set()
    for klass in connection_class.__mro__:
        init = klass.__dict__.get('__init__')
        if init is None:
            continue
        parameters = inspect.signature(init).parameters.values()
        names.update(
            parameter.name for parameter in parameters
            if parameter.kind in (parameter.POSITIONAL_OR_KEYWORD, parameter.KEYWORD_ONLY)
        )
        if not any(parameter.kind is parameter.VAR_KEYWORD for parameter in parameters):
            break
    names.discard('self')
    return names


async def _close_client(client):
    """Close ``client`` and disconnect the connections of its pool."""
    try:
        await client.aclose()
    finally:
        await client.connection_pool.disconnect()


class RedisPublisher:
    """Publishes the task messages of one app to its Redis broker from event loops.

    A kombu connection of the process prepares the messages, a second one
    declares the queues; both are set up in a worker thread on first use and
    again after a fork. Each event loop of the process has its own
    ``redis.asyncio`` client, closed when the loop shuts down its tasks
    (:func:`asyncio.run`, :class:`asyncio.Runner`). The client of a loop
    closed without that is dropped when another loop creates its client.
    """

    @classmethod
    def for_app(cls, app):
        """Return a publisher for ``app``, or :const:`None` when its broker is not one Redis server.

        Supported are the ``redis://``, ``rediss://`` and Unix socket URLs of
        kombu's Redis transport, without failover URLs; not Sentinel.
        """
        try:
            from kombu.transport import redis as kombu_redis
            connection = app.connection_for_write()
            try:
                supported = (type(connection.transport) is kombu_redis.Transport
                             and len(connection.alt) <= 1)
            finally:
                connection.release()
        except ImportError:
            return None
        return cls(app) if supported else None

    def __init__(self, app):
        self.app = app
        self._lock = threading.Lock()
        self._pid = None
        self._connection = None
        self._producer = None
        self._declaring = None
        self._client_params = None
        self._connection_class = None
        self._prefixer = None
        self._declared = set()
        self._loops = WeakKeyDictionary()

    # -- Message ----------------------------------------------------------

    def _ready(self):
        channel = self._producer.channel if self._producer is not None else None
        return self._pid == os.getpid() and channel is not None and channel.connection is not None

    def _setup(self):
        """Connect the kombu connection that prepares the messages (blocking)."""
        with self._lock:
            if self._ready():
                return
            from kombu.transport.redis import GlobalKeyPrefixMixin

            forked = self._pid != os.getpid()
            if not forked:
                for previous in (self._connection, self._declaring):
                    if previous is not None:
                        previous.release()
            connection = self.app.connection_for_write()
            producer = self.app.amqp.Producer(connection, auto_declare=False)
            channel = producer.channel
            if forked:
                self._loops = WeakKeyDictionary()
                self._client_params, self._connection_class = self._client_spec(channel._connparams())
                prefixer = GlobalKeyPrefixMixin()
                prefixer.global_keyprefix = channel.global_keyprefix
                self._prefixer = prefixer
            self._connection, self._producer = connection, producer
            self._declaring = self.app.connection_for_write()
            self._declared = set()
            self._pid = os.getpid()

    @staticmethod
    def _client_spec(params):
        """Return the ``redis.asyncio`` connection parameters and class for kombu's connection parameters.

        The class is :const:`None` when ``redis.asyncio`` does not accept
        every parameter, such as the OCSP options of an SSL connection.
        """
        import redis
        from redis.asyncio import connection as async_connection

        params = dict(params)
        sync_class = params.pop('connection_class', redis.Connection)
        if issubclass(sync_class, redis.UnixDomainSocketConnection):
            connection_class = async_connection.UnixDomainSocketConnection
        elif issubclass(sync_class, redis.SSLConnection):
            connection_class = async_connection.SSLConnection
        else:
            connection_class = async_connection.Connection
        params.pop('max_connections', None)
        params.pop('retry_on_timeout', None)
        if not set(params) <= _accepted_params(connection_class):
            return params, None
        return params, connection_class

    def _declare(self, entities, retry, retry_policy):
        """Declare ``entities`` as the publish of :meth:`~celery.Celery.send_task` does (blocking).

        The declarations use their own kombu connection, so that a failed
        declaration does not close the channel that prepares the messages;
        the next declaration reconnects it.
        """
        with self._lock:
            for entity in entities:
                maybe_declare(entity, self._declaring.default_channel, retry=retry, **retry_policy)
                self._declared.add(hash(entity))

    def _supports(self, publish):
        """Whether the async path publishes ``publish``: a direct publish to a queue whose declaration is cached."""
        queue = publish.queue
        return (
            publish.exchange == ''
            and bool(publish.routing_key)
            and queue is not None
            and not isinstance(queue, str)
            and not isinstance(queue, Broadcast)
            and all(getattr(entity, 'can_cache_declaration', False) for entity in publish.declare or ())
            and not (publish.retry and _callback_publish_retry.get())
        )

    def _commands(self, producer, publish, *args, **kwargs):
        prefix = self._prefixer._prefix_args
        return [prefix(command) for command in _collect(producer, publish, *args, **kwargs)]

    # -- Publish ----------------------------------------------------------

    async def publish(self, prepared):
        """Publish the message of a :meth:`~celery.Celery._prepare_send_task` result.

        Publishes in a worker thread with :meth:`~celery.Celery.send_task`'s
        own code when the destination is not a direct queue whose declaration
        is cached.
        """
        app = self.app
        if not self._ready():
            await asyncio.to_thread(self._setup)
        if self._connection_class is None:
            await asyncio.to_thread(app._publish_prepared_task, prepared)
            return
        options = dict(prepared.options)
        event_dispatcher = options.pop('event_dispatcher', None)
        publish = app.amqp.prepare_task_publish(prepared.message, **options)
        if not self._supports(publish):
            await asyncio.to_thread(app._publish_prepared_task, prepared)
            return

        producer = self._producer
        with self._connection._reraise_as_library_errors():
            undeclared = [entity for entity in publish.declare or () if hash(entity) not in self._declared]
            if undeclared:
                await asyncio.to_thread(self._declare, undeclared, publish.retry, publish.retry_policy)
            backend = app.backend
            if not prepared.ignore_result and _calls_on_task_call(backend):
                await self._on_task_call(backend, prepared.task_id)

            name, body, headers = prepared.name, publish.body, publish.headers
            signals.before_task_publish.send(
                sender=name, body=body,
                exchange=publish.exchange, routing_key=publish.routing_key,
                declare=publish.declare, headers=headers,
                properties=publish.properties, retry_policy=publish.retry_policy,
            )
            commands = self._commands(
                producer, producer.publish, body, **dict(task_publish_kwargs(publish), retry=False, declare=[]))
            await self._send(commands, publish.retry, publish.retry_policy)
            signals.after_task_publish.send(
                sender=name, body=body, headers=headers,
                exchange=publish.exchange, routing_key=publish.routing_key,
            )
            send_task_sent_signal(name, body, headers)
            sent_event = task_sent_event(publish)
            if sent_event:
                evd = event_dispatcher or app.amqp._event_dispatcher
                await self._publish_event(evd, producer, sent_event, publish.retry, publish.retry_policy)

    async def _publish_event(self, dispatcher, producer, fields, retry, retry_policy):
        """Publish the ``task-sent`` event as :meth:`EventDispatcher.publish` does.

        A failed publish is buffered by a dispatcher with
        ``buffer_while_offline``, and raised otherwise.
        """
        event = dispatcher._prepare_event('task-sent', fields)
        routing_key = 'task.sent'
        with dispatcher.mutex:
            commands = self._commands(
                producer, dispatcher._publish, event, producer, routing_key, retry=False, retry_policy=retry_policy)
        if not commands:
            return
        try:
            await self._send(commands, retry, retry_policy)
        except Exception:
            if not dispatcher.buffer_while_offline:
                raise
            with dispatcher.mutex:
                dispatcher._outbound_buffer.append((event, routing_key))

    async def _on_task_call(self, backend, task_id):
        """Call ``backend.on_task_call`` in a worker thread.

        The calls of one loop iteration share one thread.
        """
        loop = asyncio.get_running_loop()
        state = self._loop_state(loop)
        future = loop.create_future()
        if not state.calls:
            loop.call_soon(self._flush_calls, loop, state)
        state.calls.append((backend, task_id, future))
        await future

    def _flush_calls(self, loop, state):
        batch = state.calls[:]
        state.calls.clear()
        if batch:
            called = loop.run_in_executor(
                None, self._call_backend, [(backend, task_id) for backend, task_id, _ in batch])
            called.add_done_callback(functools.partial(self._resolve_calls, batch))

    def _call_backend(self, calls):
        """Call ``on_task_call`` for each ``(backend, task_id)``; return the exception of each, or :const:`None`.

        The calls get a producer on the connection that declares the queues.
        """
        errors = []
        with self._lock:
            producer = self.app.amqp.Producer(self._declaring.default_channel, auto_declare=False)
            for backend, task_id in calls:
                try:
                    backend.on_task_call(producer, task_id)
                except Exception as exc:  # pylint: disable=broad-except
                    errors.append(exc)
                else:
                    errors.append(None)
        return errors

    @staticmethod
    def _resolve_calls(batch, called):
        if called.cancelled():
            errors = [asyncio.CancelledError()] * len(batch)
        elif called.exception() is not None:
            errors = [called.exception()] * len(batch)
        else:
            errors = called.result()
        for (*_, future), error in zip(batch, errors):
            if future.done():
                continue
            if isinstance(error, asyncio.CancelledError):
                future.cancel()
            elif error is not None:
                future.set_exception(error)
            else:
                future.set_result(None)

    async def _send(self, commands, retry, retry_policy):
        """Send ``commands`` with the next pipeline of the running loop, retried as ``retry_policy`` says."""
        policy = dict(_ENSURE_DEFAULTS, **retry_policy)
        retry_errors = policy.get('retry_errors') or ()
        errors = (
            *self._connection.recoverable_connection_errors,
            *self._connection.recoverable_channel_errors,
            *retry_errors,
        )
        intervals = _retry_intervals(policy)
        retries = 0
        while True:
            try:
                return await self._enqueue(commands)
            except errors as exc:
                max_retries = policy['max_retries']
                if not retry or (max_retries is not None and retries >= max_retries):
                    raise
                wait = next(intervals)
                logger.warning('Retrying task publish in %ss: %r', wait, exc)
                await asyncio.sleep(wait)
                retries += 1

    async def _enqueue(self, commands):
        loop = asyncio.get_running_loop()
        state = self._loop_state(loop)
        future = loop.create_future()
        if not state.pending:
            loop.call_soon(self._flush, loop, state)
        state.pending.append((commands, future))
        return await future

    def _loop_state(self, loop):
        state = self._loops.get(loop)
        if state is None:
            for closed in [other for other in self._loops if other.is_closed()]:
                del self._loops[closed]
            client = self._create_client()
            state = self._loops[loop] = _LoopState(client, [], [], set())
            closer = loop.create_task(self._close_with_loop(loop, state))
            closer._log_destroy_pending = False
            state.tasks.add(closer)
        return state

    async def _close_with_loop(self, loop, state):
        """Close the client of ``state`` when this coroutine's task is cancelled at event loop shutdown."""
        try:
            await loop.create_future()
        except asyncio.CancelledError:
            if self._loops.get(loop) is state:
                del self._loops[loop]
            state.tasks.clear()
            await _close_client(state.client)
            raise

    def _create_client(self):
        """A ``redis.asyncio`` client with the broker connection parameters of kombu's Redis channel.

        Without a connection limit and without redis-py retries: the publish
        retry policy applies instead.
        """
        import redis.asyncio
        from redis.asyncio.retry import Retry
        from redis.backoff import NoBackoff

        pool = redis.asyncio.ConnectionPool(
            connection_class=self._connection_class, retry=Retry(NoBackoff(), 0), **self._client_params)
        return redis.asyncio.Redis(connection_pool=pool)

    def _flush(self, loop, state):
        batch = state.pending[:]
        state.pending.clear()
        if batch:
            task = loop.create_task(self._execute(state.client, batch))
            state.tasks.add(task)
            task.add_done_callback(state.tasks.discard)

    async def _execute(self, client, batch):
        """Send ``batch`` in one pipeline without a transaction and resolve the future of each publish."""
        try:
            async with client.pipeline(transaction=False) as pipe:
                for commands, _ in batch:
                    for command in commands:
                        pipe.execute_command(*command)
                replies = await pipe.execute(raise_on_error=False)
        except BaseException as exc:
            for _, future in batch:
                if not future.done():
                    if isinstance(exc, asyncio.CancelledError):
                        future.cancel()
                    else:
                        future.set_exception(exc)
            if not isinstance(exc, Exception):
                raise
            return
        position = 0
        for commands, future in batch:
            own = replies[position:position + len(commands)]
            position += len(commands)
            if future.done():
                continue
            failed = next((reply for reply in own if isinstance(reply, Exception)), None)
            if failed is not None:
                future.set_exception(failed)
            else:
                future.set_result(None)
