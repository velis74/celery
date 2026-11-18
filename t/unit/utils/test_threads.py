import asyncio
import contextvars
import copy
import os
import pickle
import socket
import subprocess
import sys
import textwrap
import threading
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from celery import _state
from celery.app.task import Task
from celery.utils.threads import (Local, LocalManager, LocalStack, bgThread, bound_open_broker_sockets,
                                  default_socket_timeout, release_local)
from t.unit import conftest


class test_default_socket_timeout:

    @pytest.fixture(autouse=True)
    def _restore_default_timeout(self):
        prev = socket.getdefaulttimeout()
        socket.setdefaulttimeout(None)
        yield
        socket.setdefaulttimeout(prev)

    def test_sets_and_restores_timeout(self):
        with default_socket_timeout(5.0):
            assert socket.getdefaulttimeout() == 5.0
        assert socket.getdefaulttimeout() is None

    def test_restores_timeout_when_body_raises(self):
        # Without try/finally the timeout leaks out of the block and every
        # socket created later in the process is silently bounded by it.
        with pytest.raises(ValueError):
            with default_socket_timeout(5.0):
                raise ValueError('boom')
        assert socket.getdefaulttimeout() is None


class test_bound_open_broker_sockets:
    """Bounding sockets that are already connected (GH-9705, celery#975)."""

    def _sock(self):
        sock, peer = socket.socketpair()
        self._open.extend((sock, peer))
        return sock

    @pytest.fixture(autouse=True)
    def _close_sockets(self):
        self._open = []
        yield
        for sock in self._open:
            sock.close()

    def test_bounds_virtual_transport_sockets(self):
        # redis and the other virtual transports keep a redis-py style
        # client per channel, read from during Channel.close().
        client, sub = self._sock(), self._sock()
        channel = SimpleNamespace(
            client=SimpleNamespace(connection=SimpleNamespace(_sock=client)),
            subclient=SimpleNamespace(connection=SimpleNamespace(_sock=sub)),
        )
        connection = SimpleNamespace(
            transport=SimpleNamespace(channels=[channel]), _connection=None)

        bound_open_broker_sockets(connection, 7.5)

        assert client.gettimeout() == 7.5
        assert sub.gettimeout() == 7.5

    def test_bounds_amqp_transport_socket(self):
        # py-amqp keeps one socket on the connection's transport; this is the
        # read that hangs in basic_cancel during shutdown (celery#975).
        sock = self._sock()
        connection = SimpleNamespace(
            transport=SimpleNamespace(channels=None),
            _connection=SimpleNamespace(_transport=SimpleNamespace(sock=sock)),
        )

        bound_open_broker_sockets(connection, 3.0)

        assert sock.gettimeout() == 3.0

    def test_never_touches_the_reconnecting_connection_property(self):
        # kombu's public ``Connection.connection`` property calls
        # _ensure_connection(), which would dial the dead broker and block -
        # worse than the bug being fixed.  Only ``_connection`` is safe.
        class Connection:
            transport = SimpleNamespace(channels=None)
            _connection = None

            @property
            def connection(self):
                raise AssertionError('must not reconnect to a dead broker')

        bound_open_broker_sockets(Connection(), 5.0)

    def test_never_touches_the_reconnecting_amqp_transport_property(self):
        # py-amqp's ``Connection.transport`` property calls self.connect()
        # when _transport is None, and _transport IS None after
        # amqp's _on_close_ok() -> collect(), which is exactly the
        # server-initiated close that lands us here.  Reading it would dial
        # the broker we are abandoning.
        class AmqpConnection:
            _transport = None

            @property
            def transport(self):
                raise AssertionError('must not reconnect to a dead broker')

        bound_open_broker_sockets(
            SimpleNamespace(transport=SimpleNamespace(channels=None),
                            _connection=AmqpConnection()), 5.0)

    def test_does_not_open_a_new_client(self):
        # The broker is already known to be unresponsive; going through the
        # ``client`` property would dial it again and block.
        class ExplodingChannel:
            @property
            def client(self):
                raise AssertionError('must not open a new connection')

        bound_open_broker_sockets(
            SimpleNamespace(
                transport=SimpleNamespace(channels=[ExplodingChannel()]),
                _connection=None),
            5.0)

    def test_ignores_connection_without_sockets(self):
        bound_open_broker_sockets(
            SimpleNamespace(transport=SimpleNamespace(channels=None),
                            _connection=None), 5.0)

    def test_survives_unusable_connection(self):
        # Best effort: teardown must carry on to close() regardless.
        class Unusable:
            @property
            def transport(self):
                raise OSError('connection is gone')

        bound_open_broker_sockets(Unusable(), 5.0)

    def test_survives_closed_socket(self):
        sock, peer = socket.socketpair()
        sock.close()
        peer.close()
        connection = SimpleNamespace(
            transport=SimpleNamespace(channels=[SimpleNamespace(
                client=SimpleNamespace(
                    connection=SimpleNamespace(_sock=sock)))]),
            _connection=None)

        bound_open_broker_sockets(connection, 5.0)


class test_bgThread:

    def test_crash(self):

        class T(bgThread):

            def body(self):
                raise KeyError()

        with patch('os._exit') as _exit:
            with conftest.stdouts():
                _exit.side_effect = ValueError()
                t = T()
                with pytest.raises(ValueError):
                    t.run()
                _exit.assert_called_with(1)

    def test_interface(self):
        x = bgThread()
        with pytest.raises(NotImplementedError):
            x.body()


class test_Local:

    def test_iter(self):
        x = Local()
        x.foo = 'bar'
        ident = x.__ident_func__()
        assert (ident, {'foo': 'bar'}) in list(iter(x))

        delattr(x, 'foo')
        assert (ident, {'foo': 'bar'}) not in list(iter(x))
        with pytest.raises(AttributeError):
            delattr(x, 'foo')

        assert x(lambda: 'foo') is not None


class test_LocalStack:

    def test_stack(self):
        x = LocalStack()
        assert x.pop() is None
        assert x.top is None
        assert x.stack == []
        assert len(x) == 0
        with pytest.raises(RuntimeError):
            x()[0]

        assert x.push(['foo']) == [['foo']]
        assert x.push(['bar']) == [['foo'], ['bar']]
        assert x.top == ['bar']
        assert x()[0] == 'bar'
        assert x.stack == [['foo'], ['bar']]
        assert len(x) == 2
        assert x.pop() == ['bar']
        assert x.top == ['foo']
        assert x.pop() == ['foo']
        assert x.pop() is None
        with pytest.raises(RuntimeError):
            x()[0]

    def test_stack_is_a_copy(self):
        x = LocalStack()
        x.push(1)
        x.stack.append(2)
        x.push(3).append(4)
        assert x.stack == [1, 3]

    def test_release_local(self):
        x = LocalStack()
        x.push(1)
        x.push(2)
        release_local(x)
        assert x.top is None
        assert len(x) == 0

    def test_instances_are_independent(self):
        x, y = LocalStack(), LocalStack()
        x.push(1)
        assert y.top is None
        x.pop()

    def test_copies_are_new_empty_stacks(self):
        x = LocalStack('named')
        x.push(1)
        holder = {'stack': x}
        for copied in (copy.copy(x), copy.deepcopy(holder)['stack'], pickle.loads(pickle.dumps(x))):
            assert copied is not x
            assert copied.name == 'named'
            assert copied.top is None
            copied.push(2)
            assert x.stack == [1]
            copied.pop()
        assert x.pop() == 1

    def test_use_fast_locals_is_deprecated_at_worker_start(self, tmp_path):
        script = textwrap.dedent('''
            import os
            import sys
            from celery import Celery, signals

            app = Celery('fast', broker='memory://', set_as_current=False)

            @signals.worker_ready.connect
            def stop(**kwargs):
                sys.stderr.flush()
                os._exit(0)

            app.Worker(pool='solo', concurrency=1, quiet=True, without_heartbeat=True,
                       without_mingle=True, without_gossip=True).start()
        ''')
        env = {**os.environ, 'USE_FAST_LOCALS': 'yes'}
        env.pop('PYTHONWARNINGS', None)
        proc = subprocess.run(
            [sys.executable, '-c', script], cwd=tmp_path,
            env=env, capture_output=True, text=True, timeout=60,
        )
        assert proc.returncode == 0, proc.stderr
        assert 'CDeprecationWarning' in proc.stderr
        assert 'USE_FAST_LOCALS' in proc.stderr

    def test_threads_are_isolated(self):
        x = LocalStack()
        x.push('main')
        barrier = threading.Barrier(3)
        seen = {}

        def target(n):
            seen[n] = [x.top]
            x.push(n)
            barrier.wait()
            seen[n].append(x.stack)
            seen[n].append(x.pop())

        threads = [threading.Thread(target=target, args=(n,)) for n in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        inherited = ['main'] if getattr(sys.flags, 'thread_inherit_context', 0) else []
        for n in range(3):
            assert seen[n] == [inherited[-1] if inherited else None, inherited + [n], n]
        assert x.stack == ['main']
        assert x.pop() == 'main'

    def test_thread_with_copied_context_keeps_snapshot(self):
        x = LocalStack()
        x.push('request-A')
        ctx = contextvars.copy_context()
        x.pop()
        x.push('request-B')
        started, release = threading.Event(), threading.Event()
        seen = []

        def target():
            seen.append(x.top)
            started.set()
            release.wait(10)
            seen.append(x.stack)
            x.push('thread')

        t = threading.Thread(target=ctx.run, args=(target,))
        t.start()
        started.wait(10)
        x.pop()
        release.set()
        t.join()
        assert seen == ['request-A', ['request-A']]
        assert x.top is None

    def test_repeated_use_does_not_grow_the_context(self):
        x = LocalStack()
        x.push(0)
        x.pop()
        size = len(contextvars.copy_context())
        for n in range(100):
            x.push(n)
            x.push(n)
            x.pop()
            release_local(x)
        assert len(contextvars.copy_context()) == size
        assert x.top is None

    def test_asyncio_tasks_are_isolated(self):
        x = LocalStack()
        x.push('outer')

        async def worker(n):
            x.push(n)
            await asyncio.sleep(0)
            stack = x.stack
            await asyncio.sleep(0)
            return stack, x.pop()

        async def main():
            return await asyncio.gather(*(worker(n) for n in range(3)))

        assert asyncio.run(main()) == [(['outer', n], n) for n in range(3)]
        assert x.stack == ['outer']
        assert x.pop() == 'outer'

    def test_run_coroutine_threadsafe_sees_caller_stack(self):
        x = LocalStack()
        loop = asyncio.new_event_loop()
        loop_thread = threading.Thread(target=loop.run_forever)
        loop_thread.start()
        barrier = threading.Barrier(3)
        seen = {}

        async def coro():
            await asyncio.sleep(0)
            return x.top, len(x)

        def caller(n):
            x.push(('req', n))
            barrier.wait()
            try:
                seen[n] = asyncio.run_coroutine_threadsafe(coro(), loop).result(timeout=10)
            finally:
                x.pop()

        try:
            threads = [threading.Thread(target=caller, args=(n,)) for n in range(3)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
        finally:
            loop.call_soon_threadsafe(loop.stop)
            loop_thread.join()
            loop.close()
        assert seen == {n: (('req', n), 1) for n in range(3)}
        assert x.top is None

    def test_gevent_greenlets_are_isolated(self):
        gevent = pytest.importorskip('gevent')
        x = LocalStack()

        def target(n):
            x.push(n)
            gevent.sleep(0.01)
            stack = x.stack
            return stack, x.pop()

        greenlets = [gevent.spawn(target, n) for n in range(3)]
        gevent.joinall(greenlets, raise_error=True)
        assert [g.value for g in greenlets] == [([n], n) for n in range(3)]
        assert x.top is None


class test_task_stacks_are_context_local:

    def test_request_and_current_task(self, app):
        @app.task(shared=False)
        def add(x, y):
            return x + y

        assert isinstance(add.request_stack, LocalStack)

        async def worker(n):
            add.push_request(id=f'id-{n}')
            _state.push_current_task(add)
            try:
                await asyncio.sleep(0)
                return add.request.id, _state.get_current_task() is add
            finally:
                _state.pop_current_task()
                add.pop_request()

        async def main():
            return await asyncio.gather(*(worker(n) for n in range(3)))

        assert asyncio.run(main()) == [(f'id-{n}', True) for n in range(3)]
        assert add.request_stack.top is None
        assert _state.get_current_task() is None

    def test_request_stack_per_task_class(self, app):
        @app.task(shared=False)
        def a():
            pass

        @app.task(shared=False)
        def b():
            pass

        assert a.request_stack is not b.request_stack
        assert Task.request_stack is None


class test_LocalManager:

    def test_init(self):
        x = LocalManager()
        assert x.locals == []
        assert x.ident_func

        def ident():
            return 1
        loc = Local()
        x = LocalManager([loc], ident_func=ident)
        assert x.locals == [loc]
        x = LocalManager(loc, ident_func=ident)
        assert x.locals == [loc]
        assert x.ident_func is ident
        assert x.locals[0].__ident_func__ is ident
        assert x.get_ident() == 1

        with patch('celery.utils.threads.release_local') as release:
            x.cleanup()
            release.assert_called_with(loc)

        assert repr(x)
