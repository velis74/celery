from unittest.mock import Mock, patch

import pytest

import t.skip
from celery.exceptions import ImproperlyConfigured
from celery.worker.components import Beat, Consumer, Hub, Pool, Timer

# some of these are tested in test_worker, so I've only written tests
# here to complete coverage.  Should move everything to this module at some
# point [-ask]


class test_Timer:

    def test_create__eventloop(self):
        w = Mock(name='w')
        w.use_eventloop = True
        Timer(w).create(w)
        assert not w.timer.queue


class test_Hub:

    def setup_method(self):
        self.w = Mock(name='w')
        self.hub = Hub(self.w)
        self.w.hub = Mock(name='w.hub')

    @patch('celery.worker.components.set_event_loop')
    @patch('celery.worker.components.get_event_loop')
    def test_create(self, get_event_loop, set_event_loop):
        self.hub._patch_thread_primitives = Mock(name='ptp')
        assert self.hub.create(self.w) is self.hub
        self.hub._patch_thread_primitives.assert_called_with(self.w)

    def test_start(self):
        self.hub.start(self.w)

    def test_stop(self):
        self.hub.stop(self.w)
        self.w.hub.close.assert_called_with()

    def test_terminate(self):
        self.hub.terminate(self.w)
        self.w.hub.close.assert_called_with()


class test_Pool:

    def test_close_terminate(self):
        w = Mock()
        comp = Pool(w)
        pool = w.pool = Mock()
        comp.close(w)
        pool.close.assert_called_with()
        comp.terminate(w)
        pool.terminate.assert_called_with()

        w.pool = None
        comp.close(w)
        comp.terminate(w)

    @t.skip.if_win32
    def test_create_when_eventloop(self):
        w = Mock()
        w.use_eventloop = w.pool_putlocks = w.pool_cls.uses_semaphore = True
        comp = Pool(w)
        w.pool = Mock()
        comp.create(w)
        assert w.process_task is w._process_task_sem

    def test_create_calls_instantiate_with_max_memory(self):
        w = Mock()
        w.use_eventloop = w.pool_putlocks = w.pool_cls.uses_semaphore = True
        comp = Pool(w)
        comp.instantiate = Mock()
        w.max_memory_per_child = 32

        comp.create(w)

        assert comp.instantiate.call_args[1]['max_memory_per_child'] == 32

    @t.skip.if_win32
    def test_create_forking_enable_fork(self):
        w = Mock()
        w.use_eventloop = w.pool_putlocks = w.pool_cls.uses_semaphore = True
        w.pool_start_method = 'fork'
        comp = Pool(w)
        comp.instantiate = Mock()

        comp.create(w)

        assert comp.instantiate.call_args[1]['forking_enable'] is True

    def test_create_forking_enable_spawn(self):
        w = Mock()
        w.use_eventloop = False
        w.pool_putlocks = w.pool_cls.uses_semaphore = True
        w.pool_start_method = 'spawn'
        comp = Pool(w)
        comp.instantiate = Mock()

        comp.create(w)

        assert comp.instantiate.call_args[1]['forking_enable'] is False

    @t.skip.if_win32
    def test_create_spawn_with_async_pool(self):
        w = Mock()
        w.use_eventloop = w.pool_putlocks = w.pool_cls.uses_semaphore = True
        w.pool_start_method = 'spawn'
        comp = Pool(w)
        comp.instantiate = Mock()

        comp.create(w)

        assert comp.instantiate.call_args[1]['forking_enable'] is False

    @t.skip.if_win32
    def test_create_without_async_concurrency(self):
        w = Mock()
        w.use_eventloop = w.pool_putlocks = w.pool_cls.uses_semaphore = True
        w.async_concurrency = 0
        comp = Pool(w)
        comp.instantiate = Mock()
        comp.instantiate.return_value.task_semaphore = None

        comp.create(w)

        assert 'async_concurrency' not in comp.instantiate.call_args[1]
        assert w._quick_acquire == w.semaphore.acquire
        assert w.semaphore.initial_value == w.min_concurrency

    @t.skip.if_win32
    def test_create_with_async_concurrency(self):
        w = Mock()
        w.use_eventloop = w.pool_putlocks = w.pool_cls.uses_semaphore = True
        w.async_concurrency, w.async_processes = 50, 2
        comp = Pool(w)
        comp.instantiate = Mock()
        task_semaphore = comp.instantiate.return_value.task_semaphore

        comp.create(w)

        kwargs = comp.instantiate.call_args[1]
        assert kwargs['async_concurrency'] == 50
        assert kwargs['async_processes'] == 2
        assert w.semaphore is task_semaphore
        assert w._quick_acquire is task_semaphore.acquire
        assert w._quick_release is task_semaphore.release

    def test_create_threaded_keeps_semaphore_unset(self):
        w = Mock()
        w.use_eventloop = False
        w.async_concurrency = 0
        comp = Pool(w)
        comp.instantiate = Mock()
        w.semaphore = None

        comp.create(w)

        assert w.semaphore is None


class test_Consumer:

    @pytest.mark.parametrize('async_concurrency,expected', [(0, 12), (5, 32)])
    def test_create_prefetch_count(self, async_concurrency, expected):
        w = Mock()
        w.max_concurrency = None
        w.concurrency = 3
        w.async_concurrency = async_concurrency
        w.prefetch_multiplier = 4
        comp = Consumer(w)
        comp.instantiate = Mock()

        comp.create(w)

        assert comp.instantiate.call_args[1]['initial_prefetch_count'] == expected


class test_Beat:

    def test_create__green(self):
        w = Mock(name='w')
        w.pool_cls.__module__ = 'foo_gevent'
        with pytest.raises(ImproperlyConfigured):
            Beat(w).create(w)

    def test_create__green_string_pool_cls(self):
        """Test Beat.create raises ImproperlyConfigured when pool_cls is a string like 'gevent'."""
        w = Mock(name='w')
        w.pool_cls = 'gevent'  # pool_cls can be a string instead of a class
        with pytest.raises(ImproperlyConfigured):
            Beat(w).create(w)

    def test_create__green_string_pool_cls_eventlet(self):
        """Test Beat.create raises ImproperlyConfigured when pool_cls is 'eventlet'."""
        w = Mock(name='w')
        w.pool_cls = 'eventlet'
        with pytest.raises(ImproperlyConfigured):
            Beat(w).create(w)
