import asyncio
import functools
import sys
import types
from contextlib import contextmanager
from unittest.mock import MagicMock, Mock, patch

import pytest

from celery import Celery, Task, signals
from celery.app.trace import set_task_awaiter
from celery.concurrency.base import BasePool
from celery.concurrency.prefork import TaskPool as PreforkTaskPool
from celery.concurrency.thread import TaskPool as ThreadTaskPool
from celery.fixups.django import (DjangoFixup, DjangoWorkerFixup, FixupWarning, _in_event_loop, _is_coroutine_task,
                                  _maybe_close_fd, fixup)
from celery.local import Proxy
from t.unit import conftest


class FixupCase:
    Fixup = None

    @contextmanager
    def fixup_context(self, app, **kwargs):
        with patch('celery.fixups.django.DjangoWorkerFixup.validate_models'):
            with patch('celery.fixups.django.symbol_by_name') as symbyname:
                with patch('celery.fixups.django.import_module') as impmod:
                    worker = Mock()
                    worker.pool_cls = Mock(__module__='celery.concurrency.prefork')
                    worker.async_concurrency = 0
                    f = self.Fixup(app, **kwargs)
                    f.worker = worker
                    yield f, impmod, symbyname


class test_DjangoFixup(FixupCase):
    Fixup = DjangoFixup

    def test_setting_default_app(self):
        from celery import _state
        prev, _state.default_app = _state.default_app, None
        try:
            app = Mock(name='app')
            DjangoFixup(app)
            app.set_default.assert_called_with()
        finally:
            _state.default_app = prev

    @patch('celery.fixups.django.DjangoWorkerFixup')
    def test_worker_fixup_property(self, DjangoWorkerFixup):
        f = DjangoFixup(self.app)
        f._worker_fixup = None
        assert f.worker_fixup is DjangoWorkerFixup()
        assert f.worker_fixup is DjangoWorkerFixup()

    def test_on_import_modules(self):
        f = DjangoFixup(self.app)
        f.worker_fixup = Mock(name='worker_fixup')
        f.on_import_modules()
        f.worker_fixup.validate_models.assert_called_with()

    def test_autodiscover_tasks(self, patching):
        patching.modules('django.apps')
        from django.apps import apps
        f = DjangoFixup(self.app)
        configs = [Mock(name='c1'), Mock(name='c2')]
        apps.get_app_configs.return_value = configs
        assert f.autodiscover_tasks() == [c.name for c in configs]

    @pytest.mark.masked_modules('django')
    def test_fixup_no_django(self, patching, mask_modules):
        with patch('celery.fixups.django.DjangoFixup') as Fixup:
            patching.setenv('DJANGO_SETTINGS_MODULE', '')
            fixup(self.app)
            Fixup.assert_not_called()

            patching.setenv('DJANGO_SETTINGS_MODULE', 'settings')
            with pytest.warns(FixupWarning):
                fixup(self.app)
            Fixup.assert_not_called()

    def test_fixup(self, patching):
        with patch('celery.fixups.django.DjangoFixup') as Fixup:
            patching.setenv('DJANGO_SETTINGS_MODULE', '')
            fixup(self.app)
            Fixup.assert_not_called()

            patching.setenv('DJANGO_SETTINGS_MODULE', 'settings')
            with conftest.module_exists('django'):
                import django
                django.VERSION = (1, 11, 1)
                fixup(self.app)
                Fixup.assert_called()

    def test_maybe_close_fd(self):
        with patch('os.close'):
            _maybe_close_fd(Mock())
            _maybe_close_fd(object())

    def test_init(self):
        with self.fixup_context(self.app) as (f, importmod, sym):
            assert f

    @pytest.mark.patched_module(
        'django',
        'django.db',
        'django.db.transaction',
    )
    def test_install(self, patching, module):
        self.app.loader = Mock()
        self.cw = patching('os.getcwd')
        self.p = patching('sys.path')
        self.sigs = patching('celery.fixups.django.signals')
        with self.fixup_context(self.app) as (f, _, _):
            self.cw.return_value = '/opt/vandelay'
            f.install()
            self.sigs.import_modules.connect.assert_called_with(
                f.on_import_modules, sender=self.app,
            )
            self.sigs.worker_init.connect.assert_called_with(
                f.on_worker_init, dispatch_uid=f._worker_init_uid,
            )
            assert self.app.loader.now == f.now

            # Specialized DjangoTask class is used
            assert self.app.task_cls == 'celery.contrib.django.task:DjangoTask'
            from celery.contrib.django.task import DjangoTask
            assert issubclass(f.app.Task, DjangoTask)
            assert hasattr(f.app.Task, 'delay_on_commit')
            assert hasattr(f.app.Task, 'apply_async_on_commit')

            self.p.insert.assert_called_with(0, '/opt/vandelay')

    def test_install_custom_user_task(self, patching):
        patching('celery.fixups.django.signals')

        self.app.task_cls = 'myapp.celery.tasks:Task'
        self.app._custom_task_cls_used = True

        with self.fixup_context(self.app) as (f, _, _):
            f.install()
            # Specialized DjangoTask class is NOT used,
            # The one from the user's class is
            assert self.app.task_cls == 'myapp.celery.tasks:Task'

    def test_install_custom_user_task_as_class_attribute(self, patching):
        patching('celery.fixups.django.signals')

        from celery.app import Celery

        class MyCeleryApp(Celery):
            task_cls = 'myapp.celery.tasks:Task'

        app = MyCeleryApp('mytestapp')

        with self.fixup_context(app) as (f, _, _):
            f.install()
            # Specialized DjangoTask class is NOT used,
            # The one from the user's class is
            assert app.task_cls == 'myapp.celery.tasks:Task'

    @pytest.mark.patched_module(
        'django',
        'django.db',
        'django.db.transaction',
    )
    def test_install_subclassed_app_without_custom_task_cls(self, patching, module):
        patching('celery.fixups.django.signals')

        from celery.app import Celery

        class MyCeleryApp(Celery):
            pass

        app = MyCeleryApp('mytestapp')

        with self.fixup_context(app) as (f, _, _):
            f.install()
            # Subclassing alone is not a custom task class,
            # so the specialized DjangoTask is still used
            assert app.task_cls == 'celery.contrib.django.task:DjangoTask'
            from celery.contrib.django.task import DjangoTask
            assert issubclass(app.Task, DjangoTask)
            assert hasattr(app.Task, 'delay_on_commit')
            assert hasattr(app.Task, 'apply_async_on_commit')

    def test_install_custom_user_task_inherited_from_base_app(self, patching):
        patching('celery.fixups.django.signals')

        from celery.app import Celery

        class BaseCeleryApp(Celery):
            task_cls = 'myapp.celery.tasks:Task'

        class MyCeleryApp(BaseCeleryApp):
            pass

        app = MyCeleryApp('mytestapp')

        with self.fixup_context(app) as (f, _, _):
            f.install()
            # Specialized DjangoTask class is NOT used,
            # The one inherited from the user's base class is
            assert app.task_cls == 'myapp.celery.tasks:Task'

    def test_now(self):
        with self.fixup_context(self.app) as (f, _, _):
            assert f.now(utc=True)
            f._now.assert_not_called()
            assert f.now(utc=False)
            f._now.assert_called()

    def test_on_worker_init(self):
        with self.fixup_context(self.app) as (f, _, _):
            with patch('celery.fixups.django.DjangoWorkerFixup') as DWF:
                mock_worker = Mock(name="worker", app=f.app)
                f.on_worker_init(sender=mock_worker)
                assert DWF.return_value.worker == mock_worker

                DWF.assert_called_with(f.app)
                DWF.return_value.install.assert_called_with()
                assert f._worker_fixup is DWF.return_value

    def test_on_worker_init_ignores_worker_of_other_app(self):
        with self.fixup_context(self.app) as (f, _, _):
            with patch('celery.fixups.django.DjangoWorkerFixup') as DWF:
                other_worker = Mock(name="worker", app=Mock(name="other_app"))
                with patch('celery.fixups.django.logger') as logger:
                    f.on_worker_init(sender=other_worker)
                DWF.assert_not_called()
                assert f._worker_fixup is None
                logger.debug.assert_called_once()
                assert logger.debug.call_args[0][1:] == (
                    f.app, other_worker, other_worker.app,
                )

    def test_on_worker_init_resolves_proxy_app(self):
        with self.fixup_context(self.app) as (f, _, _):
            with patch('celery.fixups.django.DjangoWorkerFixup') as DWF:
                worker = Mock(name="worker", app=Proxy(lambda: f.app))
                assert worker.app is not f.app
                f.on_worker_init(sender=worker)
                assert DWF.return_value.worker is worker
                DWF.return_value.install.assert_called_once_with()

    def test_on_worker_init_ignores_proxy_of_other_app(self):
        with self.fixup_context(self.app) as (f, _, _):
            with patch('celery.fixups.django.DjangoWorkerFixup') as DWF:
                other_app = Mock(name="other_app")
                worker = Mock(name="worker", app=Proxy(lambda: other_app))
                f.on_worker_init(sender=worker)
                DWF.assert_not_called()

    def test_on_worker_init_sender_without_app(self):
        with self.fixup_context(self.app) as (f, _, _):
            with patch('celery.fixups.django.DjangoWorkerFixup') as DWF:
                sender = Mock(name="sender", spec=[])
                assert not hasattr(sender, 'app')
                f.on_worker_init(sender=sender)
                assert DWF.return_value.worker is sender
                DWF.return_value.install.assert_called_once_with()

    def test_on_worker_init_fixup_with_proxy_app(self):
        # the fixup's own app can be a proxy too, e.g. current_app.
        with self.fixup_context(Proxy(lambda: self.app)) as (f, _, _):
            assert f.app is not self.app
            with patch('celery.fixups.django.DjangoWorkerFixup') as DWF:
                for worker_app in (self.app, Proxy(lambda: self.app)):
                    DWF.reset_mock()
                    worker = Mock(name="worker", app=worker_app)
                    f.on_worker_init(sender=worker)
                    assert DWF.return_value.worker is worker
                    DWF.return_value.install.assert_called_once_with()

                DWF.reset_mock()
                f.on_worker_init(
                    sender=Mock(name="worker", app=Mock(name="other_app")),
                )
                DWF.return_value.install.assert_not_called()

    def test_worker_init_uid_is_unique_per_instance(self):
        f1, f2 = DjangoFixup(self.app), DjangoFixup(self.app)
        assert f1._worker_init_uid != f2._worker_init_uid

    def test_worker_init_uid_is_a_random_token(self):
        # ids can be recycled after garbage collection, so the uid has to
        # come from uuid4() and not from id(self).
        with patch('celery.fixups.django.uuid4') as uuid4:
            uuid4.return_value.hex = 'token'
            f = DjangoFixup(self.app)
        assert f._worker_init_uid[-1] == 'token'

    def _install_real_fixups(self, *apps):
        with patch('celery.fixups.django.symbol_by_name'), \
                patch('sys.path'):
            fixups = [DjangoFixup(app).install() for app in apps]
        for f in fixups:
            f.worker_fixup = Mock(name='worker_fixup')
        return fixups

    def _uninstall_real_fixups(self, *fixups):
        for f in fixups:
            signals.import_modules.disconnect(
                f.on_import_modules, sender=f.app,
            )
            signals.worker_init.disconnect(dispatch_uid=f._worker_init_uid)

    def test_on_import_modules_only_for_own_app(self):
        other_app = Celery(set_as_current=False)
        f1, f2 = self._install_real_fixups(self.app, other_app)
        try:
            signals.import_modules.send(sender=other_app)
            f1.worker_fixup.validate_models.assert_not_called()
            f2.worker_fixup.validate_models.assert_called_once_with()

            signals.import_modules.send(sender=self.app)
            f1.worker_fixup.validate_models.assert_called_once_with()
            f2.worker_fixup.validate_models.assert_called_once_with()
        finally:
            self._uninstall_real_fixups(f1, f2)

    def test_on_import_modules_fixup_with_proxy_app(self):
        # connected with sender=<proxy>, sent with the real app: the signal
        # resolves the proxy, so it still reaches the fixup.
        other_app = Celery(set_as_current=False)
        f, = self._install_real_fixups(Proxy(lambda: self.app))
        try:
            assert f.app is not self.app
            signals.import_modules.send(sender=other_app)
            f.worker_fixup.validate_models.assert_not_called()

            signals.import_modules.send(sender=self.app)
            f.worker_fixup.validate_models.assert_called_once_with()
        finally:
            self._uninstall_real_fixups(f)

    def test_worker_init_only_for_own_app(self):
        other_app = Celery(set_as_current=False)
        f1, f2 = self._install_real_fixups(self.app, other_app)
        try:
            worker = Mock(name='worker', app=other_app)
            signals.worker_init.send(sender=worker)
            f1.worker_fixup.install.assert_not_called()
            f2.worker_fixup.install.assert_called_once_with()
            assert f2.worker_fixup.worker is worker
        finally:
            self._uninstall_real_fixups(f1, f2)

    def test_on_worker_init_warns_without_sender(self):
        with self.fixup_context(self.app) as (f, _, _):
            with patch("celery.fixups.django.DjangoWorkerFixup"):
                with pytest.warns(FixupWarning, match="called without a sender"):
                    f.on_worker_init(sender=None)


class InterfaceError(Exception):
    pass


def run_in_event_loop(fun, *args, **kwargs):
    """Call *fun* from a coroutine running on a new event loop."""
    async def main():
        return fun(*args, **kwargs)
    return asyncio.run(main())


def test_in_event_loop():
    assert not _in_event_loop()
    assert run_in_event_loop(_in_event_loop)


class test_is_coroutine_task:

    def test_sync_task(self):
        @self.app.task(shared=False)
        def add(x, y):
            return x + y
        assert not _is_coroutine_task(add)

    def test_async_task(self):
        @self.app.task(shared=False)
        async def add(x, y):
            return x + y
        assert _is_coroutine_task(add)

    def test_task_with_async_variant(self):
        async def aadd(x, y):
            return x + y

        def add(x, y):
            return x + y
        add.__celery_async__ = aadd
        assert _is_coroutine_task(self.app.task(shared=False)(add))

    def test_async_bound_task(self):
        @self.app.task(shared=False, bind=True)
        async def add(self, x, y):
            return x + y
        assert _is_coroutine_task(add)

    def test_async_task_with_autoretry(self):
        @self.app.task(shared=False, autoretry_for=(KeyError,))
        async def add(x, y):
            return x + y
        assert hasattr(add, '_orig_run')
        assert _is_coroutine_task(add)

    def test_sync_task_with_autoretry(self):
        @self.app.task(shared=False, autoretry_for=(KeyError,))
        def add(x, y):
            return x + y
        assert not _is_coroutine_task(add)

    def test_async_task_with_pydantic(self):
        pytest.importorskip('pydantic')

        @self.app.task(shared=False, pydantic=True)
        async def add(x: int, y: int) -> int:
            return x + y
        assert _is_coroutine_task(add)

    def test_async_class_based_task(self):
        class AddTask(Task):
            async def run(self, x, y):
                return x + y
        assert _is_coroutine_task(AddTask())

    def test_sync_class_based_task(self):
        class AddTask(Task):
            def run(self, x, y):
                return x + y
        assert not _is_coroutine_task(AddTask())

    def test_sync_wrapper_around_async_function(self):
        def run_sync(fun):
            @functools.wraps(fun)
            def wrapper(*args, **kwargs):
                return asyncio.run(fun(*args, **kwargs))
            return wrapper

        @self.app.task(shared=False)
        @run_sync
        async def add(x, y):
            return x + y
        assert _is_coroutine_task(add)
        assert add(1, 2) == 3


def fake_asgiref_sync(with_executors=True):
    """Module with the parts of ``asgiref.sync`` used by the fixup."""
    module = types.ModuleType('asgiref.sync')
    current, executors = [], {}

    class SyncToAsync:
        pass

    if with_executors:
        SyncToAsync.context_to_thread_executor = executors

    class ThreadSensitiveContext:
        async def __aenter__(self):
            current.append(self)
            return self

        async def __aexit__(self, *exc_info):
            executors.pop(current.pop(), None)

    def sync_to_async(fun, thread_sensitive=True):
        async def call(*args, **kwargs):
            # Starts the thread of the current context.
            executors[current[-1]] = Mock()
            return fun(*args, **kwargs)
        return call

    module.SyncToAsync = SyncToAsync
    module.ThreadSensitiveContext = ThreadSensitiveContext
    module.sync_to_async = sync_to_async
    return module


class test_DjangoWorkerFixup(FixupCase):
    Fixup = DjangoWorkerFixup

    @pytest.mark.parametrize('async_concurrency', [0, 5])
    def test_install_async_task_wrapper(self, async_concurrency):
        asgiref_sync = fake_asgiref_sync()
        with self.fixup_context(self.app) as (f, _, _), \
                patch('celery.fixups.django.signals'), \
                patch.dict(sys.modules, {'asgiref.sync': asgiref_sync}), \
                patch('celery.concurrency.aioworker.add_task_wrapper') as add:
            f.worker.async_concurrency = async_concurrency
            f.install()
            if async_concurrency:
                add.assert_called_once_with(f.run_async_task)
            else:
                add.assert_not_called()

    def test_install_async_task_wrapper_without_asgiref(self):
        with self.fixup_context(self.app) as (f, _, _), \
                patch.dict(sys.modules, {'asgiref.sync': None}), \
                patch('celery.fixups.django.logger') as logger, \
                patch('celery.concurrency.aioworker.add_task_wrapper') as add:
            f.install_async_task_wrapper()
            add.assert_not_called()
            logger.warning.assert_called_once()

    @pytest.mark.parametrize('uses_thread,with_executors,closes', [
        (True, True, True),
        (False, True, False),
        (False, False, True),
    ])
    def test_run_async_task(self, uses_thread, with_executors, closes):
        asgiref_sync = fake_asgiref_sync(with_executors)

        async def task():
            if uses_thread:
                await asgiref_sync.sync_to_async(lambda: None)()
            return 42

        with self.fixup_context(self.app) as (f, _, _), \
                patch.dict(sys.modules, {'asgiref.sync': asgiref_sync}), \
                patch.object(f, 'close_task_connections') as close:
            assert asyncio.run(f.run_async_task(task())) == 42
        assert close.called is closes

    def test_run_async_task_closes_after_error(self):
        asgiref_sync = fake_asgiref_sync()

        async def task():
            await asgiref_sync.sync_to_async(lambda: None)()
            raise KeyError('task')

        with self.fixup_context(self.app) as (f, _, _), \
                patch.dict(sys.modules, {'asgiref.sync': asgiref_sync}), \
                patch.object(f, 'close_task_connections') as close:
            with pytest.raises(KeyError):
                asyncio.run(f.run_async_task(task()))
        close.assert_called_once_with()

    def test_close_task_connections(self):
        with self.fixup_context(self.app) as (f, _, _):
            f.db_reuse_max = 10
            with patch.object(f, '_close_database') as _close, \
                    patch.object(f, 'close_cache') as close_cache:
                f.close_task_connections()
            _close.assert_called_once_with()
            close_cache.assert_called_once_with()

    def test_init(self):
        with self.fixup_context(self.app) as (f, importmod, sym):
            assert f

    def test_install(self):
        self.app.conf = {'CELERY_DB_REUSE_MAX': None}
        self.app.loader = Mock()
        with self.fixup_context(self.app) as (f, _, _):
            with patch('celery.fixups.django.signals') as sigs:
                f.install()
                sigs.beat_embedded_init.connect.assert_called_with(
                    f.close_database,
                )
                sigs.task_prerun.connect.assert_called_with(f.on_task_prerun)
                sigs.task_postrun.connect.assert_called_with(f.on_task_postrun)
                sigs.worker_process_init.connect.assert_called_with(
                    f.on_worker_process_init,
                )

    def test_on_worker_process_init(self, patching):
        with self.fixup_context(self.app) as (f, _, _):
            with patch('celery.fixups.django._maybe_close_fd', side_effect=InterfaceError) as mcf:
                _all = f._db.connections.all = Mock()
                conns = _all.return_value = [
                    Mock(), MagicMock(),
                ]
                conns[0].connection = None
                with patch.object(f, 'close_cache'):
                    with patch.object(f, '_close_database'):
                        f.interface_errors = (InterfaceError, )
                        f.on_worker_process_init()
                        mcf.assert_called_with(conns[1].connection)
                        f.close_cache.assert_called_with()
                        f._close_database.assert_called_with()

                        f.validate_models = Mock(name='validate_models')
                        patching.setenv('FORKED_BY_MULTIPROCESSING', '1')
                        f.on_worker_process_init()
                        f.validate_models.assert_called_with()

    def test_on_task_prerun(self):
        task = Mock()
        with self.fixup_context(self.app) as (f, _, _):
            task.request.is_eager = False
            with patch.object(f, 'close_database'):
                f.on_task_prerun(task)
                f.close_database.assert_called_with()

            task.request.is_eager = True
            with patch.object(f, 'close_database'):
                f.on_task_prerun(task)
                f.close_database.assert_not_called()

    def test_on_task_postrun(self):
        task = Mock()
        with self.fixup_context(self.app) as (f, _, _):
            with patch.object(f, 'close_cache'):
                task.request.is_eager = False
                with patch.object(f, 'close_database'):
                    f.on_task_postrun(task)
                    f.close_database.assert_called()
                    f.close_cache.assert_called()

            # when a task is eager, don't close connections
            with patch.object(f, 'close_cache'):
                task.request.is_eager = True
                with patch.object(f, 'close_database'):
                    f.on_task_postrun(task)
                    f.close_database.assert_not_called()
                    f.close_cache.assert_not_called()

    @pytest.mark.parametrize('method', ['on_task_prerun', 'on_task_postrun'])
    def test_on_task_signal_skips_coroutine_task(self, method):
        @self.app.task(shared=False)
        async def add(x, y):
            return x + y
        add.push_request(is_eager=False)
        set_task_awaiter(Mock(name='awaiter'))
        try:
            with self.fixup_context(self.app) as (f, _, _):
                with patch.object(f, 'close_database'), patch.object(f, 'close_cache'):
                    getattr(f, method)(add)
                    getattr(f, method)(add)
                    f.close_database.assert_not_called()
                    f.close_cache.assert_not_called()
                assert f._coroutine_task_types == {type(add): True}
        finally:
            set_task_awaiter(None)
            add.pop_request()

    @pytest.mark.parametrize('method', ['on_task_prerun', 'on_task_postrun'])
    def test_on_task_signal_handles_coroutine_task_without_awaiter(self, method):
        # Without a task awaiter the tracer does not await the coroutine.
        @self.app.task(shared=False)
        async def add(x, y):
            return x + y
        add.push_request(is_eager=False)
        try:
            with self.fixup_context(self.app) as (f, _, _):
                with patch.object(f, 'close_database'), patch.object(f, 'close_cache'):
                    getattr(f, method)(add)
                    f.close_database.assert_called_once_with()
                assert f._coroutine_task_types == {}
        finally:
            add.pop_request()

    def test_on_task_signal_handles_sync_wrapper_without_awaiter(self):
        # A sync decorator that runs the coroutine itself, on a worker
        # without async concurrency.
        def run_sync(fun):
            @functools.wraps(fun)
            def wrapper(*args, **kwargs):
                return asyncio.run(fun(*args, **kwargs))
            return wrapper

        @self.app.task(shared=False)
        @run_sync
        async def add(x, y):
            return x + y
        add.push_request(is_eager=False)
        try:
            with self.fixup_context(self.app) as (f, _, _):
                with patch.object(f, 'close_database'), patch.object(f, 'close_cache'):
                    f.on_task_prerun(add)
                    f.on_task_postrun(add)
                    assert f.close_database.call_count == 2
                    f.close_cache.assert_called_once_with()
        finally:
            add.pop_request()

    @pytest.mark.parametrize('method', ['on_task_prerun', 'on_task_postrun'])
    def test_on_task_signal_handles_sync_task(self, method):
        @self.app.task(shared=False)
        def add(x, y):
            return x + y
        add.push_request(is_eager=False)
        try:
            with self.fixup_context(self.app) as (f, _, _):
                with patch.object(f, 'close_database'), patch.object(f, 'close_cache'):
                    getattr(f, method)(add)
                    f.close_database.assert_called_once_with()
        finally:
            add.pop_request()

    def test_close_connections(self):
        with self.fixup_context(self.app) as (f, _, _):
            with patch.object(f, 'close_database'), patch.object(f, 'close_cache'):
                f.close_connections()
                f.close_database.assert_called_once_with()
                f.close_cache.assert_called_once_with()

    def test_close_database_in_event_loop(self):
        with self.fixup_context(self.app) as (f, _, _):
            with patch.object(f, '_close_database') as _close:
                f.db_reuse_max = 10
                f._db_recycles = 3
                run_in_event_loop(f.close_database)
                _close.assert_not_called()
                assert f._db_recycles == 3

    def test__close_database_in_event_loop(self):
        with self.fixup_context(self.app) as (f, _, _):
            conn = Mock()
            f._db.connections.all = Mock(return_value=[conn])
            run_in_event_loop(f._close_database)
            f._db.connections.all.assert_not_called()
            conn.close.assert_not_called()

            f._close_database()
            conn.close.assert_called_once_with()

    def test_close_cache_in_event_loop(self):
        with self.fixup_context(self.app) as (f, _, _):
            run_in_event_loop(f.close_cache)
            f._cache.close_caches.assert_not_called()

    def test_on_worker_process_init_in_event_loop(self):
        with self.fixup_context(self.app) as (f, _, _):
            conn = MagicMock()
            f._db.connections.all = Mock(return_value=[conn])
            with patch('celery.fixups.django._maybe_close_fd') as mcf:
                run_in_event_loop(f.on_worker_process_init)
                mcf.assert_called_once_with(conn.connection)
            conn.close.assert_not_called()
            f._cache.close_caches.assert_not_called()

    def test_close_database(self):
        with self.fixup_context(self.app) as (f, _, _):
            with patch.object(f, '_close_database') as _close:
                f.db_reuse_max = None
                f.close_database()
                _close.assert_called_with()
                _close.reset_mock()

                f.db_reuse_max = 10
                f._db_recycles = 3
                f.close_database()
                _close.assert_not_called()
                assert f._db_recycles == 4
                _close.reset_mock()

                f._db_recycles = 20
                f.close_database()
                _close.assert_called_with()
                assert f._db_recycles == 1

    def test__close_database(self):
        with self.fixup_context(self.app) as (f, _, _):
            conns = [Mock(), Mock(), Mock()]
            conns[1].close.side_effect = KeyError('already closed')
            f.DatabaseError = KeyError
            f.interface_errors = ()

            f._db.connections = Mock()  # ConnectionHandler
            f._db.connections.all.side_effect = lambda initialized_only: conns

            f._close_database()
            conns[0].close.assert_called_with()
            conns[1].close.assert_called_with()
            conns[2].close.assert_called_with()

            conns[1].close.side_effect = KeyError(
                'omg')
            with pytest.raises(KeyError):
                f._close_database()

    def test__close_database_django_pre_41(self):
        """Test that Django < 4.1 (without initialized_only parameter) is handled."""
        with self.fixup_context(self.app) as (f, _, _):
            conns = [Mock(), Mock()]
            f.DatabaseError = KeyError
            f.interface_errors = ()

            # Mock Django < 4.1 behavior: connections.all() doesn't accept initialized_only
            f._db.connections = Mock()

            def all_without_initialized_only(**kwargs):
                if 'initialized_only' in kwargs:
                    raise TypeError("all() got an unexpected keyword argument 'initialized_only'")
                return conns

            f._db.connections.all = Mock(side_effect=all_without_initialized_only)

            # Should fall back to calling all() without initialized_only
            f._close_database()

            # Verify it was called twice: first with initialized_only (raises), then without
            assert f._db.connections.all.call_count == 2
            # First call with initialized_only=True
            f._db.connections.all.assert_any_call(initialized_only=True)
            # Second call without arguments (fallback)
            f._db.connections.all.assert_any_call()

            # Verify connections were closed
            conns[0].close.assert_called_with()
            conns[1].close.assert_called_with()

    def test_close_database_always_closes_connections(self):
        with self.fixup_context(self.app) as (f, _, _):
            conn = Mock()
            f._db.connections.all = Mock(return_value=[conn])
            f.close_database()
            conn.close.assert_called_once_with()
            # close_if_unusable_or_obsolete is not safe to call in all conditions, so avoid using
            # it to optimize connection handling.
            conn.close_if_unusable_or_obsolete.assert_not_called()

    def test_close_database_skip_conn_pool(self):
        class Connection:
            """Mock connection without `close_pool` method."""
            alias = 'default'

            def close(self):
                pass

        with self.fixup_context(self.app) as (f, _, _):
            conn = Mock(spec=Connection)
            f._db.connections.all = Mock(return_value=[conn])
            f.close_database()
            assert not hasattr(conn, "close_pool")
            conn.close.assert_called_once_with()

    def test_close_database_suppresses_close_pool_keyerror(self):
        with self.fixup_context(self.app) as (f, _, _):
            conn = Mock()
            conn.close_pool = Mock(side_effect=KeyError("pool already closed"))
            f._db.connections.all = Mock(return_value=[conn])
            f.close_database()  # should not raise
            conn.close.assert_called_once_with()
            conn.close_pool.assert_called_once_with()

    def test_close_database_conn_pool_based_on_settings(self):
        class DJSettings:
            DATABASES = {}

        with self.fixup_context(self.app) as (f, _, _):
            conn = Mock()
            conn.alias = "default"
            conn.close_pool = Mock()
            f._db.connections.all = Mock(return_value=[conn])
            f._settings = DJSettings

            f._settings.DATABASES["default"] = {"OPTIONS": {}}
            f.close_database()
            conn.close.assert_called_once_with()
            conn.close_pool.assert_not_called()

            conn.reset_mock()
            f._settings.DATABASES["default"] = {"OPTIONS": {"pool": True}}
            f.close_database()
            conn.close.assert_called_once_with()
            conn.close_pool.assert_called_once_with()

            conn.reset_mock()
            f._settings.DATABASES["default"] = {"OPTIONS": {"pool": False}}
            f.close_database()
            conn.close.assert_called_once_with()
            conn.close_pool.assert_not_called()

    def test_close_database_conn_pool_thread_pool(self):
        class DJSettings:
            DATABASES = {}

        with self.fixup_context(self.app) as (f, _, _):
            conn = Mock()
            conn.alias = "default"
            conn.close_pool = Mock()
            f._db.connections.all = Mock(return_value=[conn])
            f._settings = DJSettings

            f._settings.DATABASES["default"] = {"OPTIONS": {"pool": True}}
            f.close_database()
            conn.close.assert_called_once_with()
            conn.close_pool.assert_called_once_with()

            conn.reset_mock()
            f.worker.pool_cls = ThreadTaskPool
            assert "prefork" not in ThreadTaskPool.__module__
            f.close_database()
            conn.close.assert_called_once_with()
            conn.close_pool.assert_not_called()

    def test_close_cache_raises_error(self):
        with self.fixup_context(self.app) as (f, _, _):
            f._cache.close_caches.side_effect = AttributeError
            f.close_cache()

    def test_close_cache(self):
        with self.fixup_context(self.app) as (f, _, _):
            f.close_cache()
            f._cache.close_caches.assert_called_with()

    @pytest.mark.patched_module('django', 'django.db', 'django.core',
                                'django.core.cache', 'django.conf',
                                'django.core.checks', 'django.db.utils')
    def test_validate_models(self, patching, module):
        f = self.Fixup(self.app)
        f.django_setup = Mock(name='django.setup')
        from django.core.checks import run_checks

        f.validate_models()
        f.django_setup.assert_called_with()
        run_checks.assert_called_with()

        # test --skip-checks flag
        f.django_setup.reset_mock()
        run_checks.reset_mock()

        patching.setenv('CELERY_SKIP_CHECKS', 'true')
        f.validate_models()
        f.django_setup.assert_called_with()
        run_checks.assert_not_called()

    def test_django_setup(self, patching):
        patching('celery.fixups.django.symbol_by_name')
        patching('celery.fixups.django.import_module')
        django, = patching.modules('django')
        f = self.Fixup(self.app)
        f.django_setup()
        django.setup.assert_called_with()

    def test__is_prefork(self):
        with self.fixup_context(self.app) as (f, _, _):
            f.worker.pool_cls = Mock(__module__='celery.concurrency.prefork')
            assert f._is_prefork()

            f.worker.pool_cls = "prefork"
            assert f._is_prefork()

            f.worker.pool_cls = Mock(__module__='celery.concurrency.thread')
            assert not f._is_prefork()

            f.worker.pool_cls = PreforkTaskPool
            assert f._is_prefork()

            f.worker.pool_cls = ThreadTaskPool
            assert not f._is_prefork()

            f.worker = None
            assert not f._is_prefork()

    def test__is_prefork_uses_child_processes(self):
        class ChildProcessPool(BasePool):
            uses_child_processes = True
        ChildProcessPool.__module__ = 'celery.concurrency.aio'

        class InProcessPool(BasePool):
            pass
        InProcessPool.__module__ = 'celery.concurrency.aio'

        with self.fixup_context(self.app) as (f, _, _):
            f.worker.pool_cls = ChildProcessPool
            assert f._is_prefork()

            f.worker.pool_cls = InProcessPool
            assert not f._is_prefork()

    def test_no_recursive_worker_instantiation(self, patching):
        """Regression test: DjangoWorkerFixup must not create a WorkController in __init__.

        Historically, DjangoWorkerFixup.__init__ instantiated a WorkController when
        called with worker=None, which could cause recursive instantiation when
        invoked from worker lifecycle signals.

        This test verifies the fixed behavior:
        - DjangoWorkerFixup(app, worker=None) must not create a WorkController
        - It should instead leave self.worker unset and rely on on_worker_init
          to attach the actual worker instance later
        """
        from celery.worker import WorkController

        patching('celery.fixups.django.symbol_by_name')
        patching('celery.fixups.django.import_module')
        patching.modules('django', 'django.db', 'django.core.checks')

        # Track WorkController instantiations
        instantiation_count = {'count': 0}
        original_init = WorkController.__init__

        def tracking_init(self_worker, *args, **kwargs):
            instantiation_count['count'] += 1
            return original_init(self_worker, *args, **kwargs)

        with patch.object(WorkController, '__init__', tracking_init):
            # Creating DjangoWorkerFixup without a worker argument
            # should NOT create a WorkController instance
            DjangoWorkerFixup(self.app)

        # EXPECTED: 0 WorkController instances created
        assert instantiation_count['count'] == 0, (
            f"DjangoWorkerFixup(app) should NOT create a WorkController, "
            f"but {instantiation_count['count']} instance(s) were created. "
            f"This is the root cause of the recursion bug."
        )
