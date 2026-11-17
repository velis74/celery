"""Django-specific customization."""
import asyncio
import contextlib
import os
import sys
import warnings
from datetime import datetime, timezone
from importlib import import_module
from typing import IO, TYPE_CHECKING, Any, Dict, List, Optional, cast
from uuid import uuid4

from kombu.utils.imports import symbol_by_name
from kombu.utils.objects import cached_property

from celery import _state, signals
from celery.app.trace import get_task_awaiter, is_async_task
from celery.exceptions import FixupWarning, ImproperlyConfigured
from celery.local import Proxy
from celery.utils.log import get_logger
from celery.worker import WorkController

if TYPE_CHECKING:
    from types import ModuleType
    from typing import Protocol

    from django.db.backends.base.base import BaseDatabaseWrapper
    from django.db.utils import ConnectionHandler

    from celery.app.base import Celery
    from celery.app.task import Task

    class DjangoDBModule(Protocol):
        connections: ConnectionHandler


__all__ = ('DjangoFixup', 'fixup')

logger = get_logger(__name__)

ERR_NOT_INSTALLED = """\
Environment variable DJANGO_SETTINGS_MODULE is defined
but Django isn't installed.  Won't apply Django fix-ups!
"""


def _maybe_close_fd(fh: IO) -> None:
    try:
        os.close(fh.fileno())
    except (AttributeError, OSError, TypeError):
        # TypeError added for celery#962
        pass


def _unwrap_app(app: Any) -> Any:
    """Return the app behind a proxy such as ``current_app``."""
    return app._get_current_object() if isinstance(app, Proxy) else app


def _in_event_loop() -> bool:
    """Return true if the current thread is running an asyncio event loop."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return False
    return True


def _is_coroutine_task(task: Any) -> bool:
    """Return true if the body of *task* is a coroutine function."""
    return is_async_task(task)


def _verify_django_version(django: "ModuleType") -> None:
    if django.VERSION < (1, 11):
        raise ImproperlyConfigured('Celery 5.x requires Django 1.11 or later.')


def fixup(app: "Celery", env: str = 'DJANGO_SETTINGS_MODULE') -> Optional["DjangoFixup"]:
    """Install Django fixup if settings module environment is set."""
    SETTINGS_MODULE = os.environ.get(env)
    if SETTINGS_MODULE and 'django' not in app.loader_cls.lower():
        try:
            import django
        except ImportError:
            warnings.warn(FixupWarning(ERR_NOT_INSTALLED), stacklevel=2)
        else:
            _verify_django_version(django)
            return DjangoFixup(app).install()
    return None


class DjangoFixup:
    """Fixup installed when using Django."""

    def __init__(self, app: "Celery"):
        self.app = app
        if _state.default_app is None:
            self.app.set_default()
        self._worker_fixup: Optional["DjangoWorkerFixup"] = None
        # Not derived from id(self): an id can be reused once a fixup has
        # been garbage collected, a per-instance token can't.
        # Only one fixup per app is supported: a second one installed for
        # the same app (e.g. calling fixup(app) by hand on top of the
        # automatic one) gets its own receiver and installs its worker
        # fixup too.
        self._worker_init_uid = (
            'celery.fixups.django.DjangoFixup.on_worker_init', uuid4().hex,
        )

    def install(self) -> "DjangoFixup":
        # Need to add project directory to path.
        # The project directory has precedence over system modules,
        # so we prepend it to the path.
        sys.path.insert(0, os.getcwd())

        self._settings = symbol_by_name('django.conf:settings')
        self.app.loader.now = self.now

        if not self.app._custom_task_cls_used:
            self.app.task_cls = 'celery.contrib.django.task:DjangoTask'

        # Several Django-enabled apps can live in one process, each with its
        # own fixup, so every fixup only reacts to its own app's signals.
        signals.import_modules.connect(self.on_import_modules, sender=self.app)
        # worker_init is sent by the worker, so it can't be filtered by
        # sender: give each fixup its own dispatch_uid so they don't share
        # one receiver, and skip other apps' workers in on_worker_init.
        signals.worker_init.connect(
            self.on_worker_init, dispatch_uid=self._worker_init_uid,
        )
        return self

    @property
    def worker_fixup(self) -> "DjangoWorkerFixup":
        if self._worker_fixup is None:
            self._worker_fixup = DjangoWorkerFixup(self.app)
        return self._worker_fixup

    @worker_fixup.setter
    def worker_fixup(self, value: "DjangoWorkerFixup") -> None:
        self._worker_fixup = value

    def on_import_modules(self, **kwargs: Any) -> None:
        # call django.setup() before task modules are imported
        self.worker_fixup.validate_models()

    def on_worker_init(self, **kwargs: Any) -> None:
        worker: Optional["WorkController"] = kwargs.get("sender")
        if worker:
            # Either side may be a proxy, e.g. a worker or a fixup created
            # with ``current_app``.
            worker_app = _unwrap_app(getattr(worker, 'app', None))
            # A sender without an app can't be told apart, so it's handled
            # like before; only workers known to be another app's are skipped.
            if worker_app is not None and worker_app is not _unwrap_app(self.app):
                logger.debug(
                    'Django fixup of app %r ignores worker_init of %r: '
                    'the worker belongs to app %r, whose own fixup handles it.',
                    self.app, worker, worker_app,
                )
                return
            self.worker_fixup.worker = worker
        else:
            warnings.warn(
                "DjangoFixup.on_worker_init called without a sender (worker instance). "
                "This may indicate a misconfiguration or an internal error.",
                FixupWarning,
                stacklevel=2,
            )
        self.worker_fixup.install()

    def now(self, utc: bool = False) -> datetime:
        return datetime.now(timezone.utc) if utc else self._now()

    def autodiscover_tasks(self) -> List[str]:
        from django.apps import apps
        return [config.name for config in apps.get_app_configs()]

    @cached_property
    def _now(self) -> datetime:
        return symbol_by_name('django.utils.timezone:now')


class DjangoWorkerFixup:
    _db_recycles = 0
    worker = None  # Set via on_worker_init callback to avoid recursive WorkController instantiation

    def __init__(self, app: "Celery") -> None:
        self.app = app
        self.db_reuse_max = self.app.conf.get('CELERY_DB_REUSE_MAX', None)
        self._db = cast("DjangoDBModule", import_module('django.db'))
        self._cache = import_module('django.core.cache')
        self._settings = symbol_by_name('django.conf:settings')

        self.interface_errors = (
            symbol_by_name('django.db.utils.InterfaceError'),
        )
        self.DatabaseError = symbol_by_name('django.db:DatabaseError')
        self._coroutine_task_types: Dict[type, bool] = {}

    def django_setup(self) -> None:
        import django
        django.setup()

    def validate_models(self) -> None:
        from django.core.checks import run_checks
        self.django_setup()
        if not os.environ.get('CELERY_SKIP_CHECKS'):
            run_checks()

    def install(self) -> "DjangoWorkerFixup":
        signals.beat_embedded_init.connect(self.close_database)
        signals.task_prerun.connect(self.on_task_prerun)
        signals.task_postrun.connect(self.on_task_postrun)
        signals.worker_process_init.connect(self.on_worker_process_init)
        if getattr(self.worker, 'async_concurrency', 0):
            self.install_async_task_wrapper()
        self.close_database()
        self.close_cache()
        return self

    def install_async_task_wrapper(self) -> None:
        """Run the coroutines of async pool tasks with :meth:`run_async_task`.

        Requires ``asgiref.sync.ThreadSensitiveContext`` (asgiref 3.3).
        """
        try:
            from asgiref.sync import ThreadSensitiveContext  # noqa: F401
        except ImportError:
            logger.warning('Django fixup: asgiref.sync.ThreadSensitiveContext '
                           'is not available, so database connections of '
                           'async tasks are not closed.')
            return
        from celery.concurrency.aioworker import add_task_wrapper
        add_task_wrapper(self.run_async_task)

    async def run_async_task(self, coroutine: Any) -> Any:
        """Await the coroutine of a task in a thread-sensitive context of its own.

        Code that the task runs with ``sync_to_async(thread_sensitive=True)``,
        such as the async methods of the Django ORM, runs in a thread of the
        task.  When the task ends, the database connections and caches of
        that thread are closed and the thread exits.
        """
        from asgiref.sync import SyncToAsync, ThreadSensitiveContext, sync_to_async
        async with ThreadSensitiveContext() as context:
            try:
                return await coroutine
            finally:
                executors = getattr(SyncToAsync, 'context_to_thread_executor', None)
                # Without a thread of its own, the task has no connections.
                if executors is None or context in executors:
                    await sync_to_async(self.close_task_connections,
                                        thread_sensitive=True)()

    def close_task_connections(self) -> None:
        """Close the database connections and caches of the current thread.

        Ignores ``CELERY_DB_REUSE_MAX``.
        """
        self._close_database()
        self.close_cache()

    def on_worker_process_init(self, **kwargs: Any) -> None:
        # Child process must validate models again if on Windows,
        # or if they were started using execv.
        if os.environ.get('FORKED_BY_MULTIPROCESSING'):
            self.validate_models()

        # close connections:
        # the parent process may have established these,
        # so need to close them.

        # calling db.close() on some DB connections will cause
        # the inherited DB conn to also get broken in the parent
        # process so we need to remove it without triggering any
        # network IO that close() might cause.
        for c in self._db.connections.all():
            if c and c.connection:
                self._maybe_close_db_fd(c)

        # use the _ version to avoid DB_REUSE preventing the conn.close() call
        self._close_database()
        self.close_cache()

    def _maybe_close_db_fd(self, c: "BaseDatabaseWrapper") -> None:
        try:
            with c.wrap_database_errors:
                _maybe_close_fd(c.connection)
        except self.interface_errors:
            pass

    def _manages_connections_for(self, task: "Task") -> bool:
        if getattr(task.request, 'is_eager', False):
            return False
        if get_task_awaiter() is None:
            return True
        # Under a task awaiter, a coroutine task runs on an event loop in
        # another thread than the one that sends the prerun/postrun signals.
        task_type = type(task)
        try:
            is_coroutine = self._coroutine_task_types[task_type]
        except KeyError:
            is_coroutine = self._coroutine_task_types[task_type] = \
                _is_coroutine_task(task)
        return not is_coroutine

    def on_task_prerun(self, sender: "Task", **kwargs: Any) -> None:
        """Called before every task."""
        if self._manages_connections_for(sender):
            self.close_database()

    def on_task_postrun(self, sender: "Task", **kwargs: Any) -> None:
        # See https://groups.google.com/group/django-users/browse_thread/thread/78200863d0c07c6d/
        if self._manages_connections_for(sender):
            self.close_connections()

    def close_connections(self) -> None:
        """Close database connections and caches at the end of a task.

        Honours ``CELERY_DB_REUSE_MAX``.  Must be called without a running
        event loop in the current thread, otherwise it does nothing.
        """
        self.close_database()
        self.close_cache()

    def close_database(self, **kwargs: Any) -> None:
        if _in_event_loop():
            logger.debug('Django fixup: not closing database connections '
                         'in a thread with a running event loop.')
            return
        if not self.db_reuse_max:
            return self._close_database()
        if self._db_recycles >= self.db_reuse_max * 2:
            self._db_recycles = 0
            self._close_database()
        self._db_recycles += 1

    def _is_prefork(self) -> bool:
        if self.worker is None:
            return False
        pool_cls = self.worker.pool_cls
        if isinstance(pool_cls, str):
            return "prefork" in pool_cls
        return getattr(pool_cls, 'uses_child_processes', False) is True or "prefork" in pool_cls.__module__

    def _close_database(self) -> None:
        if _in_event_loop():
            logger.debug('Django fixup: not closing database connections '
                         'in a thread with a running event loop.')
            return
        try:
            connections = self._db.connections.all(initialized_only=True)
        except TypeError:
            # Support Django < 4.1
            connections = self._db.connections.all()

        is_prefork = self._is_prefork()

        for conn in connections:
            try:
                conn.close()
                pool_enabled = self._settings.DATABASES.get(conn.alias, {}).get("OPTIONS", {}).get("pool")
                if pool_enabled and is_prefork and hasattr(conn, "close_pool"):
                    with contextlib.suppress(KeyError):
                        conn.close_pool()
            except self.interface_errors:
                pass
            except self.DatabaseError as exc:
                str_exc = str(exc)
                if 'closed' not in str_exc and 'not connected' not in str_exc:
                    raise

    def close_cache(self) -> None:
        if _in_event_loop():
            logger.debug('Django fixup: not closing caches '
                         'in a thread with a running event loop.')
            return
        try:
            self._cache.close_caches()
        except (TypeError, AttributeError):
            pass
