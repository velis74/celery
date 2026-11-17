"""Pool implementation abstract factory, and alias definitions."""
import os

# Import from kombu directly as it's used
# early in the import stage, where celery.utils loads
# too much (e.g., for eventlet patching)
from kombu.utils.imports import symbol_by_name

__all__ = (
    'get_implementation', 'get_available_pool_names',
    'current_process_kind', 'current_process_capacity',
)

#: :func:`current_process_kind` of a prefork pool process.
PREFORK = 'prefork'

#: :func:`current_process_kind` of an async pool process.
ASYNC = 'async'

# Kind and task capacity of this process, set by the pool process initializer.
_process_kind = None
_process_capacity = None

ALIASES = {
    'prefork': 'celery.concurrency.prefork:TaskPool',
    'eventlet': 'celery.concurrency.eventlet:TaskPool',
    'gevent': 'celery.concurrency.gevent:TaskPool',
    'solo': 'celery.concurrency.solo:TaskPool',
    'processes': 'celery.concurrency.prefork:TaskPool',  # XXX compat alias
}

try:
    import concurrent.futures  # noqa
except ImportError:
    pass
else:
    ALIASES['threads'] = 'celery.concurrency.thread:TaskPool'
#
# Allow for an out-of-tree worker pool implementation. This is used as follows:
#
#   - Set the environment variable CELERY_CUSTOM_WORKER_POOL to the name of
#     an implementation of :class:`celery.concurrency.base.BasePool` in the
#     standard Celery format of "package:class".
#   - Select this pool using '--pool custom'.
#
try:
    custom = os.environ.get('CELERY_CUSTOM_WORKER_POOL')
except KeyError:
    pass
else:
    ALIASES['custom'] = custom


def get_implementation(cls):
    """Return pool implementation by name."""
    return symbol_by_name(cls, ALIASES)


def get_available_pool_names():
    """Return all available pool type names."""
    return tuple(ALIASES.keys())


def current_process_kind():
    """Return the kind of worker pool process this code runs in.

    ``'prefork'`` in a prefork pool process, ``'async'`` in an async pool
    process (:setting:`worker_async_concurrency`), and :const:`None` in any
    other process: the main worker process, the solo, threads, eventlet
    and gevent pools, clients, and processes forked from a pool process.
    The value is set before :signal:`worker_process_init` is sent.  Task
    modules are imported in the main worker process, where the value is
    :const:`None`.
    """
    return _process_kind


def current_process_capacity():
    """Return the number of tasks this worker pool process runs at the same time.

    ``1`` in a prefork pool process,
    ``ceil(worker_async_concurrency / worker_async_processes)`` in an async
    pool process, and :const:`None` where :func:`current_process_kind`
    returns :const:`None`.
    """
    return _process_capacity


def _set_process_kind(kind, capacity):
    global _process_kind, _process_capacity
    _process_kind, _process_capacity = kind, capacity


def _reset_process_kind():
    _set_process_kind(None, None)


if hasattr(os, 'register_at_fork'):
    os.register_at_fork(after_in_child=_reset_process_kind)
