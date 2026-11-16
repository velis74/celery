"""Prefork pool with child processes for ``async def`` tasks.

Used by the worker when :setting:`worker_async_concurrency` is set.
Tasks whose body is a coroutine function run in the async part
(:class:`~celery.concurrency.aiopool.AsyncAsynPool`), all others in the
prefork part, as with :class:`celery.concurrency.prefork.TaskPool`.
"""
import math

from kombu.asynchronous.semaphore import LaxBoundedSemaphore

from celery.app.trace import is_async_task
from celery.utils.functional import noop

from . import prefork
from .aiopool import AsyncAsynPool
from .prefork import process_destructor, process_initializer

__all__ = ('TaskPool', 'TaskTypeSemaphore')


class TaskTypeSemaphore:
    """Gates each request with the semaphore of the pool part that runs it.

    Each part releases its own semaphore when its jobs are ready.
    """

    def __init__(self, is_async, sync_semaphore, async_semaphore):
        self.is_async = is_async
        self.sync_semaphore = sync_semaphore
        self.async_semaphore = async_semaphore
        # Semaphore of the request whose callback is running.
        self._running = None

    def acquire(self, callback, request, *args, **kwargs):
        # The registered task name: ``request.name`` may be a shadow name.
        semaphore = (self.async_semaphore if self.is_async(request.type)
                     else self.sync_semaphore)
        return semaphore.acquire(self._run, semaphore, callback, request,
                                 *args, **kwargs)

    def _run(self, semaphore, callback, *args, **kwargs):
        previous, self._running = self._running, semaphore
        try:
            return callback(*args, **kwargs)
        finally:
            self._running = previous

    def release(self):
        """Give back the token of the request whose callback is running."""
        (self._running or self.sync_semaphore).release()

    def clear(self):
        self.sync_semaphore.clear()
        self.async_semaphore.clear()

    def __repr__(self):
        return f'<{type(self).__name__}: {self.sync_semaphore!r} {self.async_semaphore!r}>'


class TaskPool(prefork.TaskPool):
    """Prefork pool that runs ``async def`` tasks in asyncio processes.

    Arguments:
        limit (int): Number of prefork processes.
        async_concurrency (int): Number of async tasks running at the same
            time.
        async_processes (int): Number of processes sharing the async
            concurrency.
    """

    AsyncPool = AsyncAsynPool

    supports_async_concurrency = True

    _async = None

    def __init__(self, *args, async_concurrency=1, async_processes=1,
                 **kwargs):
        super().__init__(*args, **kwargs)
        if async_concurrency < 1 or async_processes < 1:
            raise ValueError(
                'async_concurrency and async_processes must be at least 1')
        self.async_concurrency = async_concurrency
        self.async_processes = min(async_processes, async_concurrency)
        self.async_capacity = math.ceil(
            async_concurrency / self.async_processes)
        self._is_async_name = {}
        self._async_semaphore = None
        sync_semaphore = self.options.get('semaphore')
        if sync_semaphore is not None:
            self._async_semaphore = LaxBoundedSemaphore(async_concurrency)
            self.task_semaphore = TaskTypeSemaphore(
                self.is_async_task_name, sync_semaphore,
                self._async_semaphore,
            )

    def is_async_task_name(self, name):
        """Return true if the task ``name`` runs in the async part."""
        try:
            return self._is_async_name[name]
        except KeyError:
            pass
        except TypeError:  # not hashable, so not a task name
            return False
        task = self.app.tasks.get(name) if self.app is not None else None
        is_async = task is not None and is_async_task(task)
        self._is_async_name[name] = is_async
        return is_async

    def on_start(self):
        super().on_start()
        conf = self.app.conf if self.app is not None else None
        options = dict(self.options)
        options.update(
            semaphore=self._async_semaphore,
            sched_strategy=None,
        )
        self._async = self.AsyncPool(
            processes=self.async_processes,
            capacity=self.async_capacity,
            block_warning=conf.worker_async_block_warning if conf else 1.0,
            initializer=process_initializer,
            on_process_exit=process_destructor,
            enable_timeouts=True,
            synack=False,
            proc_alive_timeout=conf.worker_proc_alive_timeout if conf else None,
            **options
        )
        self.on_apply = self._apply
        self.maintain_pool = self._maintain_pools
        self.terminate_job = self._terminate_job
        self.flush = self._flush

    def _apply(self, target, args, kwargs, **options):
        part = self._pool
        if args and self.is_async_task_name(args[0]):
            part = self._async
        return part.apply_async(target, args, kwargs, **options)

    def _maintain_pools(self):
        self._pool.maintain_pool()
        self._async.maintain_pool()

    def _terminate_job(self, pid, signal=None):
        if self._async is not None and self._async.owns_pid(pid):
            return self._async.terminate_job(pid, signal)
        return self._pool.terminate_job(pid, signal)

    def _flush(self):
        self._pool.flush()
        self._async.flush()

    def restart(self):
        self._pool.restart()
        self._async.restart()
        self._pool.apply_async(noop)

    def did_start_ok(self):
        return self._pool.did_start_ok() and self._async.did_start_ok()

    def register_with_event_loop(self, loop):
        self._pool.register_with_event_loop(loop)
        sync_writes = loop.consolidate_callback
        self._async.register_with_event_loop(loop)
        async_writes = loop.consolidate_callback
        async_inqueues = self._async._fileno_to_inq
        order_async_fds = self._async.order_ready_fds

        # The hub has one consolidated write callback: give each part
        # the writable inqueues of its own processes.
        def schedule_writes(ready_fds):
            sync_fds, async_fds = [], []
            for fd in ready_fds:
                (async_fds if fd in async_inqueues else sync_fds).append(fd)
            if async_fds:
                async_writes(order_async_fds(async_fds))
            if sync_fds:
                sync_writes(sync_fds)
        loop.consolidate_callback = schedule_writes

    def _close_pools(self):
        self._pool.close()
        if self._async is not None:
            self._async.close()

    def _join_pools(self):
        self._pool.join()
        if self._async is not None:
            self._async.join()

    def on_stop(self):
        super().on_stop()
        self._async = None

    def on_terminate(self):
        if self._async is not None:
            self._async.terminate()
            self._async = None
        super().on_terminate()

    def on_close(self):
        super().on_close()
        if self._async is not None and self._async._state == prefork.RUN:
            self._async.close()

    def _get_info(self):
        info = super()._get_info()
        info.update({
            'async-concurrency': self.async_concurrency,
            'async-processes': (self._async.process_info()
                                if self._async is not None else []),
        })
        return info

    @property
    def num_processes(self):
        return self._pool._processes + self.async_concurrency
