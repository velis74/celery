import asyncio
import functools
import inspect
from unittest.mock import Mock

import pytest

from celery import states
from celery.app.task import Task
from celery.app.trace import (build_tracer, fast_trace_task, get_task_awaiter, is_async_task,
                              reset_worker_optimizations, set_task_awaiter, setup_worker_optimizations)
from celery.exceptions import Ignore, Reject, Retry


def drive(awaitable):
    """Run an awaitable that only yields bare ``None`` in the calling thread.

    Unlike :func:`asyncio.run` it can be nested, which eager retries need.
    """
    gen = awaitable.__await__()
    try:
        while True:
            gen.send(None)
    except StopIteration as exc:
        return exc.value


def run_sync(fun):
    """Sync decorator that runs the decorated ``async def`` function itself."""
    @functools.wraps(fun)
    def inner(*args, **kwargs):
        return drive(fun(*args, **kwargs))
    return inner


@pytest.fixture
def awaiter():
    calls = []

    def _awaiter(aw):
        calls.append(aw)
        return drive(aw)

    _awaiter.calls = calls
    set_task_awaiter(_awaiter)
    try:
        yield _awaiter
    finally:
        set_task_awaiter(None)


def trace(app, task, args=(), kwargs=None, request=None, task_id='id-1', **opts):
    opts.setdefault('eager', True)
    tracer = build_tracer(task.name, task, app=app, **opts)
    if request is None:
        request = {'id': task_id}
    return tracer(task_id, args, kwargs or {}, request)


class test_task_awaiter:

    def test_default_is_none(self):
        assert get_task_awaiter() is None

    def test_set_and_get(self):
        fun = Mock(name='awaiter')
        set_task_awaiter(fun)
        try:
            assert get_task_awaiter() is fun
        finally:
            set_task_awaiter(None)
        assert get_task_awaiter() is None


class test_is_async_task:

    def test_sync(self):
        @self.app.task(shared=False)
        def add(x, y):
            return x + y

        assert not is_async_task(add)

    def test_async(self):
        @self.app.task(shared=False)
        async def add(x, y):
            return x + y

        assert is_async_task(add)

    def test_async_bound(self):
        @self.app.task(shared=False, bind=True)
        async def add(self, x, y):
            return x + y

        assert is_async_task(add)

    def test_async_behind_sync_decorator(self):
        def deco(fun):
            @functools.wraps(fun)
            def inner(*args, **kwargs):
                return fun(*args, **kwargs)
            return inner

        @self.app.task(shared=False)
        @deco
        async def add(x, y):
            return x + y

        assert is_async_task(add)

    def test_class_based_async_run(self):
        class AddTask(Task):
            name = 'test_class_based_async_run'

            async def run(self, x, y):
                return x + y

        assert is_async_task(self.app.register_task(AddTask()))

    def test_custom_async_call(self):
        class AsyncCallTask(Task):
            name = 'test_custom_async_call'

            async def __call__(self, *args, **kwargs):
                return self.run(*args, **kwargs)

            def run(self, x, y):
                return x + y

        assert is_async_task(self.app.register_task(AsyncCallTask()))


class test_trace_async:

    def setup_method(self):
        @self.app.task(shared=False, bind=True)
        async def ok(self, x, y):
            await asyncio.sleep(0)
            return x + y, self.request.id

        self.ok = ok

        @self.app.task(shared=False)
        async def raises(exc):
            await asyncio.sleep(0)
            raise exc

        self.raises = raises

        @self.app.task(shared=False)
        def add(x, y):
            return x + y

        self.add = add

    def test_success(self, awaiter):
        ret = trace(self.app, self.ok, (2, 2), task_id='id-ok')
        assert ret.retval == (4, 'id-ok')
        assert ret.info is None
        assert len(awaiter.calls) == 1

    def test_success_stores_result(self, awaiter):
        self.ok.backend = Mock(name='backend')
        trace(self.app, self.ok, (2, 2), task_id='id-ok', eager=False,
              request={'id': 'id-ok'})
        self.ok.backend.mark_as_done.assert_called_once()
        assert self.ok.backend.mark_as_done.call_args[0][:2] == ('id-ok', (4, 'id-ok'))

    def test_with_asyncio_run(self):
        set_task_awaiter(asyncio.run)
        try:
            ret = trace(self.app, self.ok, (1, 2), task_id='id-run')
        finally:
            set_task_awaiter(None)
        assert ret.retval == (3, 'id-run')

    def test_failure(self, awaiter):
        ret = trace(self.app, self.raises, (KeyError('foo'),))
        assert ret.info.state == states.FAILURE
        assert isinstance(ret.info.retval, KeyError)

    def test_failure_propagates(self, awaiter):
        with pytest.raises(KeyError):
            trace(self.app, self.raises, (KeyError('foo'),), propagate=True)

    @pytest.mark.parametrize('exc,state', [
        (Retry('retry', when=10), states.RETRY),
        (Reject('rejected'), states.REJECTED),
        (Ignore(), states.IGNORED),
    ])
    def test_control_exceptions(self, awaiter, exc, state):
        ret = trace(self.app, self.raises, (exc,))
        assert ret.info.state == state

    def test_tracer_built_before_awaiter_is_set(self):
        tracer = build_tracer(self.ok.name, self.ok, eager=True, app=self.app)
        set_task_awaiter(drive)
        try:
            ret = tracer('id-late', (1, 1), {}, {'id': 'id-late'})
        finally:
            set_task_awaiter(None)
        assert ret.retval == (2, 'id-late')

    def test_sync_task_does_not_call_awaiter(self, awaiter):
        ret = trace(self.app, self.add, (2, 2))
        assert ret.retval == 4
        assert awaiter.calls == []

    def test_non_awaitable_return_is_not_awaited(self, awaiter):
        @self.app.task(shared=False)
        @run_sync
        async def resolved(x):
            return x

        ret = trace(self.app, resolved, (5,))
        assert ret.retval == 5
        assert awaiter.calls == []

    def test_without_awaiter_returns_awaitable(self):
        ret = trace(self.app, self.ok, (2, 2))
        try:
            assert inspect.iscoroutine(ret.retval)
            assert ret.info is None
        finally:
            ret.retval.close()

    def test_apply(self, awaiter):
        res = self.ok.apply((2, 3), task_id='id-apply')
        assert res.state == states.SUCCESS
        assert res.get() == (5, 'id-apply')

    def test_apply_failure(self, awaiter):
        res = self.raises.apply((KeyError('foo'),))
        assert res.state == states.FAILURE
        assert isinstance(res.result, KeyError)

    def test_fast_trace_task(self, awaiter):
        self.app.tasks[self.ok.name].__trace__ = build_tracer(
            self.ok.name, self.ok, app=self.app,
        )
        self.ok.backend = Mock(name='backend')
        failed, retstr, _ = fast_trace_task(
            self.ok.name, 'id-fast', {'id': 'id-fast'}, ((2, 2), {}, {}), None, None,
            _loc=[self.app.tasks, {}, 'hostname'],
        )
        assert failed == 0
        assert retstr == repr((4, 'id-fast'))

    def test_custom_call_with_stack_protection(self, awaiter):
        class CustomCallTask(Task):
            def __call__(self, *args, **kwargs):
                return super().__call__(*args, **kwargs)

        @self.app.task(shared=False, bind=True, base=CustomCallTask)
        async def custom(self):
            await asyncio.sleep(0)
            return self.request.id, len(self.request_stack.stack)

        setup_worker_optimizations(self.app)
        try:
            ret = trace(self.app, custom, task_id='id-custom', eager=False,
                        request={'id': 'id-custom'})
        finally:
            reset_worker_optimizations(self.app)
        assert ret.retval == ('id-custom', 1)


class test_task_call_async:

    def setup_method(self):
        @self.app.task(shared=False, bind=True)
        async def info(self, x):
            await asyncio.sleep(0)
            return (self.request.args, self.request.called_directly,
                    len(self.request_stack.stack))

        self.info = info

    def test_returns_coroutine(self):
        coro = self.info(1)
        try:
            assert inspect.iscoroutine(coro)
            assert self.info.request_stack.top is None
        finally:
            coro.close()

    def test_request_pushed_while_running(self):
        assert asyncio.run(self.info(1)) == ((1,), True, 1)
        assert self.info.request_stack.top is None

    def test_request_popped_on_error(self):
        @self.app.task(shared=False)
        async def fails():
            raise KeyError('foo')

        with pytest.raises(KeyError):
            asyncio.run(fails())
        assert fails.request_stack.top is None

    def test_awaited_from_async_task(self, awaiter):
        @self.app.task(shared=False, bind=True)
        async def outer(self):
            inner_info = await info(2)
            return self.request.id, inner_info

        info = self.info
        ret = trace(self.app, outer, task_id='id-outer')
        assert ret.retval == ('id-outer', ((2,), True, 1))

    def test_sync_task_unchanged(self):
        @self.app.task(shared=False, bind=True)
        def sync_info(self, x):
            return self.request.args

        assert sync_info(1) == (1,)

    def test_sync_decorator_over_async_returns_value(self):
        @self.app.task(shared=False, bind=True)
        @run_sync
        async def double(self, x):
            return x * 2, self.request.args

        assert double(2) == (4, (2,))
        assert double.request_stack.top is None


class test_autoretry_async:

    def test_wrapper_is_async(self):
        @self.app.task(shared=False, autoretry_for=(ZeroDivisionError,))
        async def div(a, b):
            return a / b

        assert inspect.iscoroutinefunction(div.run)
        assert inspect.iscoroutinefunction(div._orig_run)
        assert div.run.__wrapped__ is div._orig_run
        assert is_async_task(div)

    def test_sync_decorator_over_async(self):
        @self.app.task(shared=False, autoretry_for=(KeyError,))
        @run_sync
        async def triple(x):
            return x * 3

        assert not inspect.iscoroutinefunction(triple.run)
        assert triple(2) == 6
        assert triple.apply((2,)).get() == 6

    def test_sync_decorator_over_async_retries(self):
        @self.app.task(shared=False, bind=True, max_retries=2,
                       autoretry_for=(ZeroDivisionError,))
        @run_sync
        async def div(self, a, b):
            self.iterations += 1
            return a / b

        div.iterations = 0
        res = div.apply((1, 0))
        assert div.iterations == 3
        assert isinstance(res.result, ZeroDivisionError)

    def test_retries_on_exception_from_coroutine(self, awaiter):
        @self.app.task(shared=False, bind=True, max_retries=3,
                       autoretry_for=(ZeroDivisionError,))
        async def div(self, a, b):
            self.iterations += 1
            await asyncio.sleep(0)
            return a / b

        div.iterations = 0
        res = div.apply((1, 0))
        assert div.iterations == 4
        assert res.state == states.FAILURE
        assert isinstance(res.result, ZeroDivisionError)

    def test_success(self, awaiter):
        @self.app.task(shared=False, autoretry_for=(ZeroDivisionError,))
        async def div(a, b):
            return a / b

        assert div.apply((4, 2)).get() == 2

    @pytest.mark.parametrize('exc', [Ignore(), Retry('retry')])
    def test_control_exceptions_not_retried(self, exc):
        @self.app.task(shared=False, autoretry_for=(Exception,))
        async def raises():
            raise exc

        raises.retry = Mock(name='retry')
        with pytest.raises(type(exc)):
            asyncio.run(raises.run())
        raises.retry.assert_not_called()

    def test_dont_autoretry_for(self):
        @self.app.task(shared=False, autoretry_for=(Exception,),
                       dont_autoretry_for=(KeyError,))
        async def raises():
            raise KeyError('foo')

        raises.retry = Mock(name='retry')
        with pytest.raises(KeyError):
            asyncio.run(raises.run())
        raises.retry.assert_not_called()

    def test_retry_returned_is_raised(self):
        @self.app.task(shared=False, autoretry_for=(KeyError,))
        async def raises():
            raise KeyError('foo')

        retry = Retry('retry')
        raises.retry = Mock(name='retry', return_value=retry)
        with pytest.raises(Retry) as excinfo:
            asyncio.run(raises.run())
        assert excinfo.value is retry


class test_pydantic_async:

    def setup_method(self):
        pydantic = pytest.importorskip('pydantic')

        class Model(pydantic.BaseModel):
            value: int

        self.Model = Model

    def test_validates_and_dumps(self):
        Model = self.Model
        check = Mock()

        @self.app.task(shared=False, pydantic=True)
        async def double(arg: Model) -> Model:
            await asyncio.sleep(0)
            check(arg)
            return Model(value=arg.value * 2)

        assert inspect.iscoroutinefunction(double.run)
        assert is_async_task(double)
        assert asyncio.run(double.run({'value': 2})) == {'value': 4}
        check.assert_called_once_with(Model(value=2))

    def test_apply(self, awaiter):
        Model = self.Model

        @self.app.task(shared=False, pydantic=True)
        async def double(arg: Model) -> Model:
            return Model(value=arg.value * 2)

        assert double.apply(({'value': 3},)).get() == {'value': 6}

    def test_with_autoretry(self, awaiter):
        Model = self.Model

        @self.app.task(shared=False, bind=True, pydantic=True, max_retries=2,
                       autoretry_for=(ZeroDivisionError,))
        async def div(self, arg: Model) -> Model:
            self.iterations += 1
            return Model(value=1 // arg.value)

        div.iterations = 0
        res = div.apply(({'value': 0},))
        assert div.iterations == 3
        assert isinstance(res.result, ZeroDivisionError)
        assert div.apply(({'value': 1},)).get() == {'value': 1}

    def test_sync_decorator_over_async(self):
        Model = self.Model

        @self.app.task(shared=False, pydantic=True)
        @run_sync
        async def double(arg: Model) -> Model:
            return Model(value=arg.value * 2)

        assert not inspect.iscoroutinefunction(double.run)
        assert double({'value': 2}) == {'value': 4}
        assert double.apply(({'value': 3},)).get() == {'value': 6}
