"""The task and request a task body sees, wherever the body runs.

Scenarios through the public API: ``current_task``, ``Task.request`` and
``app.current_worker_task``, for eager :meth:`~celery.app.task.Task.apply`,
direct calls, the tracer, threads, :mod:`asyncio` and threads started from
a task.  Real pools are covered by :mod:`t.integration.test_context_stack`.
"""
import asyncio
import subprocess
import sys
import textwrap
import threading
from pathlib import Path
from unittest.mock import patch

import pytest

from celery import _state, current_task
from celery.app.trace import build_tracer
from celery.utils.threads import LocalStack

ROOT = Path(__file__).resolve().parents[3]
INHERIT_CONTEXT = bool(getattr(sys.flags, 'thread_inherit_context', 0))


def name(task):
    return None if task is None else task.name


def seen():
    """Return the current task's name and request id, as task code sees them."""
    task = current_task._get_current_object()
    return name(task), (task.request.id if task is not None else None)


@pytest.fixture
def tasks(app):
    """Bound tasks reporting what they see."""
    class Tasks:
        pass

    t = Tasks()

    @app.task(bind=True, shared=False)
    def whoami(self):
        return seen(), self.request.id, name(app.current_worker_task)

    @app.task(bind=True, shared=False)
    def other(self):
        return seen(), self.request.id

    @app.task(bind=True, shared=False)
    def nested(self):
        before = seen(), name(app.current_worker_task)
        inner = whoami.apply().get()
        direct = whoami()
        after = seen(), name(app.current_worker_task)
        return before, inner, direct, after

    @app.task(bind=True, shared=False)
    def fails(self):
        raise KeyError(self.request.id)

    @app.task(bind=True, shared=False)
    def waits(self, barrier):
        before = seen(), self.request.id
        barrier.wait(10)
        return before, (seen(), self.request.id)

    t.whoami, t.other, t.nested, t.fails, t.waits = whoami, other, nested, fails, waits
    return t


def assert_no_task(*task_classes):
    assert current_task._get_current_object() is None
    assert _state.get_current_worker_task() is None
    for task in task_classes:
        assert task.request.id is None


class test_eager_apply_and_direct_call:
    """Scenario 1, in process: ``apply()``, ``__call__``, nested calls."""

    def test_apply_sees_its_own_task_and_request(self, app, tasks):
        res = tasks.whoami.apply(task_id='id-1')
        (task, current_id), request_id, worker_task = res.get()
        assert task == tasks.whoami.name
        assert current_id == request_id == 'id-1'
        assert worker_task == tasks.whoami.name

    def test_no_task_after_apply(self, app, tasks):
        tasks.whoami.apply(task_id='id-1')
        assert_no_task(tasks.whoami)

    def test_no_task_after_failing_apply(self, app, tasks):
        res = tasks.fails.apply(task_id='id-1')
        assert isinstance(res.result, KeyError)
        assert_no_task(tasks.fails)

    def test_direct_call_sees_task_and_is_not_a_worker_task(self, app, tasks):
        (task, _), request_id, worker_task = tasks.whoami()
        assert task == tasks.whoami.name
        assert request_id is None
        assert worker_task is None
        assert_no_task(tasks.whoami)

    def test_nested_apply_and_call_see_the_inner_task_then_the_outer_again(self, app, tasks):
        before, inner, direct, after = tasks.nested.apply(task_id='outer').get()
        assert before == ((tasks.nested.name, 'outer'), tasks.nested.name)
        (inner_task, inner_id), inner_request_id, inner_worker_task = inner
        assert inner_task == tasks.whoami.name
        assert inner_id == inner_request_id != 'outer'
        assert inner_worker_task == tasks.whoami.name
        (direct_task, _), direct_request_id, direct_worker_task = direct
        assert direct_task == tasks.whoami.name
        assert direct_request_id is None
        assert direct_worker_task == tasks.nested.name
        assert after == before
        assert_no_task(tasks.nested, tasks.whoami)

    def test_tracer_sees_its_own_task_and_leaves_none(self, app, tasks):
        tracer = build_tracer(tasks.whoami.name, tasks.whoami, app=app, eager=True)
        info = tracer('id-traced', (), {}, {'id': 'id-traced'})
        (task, current_id), request_id, worker_task = info.retval
        assert task == tasks.whoami.name
        assert current_id == request_id == 'id-traced'
        assert worker_task == tasks.whoami.name
        assert_no_task(tasks.whoami)


class test_threads:
    """Scenario 2, in process: concurrent ``apply()`` in several threads."""

    def test_concurrent_apply_in_threads_sees_own_request(self, app, tasks):
        n = 4
        barrier = threading.Barrier(n)
        results = {}

        def run(i):
            results[i] = tasks.waits.apply((barrier,), task_id=f'id-{i}').get()

        threads = [threading.Thread(target=run, args=(i,)) for i in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(30)
        for i in range(n):
            mine = ((tasks.waits.name, f'id-{i}'), f'id-{i}')
            assert results[i] == (mine, mine)
        assert_no_task(tasks.waits)


class test_asyncio:
    """Scenario 4: :mod:`asyncio` inside a sync task, and async tasks."""

    def test_coroutines_gathered_in_a_sync_task_see_the_task(self, app):
        @app.task(bind=True, shared=False)
        def gathers(self):
            async def read(i):
                await asyncio.sleep(0.001 * (3 - i))
                return seen(), self.request.id

            async def main():
                return await asyncio.gather(*(read(i) for i in range(3)))

            return asyncio.run(main())

        res = gathers.apply(task_id='id-g').get()
        assert res == [((gathers.name, 'id-g'), 'id-g')] * 3
        assert_no_task(gathers)

    def test_async_tasks_awaited_concurrently_see_their_own_request(self, app):
        @app.task(bind=True, shared=False)
        async def first(self, n):
            await asyncio.sleep(0.001 * (5 - n))
            task, _ = seen()
            return n, task, self.request.args

        @app.task(bind=True, shared=False)
        async def second(self, n):
            await asyncio.sleep(0.001 * n)
            task, _ = seen()
            return n, task, self.request.args

        async def main():
            return await asyncio.gather(
                *(first(n) for n in range(5)), *(second(n) for n in range(5)))

        results = asyncio.run(main())
        assert results == [(n, first.name, (n,)) for n in range(5)] + [(n, second.name, (n,)) for n in range(5)]
        assert_no_task(first, second)


class test_threads_started_by_a_task:
    """Scenario 5: threads and executors a task body starts."""

    @pytest.mark.skipif(INHERIT_CONTEXT, reason='threads inherit the context')
    def test_thread_started_in_task_sees_no_task(self, app):
        @app.task(bind=True, shared=False)
        def starts_thread(self):
            out = []
            t = threading.Thread(target=lambda: out.append(seen()))
            t.start()
            t.join(10)
            return out

        assert starts_thread.apply(task_id='id-t').get() == [(None, None)]

    @pytest.mark.skipif(sys.version_info < (3, 14), reason='thread_inherit_context is new in 3.14')
    def test_thread_started_in_task_sees_the_task_at_start_with_inherit_context(self):
        script = textwrap.dedent('''
            import threading
            from celery import Celery, current_task

            app = Celery(set_as_current=False)

            def seen():
                task = current_task._get_current_object()
                return None if task is None else (task.name, task.request.id)

            @app.task(bind=True, name='starts_thread')
            def starts_thread(self):
                started, finished = threading.Event(), threading.Event()
                out = []

                def target():
                    out.append(seen())
                    started.set()
                    finished.wait(10)
                    out.append(seen())

                t = threading.Thread(target=target)
                t.start()
                started.wait(10)
                return t, finished, out

            t, finished, out = starts_thread.apply(task_id='id-t').get()
            finished.set()
            t.join(10)
            print(repr(out))
        ''')
        proc = subprocess.run(
            [sys.executable, '-X', 'thread_inherit_context=1', '-c', script],
            cwd=ROOT, capture_output=True, text=True, timeout=60)
        assert proc.returncode == 0, proc.stderr
        assert proc.stdout.strip() == repr([('starts_thread', 'id-t')] * 2)

    @pytest.mark.skipif(sys.version_info < (3, 14), reason='thread_inherit_context is new in 3.14')
    def test_reused_executor_thread_sees_the_task_that_started_it_with_inherit_context(self):
        script = textwrap.dedent('''
            from concurrent.futures import ThreadPoolExecutor
            from celery import Celery, current_task

            app = Celery(set_as_current=False)
            pool = ThreadPoolExecutor(1)

            def seen():
                task = current_task._get_current_object()
                return None if task is None else (task.name, task.request.id)

            @app.task(bind=True, name='submits')
            def submits(self):
                return pool.submit(seen).result(10)

            first = submits.apply(task_id='id-a').get()
            second = submits.apply(task_id='id-b').get()
            pool.shutdown()
            print(repr([first, second]))
        ''')
        proc = subprocess.run(
            [sys.executable, '-X', 'thread_inherit_context=1', '-c', script],
            cwd=ROOT, capture_output=True, text=True, timeout=60)
        assert proc.returncode == 0, proc.stderr
        assert proc.stdout.strip() == repr([('submits', 'id-a')] * 2)

    def test_asyncio_to_thread_sees_the_calling_task(self, app):
        @app.task(bind=True, shared=False)
        def to_thread(self):
            async def main():
                return await asyncio.to_thread(seen)
            return asyncio.run(main())

        assert to_thread.apply(task_id='id-t').get() == (to_thread.name, 'id-t')

    @pytest.mark.skipif(INHERIT_CONTEXT, reason='threads inherit the context')
    def test_run_in_executor_sees_no_task(self, app):
        @app.task(bind=True, shared=False)
        def in_executor(self):
            async def main():
                return await asyncio.get_running_loop().run_in_executor(None, seen)
            return asyncio.run(main())

        assert in_executor.apply(task_id='id-t').get() == (None, None)


class test_push_and_pop_pair_up:
    """Scenario 6: Celery pops every stack entry in the context that pushed it.

    Each pop must see on top the object the matching push added, and leave
    the stack as it was before that push.
    """

    @pytest.fixture
    def recorded(self, tasks):
        pairs = []
        stacks = [_state._task_stack] + [
            t.request_stack for t in (tasks.whoami, tasks.nested, tasks.fails)]
        patches = []
        for stack in stacks:
            pending = []
            push, pop = stack.push, stack.pop

            def recording_push(obj, stack=stack, push=push, pending=pending):
                before = list(stack.stack)
                result = push(obj)
                pending.append((obj, before))
                return result

            def recording_pop(stack=stack, pop=pop, pending=pending):
                obj, before = pending.pop()
                top = stack.top
                result = pop()
                pairs.append((top is obj, list(stack.stack) == before))
                return result

            patches.append(patch.object(stack, 'push', recording_push))
            patches.append(patch.object(stack, 'pop', recording_pop))
        for p in patches:
            p.start()
        yield pairs, stacks
        for p in reversed(patches):
            p.stop()

    def check(self, recorded, expected_pairs):
        pairs, stacks = recorded
        assert pairs == [(True, True)] * expected_pairs
        assert [len(s) for s in stacks] == [0] * len(stacks)

    def test_eager_apply(self, recorded, tasks):
        tasks.whoami.apply()
        self.check(recorded, 2)

    def test_failing_eager_apply(self, recorded, tasks):
        tasks.fails.apply()
        self.check(recorded, 2)

    def test_direct_call(self, recorded, tasks):
        tasks.whoami()
        self.check(recorded, 2)

    def test_nested_apply_and_call(self, recorded, tasks):
        tasks.nested.apply()
        self.check(recorded, 6)

    def test_tracer(self, recorded, app, tasks):
        tracer = build_tracer(tasks.whoami.name, tasks.whoami, app=app)
        tracer('id-traced', (), {}, {'id': 'id-traced'})
        self.check(recorded, 2)

    def test_apply_inside_an_asyncio_task(self, recorded, tasks):
        async def main():
            tasks.nested.apply()
            return seen()

        assert asyncio.run(main()) == (None, None)
        self.check(recorded, 6)


def test_stacks_are_local_stacks(tasks):
    assert isinstance(_state._task_stack, LocalStack)
    assert isinstance(tasks.whoami.request_stack, LocalStack)
