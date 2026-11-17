#!/usr/bin/env python
"""Run the async worker demo.

Starts a worker subprocess for each scenario, sends tasks from
:mod:`tasks`, checks the outcome and prints a report.  Two modes:

* ``classic``: ``--concurrency`` only; sync tasks.
* ``async``: ``--concurrency`` and ``--aconcurrency``; sync and async tasks.

Usage::

    python run_demo.py [--mode classic|async|both] [--scenario NAME[,NAME]|all]

Each worker consumes its own queue and runs in its own process group, which
is killed when the script exits, also on Ctrl+C.
"""
import argparse
import atexit
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from collections import Counter
from pathlib import Path

import tasks
from kombu import Connection

HERE = Path(__file__).resolve().parent

MODES = {
    'classic': ['--concurrency', '8'],
    'async': ['--concurrency', '2', '--aconcurrency', '200', '--aprocesses', '2'],
}

WORKER_START_TIMEOUT = 60
WORKER_STOP_TIMEOUT = 30

_workers = []


class Worker:
    """A worker subprocess consuming its own queue."""

    def __init__(self, logdir, args):
        self.name = f'async-worker-demo-{uuid.uuid4().hex[:8]}'
        self.node = f'{self.name}@{socket.gethostname()}'
        self.queue = self.name
        self.log = Path(logdir) / f'{self.name}.log'
        self.args = list(args)
        self.proc = None
        _workers.append(self)

    def start(self):
        """Start the worker and wait until it answers a ping."""
        with open(self.log, 'ab') as log:
            self.proc = subprocess.Popen(
                [sys.executable, '-m', 'celery', '-A', 'tasks', 'worker',
                 '-n', self.node, '-Q', self.queue, '-l', 'INFO',
                 '--without-gossip', '--without-mingle', '--without-heartbeat',
                 *self.args],
                cwd=HERE, stdin=subprocess.DEVNULL, stdout=log,
                stderr=subprocess.STDOUT, start_new_session=True,
            )
        deadline = time.monotonic() + WORKER_START_TIMEOUT
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                break
            replies = tasks.app.control.ping(
                destination=[self.node], timeout=0.5) or []
            if any(self.node in reply for reply in replies):
                return self
            time.sleep(0.2)
        raise RuntimeError(f'Worker {self.node} did not start; see {self.log}')

    def send(self, task, *args, **options):
        return task.apply_async(args, queue=self.queue, **options)

    def read_log(self):
        return self.log.read_text(errors='replace')

    def log_lines(self, pattern):
        return [line for line in self.read_log().splitlines()
                if re.search(pattern, line)]

    def wait_for_log(self, pattern, timeout=10):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            lines = self.log_lines(pattern)
            if lines:
                return lines
            time.sleep(0.1)
        return []

    def alive(self):
        return self.proc is not None and self.proc.poll() is None

    def kill_group(self):
        """Kill the worker and all its child processes with SIGKILL."""
        if self.proc is None:
            return
        try:
            os.killpg(self.proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        self.proc.wait()

    def warm_shutdown(self, timeout=WORKER_STOP_TIMEOUT):
        """Send SIGTERM and wait for the exit; return seconds or None."""
        start = time.monotonic()
        self.proc.send_signal(signal.SIGTERM)
        try:
            self.proc.wait(timeout)
        except subprocess.TimeoutExpired:
            return None
        return time.monotonic() - start

    def stop(self):
        """Warm shutdown, then kill whatever is left of the process group."""
        if self.proc is not None:
            if self.proc.poll() is None:
                self.warm_shutdown()
            self.kill_group()
            self.proc = None
        self.delete_queue()

    def delete_queue(self):
        try:
            with Connection(tasks.BROKER) as conn:
                conn.default_channel.queue_delete(self.queue)
        except Exception:  # pylint: disable=broad-except
            pass


def cleanup():
    for worker in _workers:
        if worker.proc is not None:
            worker.kill_group()
            worker.proc = None
            worker.delete_queue()


def _on_signal(signum, frame):
    raise KeyboardInterrupt


# Helpers.

def wait_ready(results, timeout):
    """Wait until all results are ready; return the seconds it took."""
    start = time.monotonic()
    pending = list(results)
    while pending:
        pending = [r for r in pending if not r.ready()]
        if not pending:
            break
        if time.monotonic() - start > timeout:
            raise TimeoutError(f'{len(pending)} of {len(results)} tasks not ready '
                               f'after {timeout}s')
        time.sleep(0.05)
    return time.monotonic() - start


def wait_started(results, timeout=20):
    """Wait until all slow tasks report PROGRESS; return {pid: [result]}."""
    deadline = time.monotonic() + timeout
    while True:
        by_pid = {}
        for r in results:
            info = r.info if r.state == 'PROGRESS' else None
            if info:
                by_pid.setdefault(info['pid'], []).append(r)
        if sum(len(v) for v in by_pid.values()) == len(results):
            return by_pid
        if time.monotonic() > deadline:
            raise TimeoutError('tasks did not start')
        time.sleep(0.05)


def outcome(result):
    """Short text for the final state of a result."""
    if result.state == 'SUCCESS':
        return 'SUCCESS'
    exc = result.result
    if isinstance(exc, BaseException):
        return f'{result.state} {type(exc).__name__}'
    return result.state


def summarise(results):
    return ', '.join(f'{n} {state}' for state, n in
                     sorted(Counter(outcome(r) for r in results).items()))


def short(line, width=110):
    line = re.sub(r'^\[[^\]]*\]\s*', '', line.strip())
    return line if len(line) <= width else line[:width - 3] + '...'


class Context:
    def __init__(self, logdir, options):
        self.logdir = logdir
        self.options = options
        self.markers = Path(logdir) / 'markers'
        self.markers.mkdir(exist_ok=True)
        self.throughput = {}

    def worker(self, mode, *extra):
        return Worker(self.logdir, MODES[mode] + list(extra)).start()


def pick(mode, sync_task, async_task):
    return async_task if mode == 'async' else sync_task


# Scenarios.  Each returns a list of (check, ok, detail); ok None means n/a.

def scenario_basics(ctx, mode):
    w = ctx.worker(mode)
    rows = []
    try:
        count = 50 if mode == 'async' else 16
        echo = pick(mode, tasks.echo_id, tasks.aecho_id)
        results = [w.send(echo, 0.3) for _ in range(count)]
        elapsed = wait_ready(results, 60)
        own = sum(r.result == r.id for r in results)
        rows.append(('request id', own == count,
                     f'{own}/{count} tasks returned their own request id '
                     f'({elapsed:.2f}s)'))

        r = w.send(pick(mode, tasks.fail, tasks.afail))
        wait_ready([r], 30)
        rows.append(('failure', r.state == 'FAILURE' and isinstance(r.result, ValueError),
                     f'{r.state} {r.result!r}'))

        r = w.send(pick(mode, tasks.retry_once, tasks.aretry_once))
        wait_ready([r], 30)
        rows.append(('retry', r.state == 'SUCCESS' and r.result == 1,
                     f'{r.state}, retries={r.result}'))

        if mode == 'async':
            r = w.send(tasks.anested)
            wait_ready([r], 30)
            ok = r.successful() and r.result[0] == r.result[2] == r.id
            rows.append(('await another task', ok,
                         f'{r.state}; request id before/after the await is the own id: '
                         f'{ok}; inner request id: {r.result[1] if r.successful() else "-"}'))

            cpu = [w.send(tasks.cpu_work, 2_000_000) for _ in range(4)]
            aio = [w.send(tasks.aio_sleep, 0.5) for _ in range(20)]
            elapsed = wait_ready(cpu + aio, 60)
            cpu_pids = {r.result[1] for r in cpu}
            aio_pids = {r.result for r in aio}
            rows.append(('routing', not cpu_pids & aio_pids and all(r.successful() for r in cpu + aio),
                         f'4 sync in pids {sorted(cpu_pids)}, 20 async in pids '
                         f'{sorted(aio_pids)} ({elapsed:.2f}s)'))
    finally:
        w.stop()
    return rows


def scenario_throughput(ctx, mode):
    w = ctx.worker(mode)
    n, seconds = ctx.options.io_tasks, ctx.options.io_seconds
    try:
        task = pick(mode, tasks.io_sleep, tasks.aio_sleep)
        start = time.monotonic()
        results = [w.send(task, seconds) for _ in range(n)]
        wait_ready(results, 600)
        elapsed = time.monotonic() - start
        ok = sum(r.successful() for r in results)
        pids = len({r.result for r in results if r.successful()})
    finally:
        w.stop()
    ctx.throughput[mode] = elapsed
    slots = 'concurrency 8' if mode == 'classic' else 'aconcurrency 200'
    return [('wall time', ok == n,
             f'{n} x sleep({seconds}s), {slots}: {elapsed:.2f}s '
             f'({n / elapsed:.0f} tasks/s), {ok}/{n} SUCCESS in {pids} process(es)')]


def scenario_soft_limit(ctx, mode):
    w = ctx.worker(mode)
    rows = []
    try:
        start = time.monotonic()
        r = w.send(pick(mode, tasks.soft_limited, tasks.asoft_limited), 3)
        others = [w.send(pick(mode, tasks.io_sleep, tasks.aio_sleep), 1)
                  for _ in range(3)]
        wait_ready([r], 30)
        elapsed = time.monotonic() - start
        wait_ready(others, 30)
        rows.append(('soft_time_limit=0.5', outcome(r) == 'FAILURE SoftTimeLimitExceeded',
                     f'{outcome(r)} after {elapsed:.2f}s; 3 other tasks: {summarise(others)}'))
        if mode == 'async':
            r = w.send(tasks.asoft_cleanup, 3)
            wait_ready([r], 30)
            rows.append(('coroutine catches it', r.state == 'SUCCESS' and r.result == 'cleaned up',
                         f'{r.state} {r.result!r}'))
    finally:
        w.stop()
    return rows


def scenario_hard_limit(ctx, mode):
    w = ctx.worker(mode)
    rows = []
    try:
        start = time.monotonic()
        r = w.send(pick(mode, tasks.hard_limited, tasks.ahard_limited), 5)
        others = [w.send(pick(mode, tasks.io_sleep, tasks.aio_sleep), 2)
                  for _ in range(6 if mode == 'async' else 3)]
        wait_ready([r], 30)
        elapsed = time.monotonic() - start
        wait_ready(others, 30)
        ok = (outcome(r) == 'FAILURE TimeLimitExceeded'
              and all(o.successful() for o in others))
        rows.append(('time_limit=1', ok,
                     f'{outcome(r)} after {elapsed:.2f}s; {len(others)} other tasks: '
                     f'{summarise(others)}'))
        pattern = 'Recycling async pool process' if mode == 'async' else 'Hard time limit'
        lines = w.wait_for_log(pattern, 5)
        rows.append(('log', bool(lines), short(lines[0]) if lines else 'no log line'))
        # The pool still runs tasks at full capacity afterwards.
        after = [w.send(pick(mode, tasks.io_sleep, tasks.aio_sleep), 0.5)
                 for _ in range(8)]
        elapsed = wait_ready(after, 30)
        rows.append(('capacity after', all(a.successful() for a in after),
                     f'8 x sleep(0.5s) took {elapsed:.2f}s'))
    finally:
        w.stop()
    return rows


def scenario_revoke(ctx, mode):
    w = ctx.worker(mode)
    try:
        task = pick(mode, tasks.slow, tasks.aslow)
        results = [w.send(task, 3, str(ctx.markers)) for _ in range(6 if mode == 'async' else 3)]
        by_pid = wait_started(results)
        target = results[0]
        start = time.monotonic()
        tasks.app.control.revoke(target.id, terminate=True)
        wait_ready([target], 30)
        elapsed = time.monotonic() - start
        others = results[1:]
        wait_ready(others, 30)
        same = [r for r in others if r in by_pid[target_pid(by_pid, target)]]
    finally:
        w.stop()
    ok = target.state == 'REVOKED' and all(r.successful() for r in others)
    return [('revoke(terminate=True)', ok,
             f'{target.state} {elapsed:.2f}s after the revoke; {len(others)} other tasks '
             f'({len(same)} in the same process): {summarise(others)}')]


def target_pid(by_pid, result):
    return next(pid for pid, rs in by_pid.items() if result in rs)


def scenario_watchdog(ctx, mode):
    if mode != 'async':
        return [('loop watchdog', None, 'async tasks only')]
    w = ctx.worker(mode)
    try:
        r = w.send(tasks.ablock_loop, 1.5)
        time.sleep(0.2)
        others = [w.send(tasks.aio_sleep, 0.2) for _ in range(3)]
        wait_ready([r] + others, 30)
        lines = w.wait_for_log('has not run for', 5)
        log = w.read_log().splitlines()
    finally:
        w.stop()
    # Innermost frame of the logged stack: the blocking call in the task.
    frame = next((f'{Path(m[1]).name}:{m[2]} in {m[3]}: {log[i + 1].strip()}'
                  for i, line in enumerate(log[:-1])
                  if (m := re.search(r'File "(.+)", line (\d+), in (ablock_loop)', line))), None)
    detail = short(lines[0]) if lines else 'no warning logged'
    if frame:
        detail += f'\n    innermost frame: {frame}'
    return [('blocking call', bool(lines) and r.successful(),
             f'{detail}\n    task {outcome(r)}, 3 tasks sent meanwhile: {summarise(others)}')]


def _kill_child(ctx, w, mode, late):
    if late:
        task = pick(mode, tasks.slow_late, tasks.aslow_late)
    else:
        task = pick(mode, tasks.slow, tasks.aslow)
    count = 10 if mode == 'async' else 4
    results = [w.send(task, 3, str(ctx.markers)) for _ in range(count)]
    by_pid = wait_started(results)
    pid, victims = max(by_pid.items(), key=lambda item: len(item[1]))
    os.kill(pid, signal.SIGKILL)
    wait_ready(results, 60)
    survivors = [r for r in results if r not in victims]
    if late:
        restarted = sum(r.successful() and r.result['starts'] == 2 for r in victims)
        ok = all(r.successful() for r in results) and restarted == len(victims)
        detail = (f'{len(victims)} of {count} tasks on killed pid {pid}: {summarise(victims)}, '
                  f'{restarted} ran twice; others: {summarise(survivors)}')
    else:
        lost = sum(outcome(r) == 'FAILURE WorkerLostError' for r in victims)
        ok = lost == len(victims) and all(r.successful() for r in survivors)
        detail = (f'{len(victims)} of {count} tasks on killed pid {pid}: {summarise(victims)}; '
                  f'others: {summarise(survivors) or "none"}')
    return ok, detail


def scenario_kill_child(ctx, mode):
    w = ctx.worker(mode)
    rows = []
    try:
        ok, detail = _kill_child(ctx, w, mode, late=False)
        rows.append(('SIGKILL child', ok, detail))
        ok, detail = _kill_child(ctx, w, mode, late=True)
        rows.append(('... with acks_late', ok, detail))
    finally:
        w.stop()
    return rows


def scenario_kill_worker(ctx, mode):
    w = ctx.worker(mode)
    try:
        task = pick(mode, tasks.slow_late, tasks.aslow_late)
        count = 20 if mode == 'async' else 6
        results = [w.send(task, 3, str(ctx.markers)) for _ in range(count)]
        wait_started(results)
        w.kill_group()
        killed = time.monotonic()
        states = summarise(results)
        w.start()
        wait_ready(results, 90)
        elapsed = time.monotonic() - killed
    finally:
        w.stop()
    twice = sum(r.successful() and r.result['starts'] == 2 for r in results)
    ok = all(r.successful() for r in results) and twice == count
    return [('SIGKILL worker + restart', ok,
             f'{count} acks_late tasks killed in state {states}; after restart '
             f'{summarise(results)}, {twice} ran twice, {elapsed:.1f}s after the kill')]


def scenario_max_tasks(ctx, mode):
    if mode == 'async':
        args = ['--concurrency', '2', '--aconcurrency', '10', '--aprocesses', '2']
        task, count = tasks.aio_sleep, 24
    else:
        args = ['--concurrency', '2']
        task, count = tasks.io_sleep, 12
    w = Worker(ctx.logdir, args + ['--max-tasks-per-child', '3']).start()
    try:
        results = [w.send(task, 0.2) for _ in range(count)]
        wait_ready(results, 60)
        per_pid = Counter(r.result for r in results if r.successful())
        recycles = len(w.log_lines('Recycling async pool process'))
    finally:
        w.stop()
    ok = sum(per_pid.values()) == count and max(per_pid.values()) <= 3
    detail = (f'{count} tasks ran in {len(per_pid)} processes, at most '
              f'{max(per_pid.values())} per process')
    if mode == 'async':
        detail += f'; {recycles} recycle log lines'
    return [('max_tasks_per_child=3', ok, detail)]


def scenario_shutdown(ctx, mode):
    w = ctx.worker(mode)
    try:
        task = pick(mode, tasks.slow, tasks.aslow)
        count = 50 if mode == 'async' else 8
        results = [w.send(task, 3, str(ctx.markers)) for _ in range(count)]
        wait_started(results)
        took = w.warm_shutdown()
        code = w.proc.returncode
        wait_ready(results, 10)
    finally:
        w.stop()
    ok = took is not None and code == 0 and all(r.successful() for r in results)
    took_text = f'{took:.2f}s' if took is not None else 'did not exit'
    return [('SIGTERM with tasks running', ok,
             f'{count} running tasks: {summarise(results)}; worker exited in '
             f'{took_text} with code {code}')]


SCENARIOS = {
    'basics': scenario_basics,
    'throughput': scenario_throughput,
    'soft_limit': scenario_soft_limit,
    'hard_limit': scenario_hard_limit,
    'revoke': scenario_revoke,
    'watchdog': scenario_watchdog,
    'kill_child': scenario_kill_child,
    'kill_worker': scenario_kill_worker,
    'max_tasks': scenario_max_tasks,
    'shutdown': scenario_shutdown,
}


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    parser.add_argument('--mode', choices=['classic', 'async', 'both'], default='both')
    parser.add_argument('--scenario', default='all',
                        help='comma-separated names out of: ' + ', '.join(SCENARIOS)
                        + '; or "all" (default)')
    parser.add_argument('--io-tasks', type=int, default=500,
                        help='number of tasks in the throughput scenario (default 500)')
    parser.add_argument('--io-seconds', type=float, default=0.5,
                        help='sleep of each throughput task in seconds (default 0.5)')
    parser.add_argument('--keep-logs', action='store_true',
                        help='keep the worker logs after a successful run')
    options = parser.parse_args(argv)
    names = list(SCENARIOS) if options.scenario == 'all' else options.scenario.split(',')
    unknown = [n for n in names if n not in SCENARIOS]
    if unknown:
        parser.error(f'unknown scenario(s): {", ".join(unknown)}')
    options.scenarios = names
    options.modes = ['classic', 'async'] if options.mode == 'both' else [options.mode]
    return options


def check_services():
    with Connection(tasks.BROKER, connect_timeout=3) as conn:
        conn.ensure_connection(max_retries=1, timeout=3)
    client = getattr(tasks.app.backend, 'client', None)
    if client is not None:
        client.ping()


def print_table(rows):
    print()
    print('=' * 100)
    print(f'{"Scenario":<12} {"Mode":<8} {"Check":<28} Result  Detail')
    print('-' * 100)
    for scenario, mode, check, ok, detail in rows:
        result = 'n/a' if ok is None else ('PASS' if ok else 'FAIL')
        first, *rest = detail.split('\n')
        print(f'{scenario:<12} {mode:<8} {check:<28} {result:<7} {first}')
        for line in rest:
            print(f'{"":<58}{line}')
    print('=' * 100)


def main(argv=None):
    options = parse_args(argv)
    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)
    atexit.register(cleanup)
    check_services()
    logdir = tempfile.mkdtemp(prefix='async-worker-demo-')
    ctx = Context(logdir, options)
    print(f'Broker {tasks.BROKER}, backend {tasks.BACKEND}; worker logs in {logdir}')
    print(f'classic mode: {" ".join(MODES["classic"])}')
    print(f'async mode:   {" ".join(MODES["async"])}')
    rows = []
    try:
        for mode in options.modes:
            for name in options.scenarios:
                print(f'--> {name} ({mode})', flush=True)
                start = time.monotonic()
                try:
                    checks = SCENARIOS[name](ctx, mode)
                except (TimeoutError, RuntimeError) as exc:
                    checks = [('error', False, str(exc))]
                for check, ok, detail in checks:
                    rows.append((name, mode, check, ok, detail))
                    result = 'n/a' if ok is None else ('PASS' if ok else 'FAIL')
                    print(f'    {result} {check}: {detail}', flush=True)
                print(f'    ({time.monotonic() - start:.1f}s)', flush=True)
    except KeyboardInterrupt:
        print('\nInterrupted; stopping workers.')
        cleanup()
        return 130
    finally:
        cleanup()
    print_table(rows)
    if len(ctx.throughput) == 2:
        classic, aio = ctx.throughput['classic'], ctx.throughput['async']
        print(f'Throughput: classic {classic:.2f}s, async {aio:.2f}s '
              f'({classic / aio:.1f}x faster)')
    failed = [row for row in rows if row[3] is False]
    print(f'{len(rows) - len(failed)} of {len(rows)} checks passed or n/a, '
          f'{len(failed)} failed.')
    if failed or options.keep_logs:
        print(f'Worker logs: {logdir}')
    else:
        shutil.rmtree(logdir, ignore_errors=True)
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main())
