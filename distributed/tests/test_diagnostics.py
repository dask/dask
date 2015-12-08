import pytest
import sys
from tornado import gen
from tornado.queues import Queue

from distributed.scheduler import Scheduler
from distributed.executor import Executor, wait
from distributed.utils_test import (cluster, slow, _test_cluster, loop, inc,
        div, dec)
from distributed.utils import All
from distributed.diagnostics import (Progress, TextProgressBar, SchedulerPlugin,
        ProgressWidget, MultiProgress)


def test_diagnostic(loop):
    @gen.coroutine
    def f(c, a, b):
        s = Scheduler((c.ip, c.port), loop=loop)
        yield s._sync_center()
        done = s.start()
        sched, report = Queue(), Queue(); s.handle_queues(sched, report)
        msg = yield report.get(); assert msg['op'] == 'stream-start'

        class Counter(SchedulerPlugin):
            def start(self, scheduler):
                scheduler.diagnostics.append(self)
                self.count = 0

            def task_finished(self, scheduler, key, worker, nbytes):
                self.count += 1

        counter = Counter()
        counter.start(s)

        assert counter.count == 0
        sched.put_nowait({'op': 'update-graph',
               'dsk': {'x': (inc, 1),
                       'y': (inc, 'x'),
                       'z': (inc, 'y')},
               'keys': ['z']})

        while True:
            msg = yield report.get()
            if msg['op'] == 'key-in-memory' and msg['key'] == 'z':
                break

        assert counter.count == 3

        sched.put_nowait({'op': 'close'})
        yield done

    _test_cluster(f, loop)


def test_many_Progresss(loop):
    @gen.coroutine
    def f(c, a, b):
        s = Scheduler((c.ip, c.port), loop=loop)
        yield s._sync_center()
        done = s.start()
        sched, report = Queue(), Queue(); s.handle_queues(sched, report)
        msg = yield report.get(); assert msg['op'] == 'stream-start'

        s.update_graph(dsk={'x': (inc, 1),
                            'y': (inc, 'x'),
                            'z': (inc, 'y')},
                       keys=['z'])

        bars = [Progress(keys=['z'], scheduler=s) for i in range(10)]

        while True:
            msg = yield report.get()
            if msg['op'] == 'key-in-memory' and msg['key'] == 'z':
                break

        assert all(b.status == 'finished' for b in bars)
        sched.put_nowait({'op': 'close'})
        yield done

    _test_cluster(f, loop)


def test_multiprogress(loop):
    @gen.coroutine
    def f(c, a, b):
        s = Scheduler((c.ip, c.port), loop=loop)
        yield s._sync_center()
        done = s.start()
        sched, report = Queue(), Queue(); s.handle_queues(sched, report)
        msg = yield report.get(); assert msg['op'] == 'stream-start'

        s.update_graph(dsk={'x-1': (inc, 1),
                            'x-2': (inc, 'x-1'),
                            'x-3': (inc, 'x-2'),
                            'y-1': (dec, 'x-3'),
                            'y-2': (dec, 'y-1')},
                       keys=['y-2'])

        p = MultiProgress(['y-2'], scheduler=s, func=lambda s: s.split('-')[0])

        assert p.keys == {'x': {'x-1', 'x-2', 'x-3'},
                          'y': {'y-1', 'y-2'}}

        while True:
            msg = yield report.get()
            if msg['op'] == 'key-in-memory' and msg['key'] == 'x-3':
                break

        assert p.keys == {'x': set(),
                          'y': {'y-1', 'y-2'}}

        while True:
            msg = yield report.get()
            if msg['op'] == 'key-in-memory' and msg['key'] == 'y-2':
                break

        assert p.keys == {'x': set(),
                          'y': set()}

        assert p.status == 'finished'

        sched.put_nowait({'op': 'close'})
        yield done

    _test_cluster(f, loop)


def test_TextProgressBar(loop, capsys):
    @gen.coroutine
    def f(c, a, b):
        s = Scheduler((c.ip, c.port), loop=loop)
        yield s._sync_center()
        done = s.start()
        sched, report = Queue(), Queue(); s.handle_queues(sched, report)
        msg = yield report.get(); assert msg['op'] == 'stream-start'

        s.update_graph(dsk={'x': (inc, 1),
                            'y': (inc, 'x'),
                            'z': (inc, 'y')},
                       keys=['z'])
        progress = TextProgressBar(['z'], scheduler=s)
        progress.start()

        assert progress.all_keys == {'x', 'y', 'z'}
        assert progress.keys == {'x', 'y', 'z'}

        while True:
            msg = yield report.get()
            if msg['op'] == 'key-in-memory' and msg['key'] == 'z':
                break

        assert progress.keys == set()
        check_bar_completed(capsys)

        assert progress not in s.diagnostics

        sched.put_nowait({'op': 'close'})
        yield done

    _test_cluster(f, loop)


def test_TextProgressBar_error(loop, capsys):
    @gen.coroutine
    def f(c, a, b):
        s = Scheduler((c.ip, c.port), loop=loop)
        yield s._sync_center()
        done = s.start()
        sched, report = Queue(), Queue(); s.handle_queues(sched, report)
        msg = yield report.get(); assert msg['op'] == 'stream-start'

        s.update_graph(dsk={'x': (div, 1, 0)},
                       keys=['x'])
        progress = TextProgressBar(['x'], scheduler=s)
        progress.start()

        while True:
            msg = yield report.get()
            if msg.get('key') == 'x':
                break

        assert progress.status == 'error'
        assert not progress._timer.is_alive()

        progress = TextProgressBar(['x'], scheduler=s)
        progress.start()
        assert progress.status == 'error'
        assert not progress._timer or not progress._timer.is_alive()

        sched.put_nowait({'op': 'close'})
        yield done

    _test_cluster(f, loop)


def test_TextProgressBar_empty(loop, capsys):
    @gen.coroutine
    def f(c, a, b):
        s = Scheduler((c.ip, c.port), loop=loop)
        yield s._sync_center()
        done = s.start()
        sched, report = Queue(), Queue(); s.handle_queues(sched, report)
        msg = yield report.get(); assert msg['op'] == 'stream-start'

        p = TextProgressBar([], scheduler=s)
        p.start()
        assert p.status == 'finished'
        check_bar_completed(capsys)

    _test_cluster(f, loop)


def test_progressbar_sync(loop, capsys):
    with cluster() as (c, [a, b]):
        with Executor(('127.0.0.1', c['port']), loop=loop) as e:
            f = e.submit(lambda: 1)
            g = e.submit(lambda: 2)
            p = TextProgressBar([f, g])
            p.start()
            # assert p in e.scheduler.diagnostics
            assert p.scheduler is e.scheduler
            f.result()
            g.result()
            sys.stdout.flush()
            check_bar_completed(capsys)
            assert len(p.all_keys) == 2

            h = e.submit(lambda x, y: x / y, 1, 0)
            p = TextProgressBar([h])
            p.start()
            h.exception()
            with pytest.raises(AssertionError):
                check_bar_completed(capsys)


def test_Progress_no_scheduler():
    with pytest.raises(ValueError):
        Progress([])


def check_bar_completed(capsys, width=40):
    out, err = capsys.readouterr()
    bar, percent, time = [i.strip() for i in out.split('\r')[-1].split('|')]
    assert bar == '[' + '#'*width + ']'
    assert percent == '100% Completed'
