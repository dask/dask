import pytest
import sys
from tornado import gen

from distributed.scheduler import Scheduler
from distributed.executor import Executor, wait
from distributed.utils_test import cluster, slow, _test_cluster, loop, inc
from distributed.utils import All
from distributed.diagnostics import ProgressBar, TextProgressBar, Diagnostic


def test_diagnostic(loop):
    @gen.coroutine
    def f(c, a, b):
        s = Scheduler((c.ip, c.port))
        yield s._sync_center()
        done = s.start()

        class Counter(Diagnostic):
            def start(self, scheduler):
                scheduler.diagnostics.append(self)
                self.count = 0

            def task_finished(self, scheduler, key, worker, nbytes):
                self.count += 1

        counter = Counter()
        counter.start(s)

        assert counter.count == 0
        s.scheduler_queue.put_nowait({'op': 'update-graph',
                                      'dsk': {'x': (inc, 1),
                                              'y': (inc, 'x'),
                                              'z': (inc, 'y')},
                                      'keys': ['z']})

        while True:
            msg = yield s.report_queue.get()
            if msg['op'] == 'key-in-memory' and msg['key'] == 'z':
                break

        assert counter.count == 3

        s.scheduler_queue.put_nowait({'op': 'close'})
        yield done

    _test_cluster(f, loop)


def test_TextProgressBar(loop, capsys):
    @gen.coroutine
    def f(c, a, b):
        s = Scheduler((c.ip, c.port))
        yield s._sync_center()
        done = s.start()

        with TextProgressBar(s) as progress:
            s.update_graph(dsk={'x': (inc, 1),
                                'y': (inc, 'x'),
                                'z': (inc, 'y')},
                           keys=['z'])

            assert progress.all_keys == {'x', 'y', 'z'}
            assert progress.keys == {'x', 'y', 'z'}

            while True:
                msg = yield s.report_queue.get()
                if msg['op'] == 'key-in-memory' and msg['key'] == 'z':
                    break

            assert progress.keys == set()
            check_bar_completed(capsys)
        assert progress not in s.diagnostics

        s.scheduler_queue.put_nowait({'op': 'close'})
        yield done

    _test_cluster(f, loop)


def test_progressbar_sync(loop, capsys):
    with cluster() as (c, [a, b]):
        with Executor(('127.0.0.1', c['port']), loop=loop) as e:
            with TextProgressBar() as p:
                assert p.scheduler is e.scheduler
                assert p in e.scheduler.diagnostics
                f = e.submit(lambda: 1)
                g = e.submit(lambda: 2)
                f.result()
                g.result()
                check_bar_completed(capsys)
                assert len(p.all_keys) == 2


def test_progressbar_done_futures(loop, capsys):
    with cluster() as (c, [a, b]):
        with Executor(('127.0.0.1', c['port']), loop=loop) as e:
            x = e.submit(lambda x: x + 1, 1)
            wait([x], show_progress=True)
            sys.stdout.flush()
            check_bar_completed(capsys)
            wait([x], show_progress=True)
            sys.stdout.flush()
            check_bar_completed(capsys)


def test_progressbar_no_scheduler():
    with pytest.raises(ValueError):
        ProgressBar()


def check_bar_completed(capsys, width=40):
    out, err = capsys.readouterr()
    bar, percent, time = [i.strip() for i in out.split('\r')[-1].split('|')]
    assert bar == '[' + '#'*width + ']'
    assert percent == '100% Completed'
