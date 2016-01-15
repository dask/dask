import pytest

from tornado import gen

from distributed import Executor, Scheduler
from distributed.diagnostics.progressbar import TextProgressBar, progress
from distributed.utils_test import (cluster, _test_cluster, loop, inc,
        div, dec, cluster_center)
from time import time, sleep


def test_text_progressbar(capsys, loop):
    with cluster(nanny=True) as (s, [a, b]):
        with Executor(('127.0.0.1', s['port']), loop=loop) as e:
            futures = e.map(inc, range(10))
            p = TextProgressBar(futures, interval=0.01, complete=True)
            e.gather(futures)

            start = time()
            while p.status != 'finished':
                sleep(0.01)
                assert time() - start < 5

            check_bar_completed(capsys)
            assert p._last_response == {'all': 10,
                                        'remaining': 0,
                                        'status': 'finished'}
            assert p.stream.closed()


def test_TextProgressBar_error(loop, capsys):
    @gen.coroutine
    def f(c, a, b):
        s = Scheduler((c.ip, c.port), loop=loop)
        yield s.sync_center()
        done = s.start()

        s.update_graph(dsk={'x': (div, 1, 0)},
                       keys=['x'])

        progress = TextProgressBar(['x'], scheduler=(s.ip, s.port),
                                   start=False, interval=0.01)
        yield progress.listen()

        assert progress.status == 'error'
        assert progress.stream.closed()

        progress = TextProgressBar(['x'], scheduler=(s.ip, s.port),
                                   start=False, interval=0.01)
        yield progress.listen()
        assert progress.status == 'error'
        assert progress.stream.closed()

        s.close()
        yield done

    _test_cluster(f, loop)


def test_TextProgressBar_empty(loop, capsys):
    @gen.coroutine
    def f(c, a, b):
        s = Scheduler((c.ip, c.port), loop=loop)
        yield s.sync_center()
        done = s.start()

        progress = TextProgressBar([], scheduler=(s.ip, s.port), start=False,
                                   interval=0.01)
        yield progress.listen()

        assert progress.status == 'finished'
        check_bar_completed(capsys)

        s.close()
        yield done

    _test_cluster(f, loop)


def check_bar_completed(capsys, width=40):
    out, err = capsys.readouterr()
    bar, percent, time = [i.strip() for i in out.split('\r')[-1].split('|')]
    assert bar == '[' + '#'*width + ']'
    assert percent == '100% Completed'


def test_progress_function(loop, capsys):
    with cluster() as (s, [a, b]):
        with Executor(('127.0.0.1', s['port']), loop=loop) as e:
            f = e.submit(lambda: 1)
            g = e.submit(lambda: 2)

            progress([[f], [[g]]], notebook=False)
            check_bar_completed(capsys)
