import pytest

from tornado import gen
from tornado.queues import Queue

from distributed import Executor, Scheduler
from distributed.diagnostics.progressbar import TextProgressBar
from distributed.utils_test import (cluster, scheduler, slow, _test_cluster, loop, inc,
        div, dec, cluster_center)
from time import time, sleep


def test_text_progressbar(capsys, loop):
    with cluster(nanny=True) as (s, [a, b]):
        with Executor(('127.0.0.1', s['port']), loop=loop) as e:
            futures = e.map(inc, range(10))
            p = TextProgressBar(futures, interval=0.01)
            e.gather(futures)
            sleep(0.2)
            check_bar_completed(capsys)
            assert p._last_response == {'all': 10, 'left': 0}
            assert p.stream.closed()


def test_TextProgressBar_error(loop, capsys):
    @gen.coroutine
    def f(c, a, b):
        s = Scheduler((c.ip, c.port), loop=loop)
        s.listen(0)
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
        s.listen(0)
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
