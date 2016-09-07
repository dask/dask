from __future__ import print_function, division, absolute_import

import pytest

from tornado import gen

from distributed import Client, Scheduler, Worker
from distributed.diagnostics.progressbar import TextProgressBar, progress
from distributed.utils_test import (cluster, loop, inc,
        div, dec, gen_cluster)
from distributed.worker import dumps_task
from time import time, sleep


def test_text_progressbar(capsys, loop):
    with cluster(nanny=True) as (s, [a, b]):
        with Client(('127.0.0.1', s['port']), loop=loop) as c:
            futures = c.map(inc, range(10))
            p = TextProgressBar(futures, interval=0.01, complete=True)
            c.gather(futures)

            start = time()
            while p.status != 'finished':
                sleep(0.01)
                assert time() - start < 5

            check_bar_completed(capsys)
            assert p._last_response == {'all': 10,
                                        'remaining': 0,
                                        'status': 'finished'}
            assert p.stream.closed()


@gen_cluster(client=True)
def test_TextProgressBar_error(c, s, a, b):
    x = c.submit(div, 1, 0)

    progress = TextProgressBar([x.key], scheduler=(s.ip, s.port),
                               start=False, interval=0.01)
    yield progress.listen()

    assert progress.status == 'error'
    assert progress.stream.closed()

    progress = TextProgressBar([x.key], scheduler=(s.ip, s.port),
                               start=False, interval=0.01)
    yield progress.listen()
    assert progress.status == 'error'
    assert progress.stream.closed()


def test_TextProgressBar_empty(loop, capsys):
    @gen.coroutine
    def f():
        s = Scheduler(loop=loop)
        done = s.start(0)
        a = Worker(s.ip, s.port, loop=loop, ncores=1)
        b = Worker(s.ip, s.port, loop=loop, ncores=1)
        yield [a._start(0), b._start(0)]

        progress = TextProgressBar([], scheduler=(s.ip, s.port), start=False,
                                   interval=0.01)
        yield progress.listen()

        assert progress.status == 'finished'
        check_bar_completed(capsys)

        yield [a._close(), b._close()]
        s.close()
        yield done

    loop.run_sync(f)


def check_bar_completed(capsys, width=40):
    out, err = capsys.readouterr()
    bar, percent, time = [i.strip() for i in out.split('\r')[-1].split('|')]
    assert bar == '[' + '#'*width + ']'
    assert percent == '100% Completed'


def test_progress_function(loop, capsys):
    with cluster() as (s, [a, b]):
        with Client(('127.0.0.1', s['port']), loop=loop) as c:
            f = c.submit(lambda: 1)
            g = c.submit(lambda: 2)

            progress([[f], [[g]]], notebook=False)
            check_bar_completed(capsys)

            progress(f)
            check_bar_completed(capsys)
