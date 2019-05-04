from __future__ import print_function, division, absolute_import

from time import sleep

from tornado import gen

from distributed import Scheduler, Worker
from distributed.diagnostics.progressbar import TextProgressBar, progress
from distributed.metrics import time
from distributed.utils_test import inc, div, gen_cluster
from distributed.utils_test import client, loop, cluster_fixture  # noqa: F401


def test_text_progressbar(capsys, client):
    futures = client.map(inc, range(10))
    p = TextProgressBar(futures, interval=0.01, complete=True)
    client.gather(futures)

    start = time()
    while p.status != "finished":
        sleep(0.01)
        assert time() - start < 5

    check_bar_completed(capsys)
    assert p._last_response == {"all": 10, "remaining": 0, "status": "finished"}
    assert p.comm.closed()


@gen_cluster(client=True)
def test_TextProgressBar_error(c, s, a, b):
    x = c.submit(div, 1, 0)

    progress = TextProgressBar(
        [x.key], scheduler=(s.ip, s.port), start=False, interval=0.01
    )
    yield progress.listen()

    assert progress.status == "error"
    assert progress.comm.closed()

    progress = TextProgressBar(
        [x.key], scheduler=(s.ip, s.port), start=False, interval=0.01
    )
    yield progress.listen()
    assert progress.status == "error"
    assert progress.comm.closed()


def test_TextProgressBar_empty(loop, capsys):
    @gen.coroutine
    def f():
        s = Scheduler(loop=loop)
        done = s.start(0)
        a = Worker(s.ip, s.port, loop=loop, ncores=1)
        b = Worker(s.ip, s.port, loop=loop, ncores=1)
        yield [a._start(0), b._start(0)]

        progress = TextProgressBar(
            [], scheduler=(s.ip, s.port), start=False, interval=0.01
        )
        yield progress.listen()

        assert progress.status == "finished"
        check_bar_completed(capsys)

        yield [a.close(), b.close()]
        s.close()
        yield done

    loop.run_sync(f)


def check_bar_completed(capsys, width=40):
    out, err = capsys.readouterr()
    # trailing newline so grab next to last line for final state of bar
    bar, percent, time = [i.strip() for i in out.split("\r")[-2].split("|")]
    assert bar == "[" + "#" * width + "]"
    assert percent == "100% Completed"


def test_progress_function(client, capsys):
    f = client.submit(lambda: 1)
    g = client.submit(lambda: 2)

    progress([[f], [[g]]], notebook=False)
    check_bar_completed(capsys)

    progress(f)
    check_bar_completed(capsys)
