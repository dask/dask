from time import sleep

import pytest

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
async def test_TextProgressBar_error(c, s, a, b):
    x = c.submit(div, 1, 0)

    progress = TextProgressBar([x.key], scheduler=s.address, start=False, interval=0.01)
    await progress.listen()

    assert progress.status == "error"
    assert progress.comm.closed()

    progress = TextProgressBar([x.key], scheduler=s.address, start=False, interval=0.01)
    await progress.listen()
    assert progress.status == "error"
    assert progress.comm.closed()


@pytest.mark.asyncio
async def test_TextProgressBar_empty(capsys):
    async with Scheduler(port=0) as s:
        async with Worker(s.address, nthreads=1) as a:
            async with Worker(s.address, nthreads=1) as b:
                progress = TextProgressBar(
                    [], scheduler=s.address, start=False, interval=0.01
                )
                await progress.listen()

                assert progress.status == "finished"
                check_bar_completed(capsys)


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


def test_progress_function_w_kwargs(client, capsys):
    f = client.submit(lambda: 1)
    g = client.submit(lambda: 2)

    progress(f, interval="20ms")
    check_bar_completed(capsys)
