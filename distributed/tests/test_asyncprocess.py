from __future__ import print_function, division, absolute_import

from datetime import timedelta
import gc
import os
import signal
import sys
from time import sleep
import weakref

import pytest
from tornado import gen
from tornado.locks import Event

from distributed.metrics import time
from distributed.process import AsyncProcess
from distributed.utils import ignoring, mp_context
from distributed.utils_test import gen_test


def feed(in_q, out_q):
    obj = in_q.get(timeout=5)
    out_q.put(obj)

def exit(q):
    sys.exit(q.get())

def exit_now(rc=0):
    sys.exit(rc)

def exit_with_signal(signum):
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    while True:
        os.kill(os.getpid(), signum)
        sleep(0.01)

def wait():
    while True:
        sleep(0.01)


@gen_test()
def test_simple():
    to_child = mp_context.Queue()
    from_child = mp_context.Queue()

    proc = AsyncProcess(target=feed, args=(to_child, from_child))
    assert not proc.is_alive()
    assert proc.pid is None
    assert proc.exitcode is None
    assert not proc.daemon
    proc.daemon = True
    assert proc.daemon

    wr1 = weakref.ref(proc)
    wr2 = weakref.ref(proc._process)

    # join() before start()
    with pytest.raises(AssertionError):
        yield proc.join()

    yield proc.start()
    assert proc.is_alive()
    assert proc.pid is not None
    assert proc.exitcode is None

    t1 = time()
    yield proc.join(timeout=0.02)
    dt = time() - t1
    assert 0.2 >= dt >= 0.01
    assert proc.is_alive()
    assert proc.pid is not None
    assert proc.exitcode is None

    # setting daemon attribute after start()
    with pytest.raises(AssertionError):
        proc.daemon = False

    to_child.put(5)
    assert from_child.get() == 5

    # child should be stopping now
    t1 = time()
    yield proc.join(timeout=10)
    dt = time() - t1
    assert dt <= 1.0
    assert not proc.is_alive()
    assert proc.pid is not None
    assert proc.exitcode == 0

    # join() again
    t1 = time()
    yield proc.join()
    dt = time() - t1
    assert dt <= 0.6

    del proc
    gc.collect()
    if wr1() is not None:
        # Help diagnosing
        from types import FrameType
        p = wr1()
        if p is not None:
            rc = sys.getrefcount(p)
            refs = gc.get_referrers(p)
            del p
            print("refs to proc:", rc, refs)
            frames = [r for r in refs if isinstance(r, FrameType)]
            for i, f in enumerate(frames):
                print("frames #%d:" % i,
                      f.f_code.co_name, f.f_code.co_filename, sorted(f.f_locals))
        pytest.fail("AsyncProcess should have been destroyed")
    t1 = time()
    while wr2() is not None:
        yield gen.sleep(0.01)
        gc.collect()
        dt = time() - t1
        assert dt < 2.0


@gen_test()
def test_exitcode():
    q = mp_context.Queue()

    proc = AsyncProcess(target=exit, kwargs={'q': q})
    proc.daemon = True
    assert not proc.is_alive()
    assert proc.exitcode is None

    yield proc.start()
    assert proc.is_alive()
    assert proc.exitcode is None

    q.put(5)
    yield proc.join(timeout=3.0)
    assert not proc.is_alive()
    assert proc.exitcode == 5


@pytest.mark.skipif(os.name == 'nt', reason="POSIX only")
@gen_test()
def test_signal():
    proc = AsyncProcess(target=exit_with_signal, args=(signal.SIGINT,))
    proc.daemon = True
    assert not proc.is_alive()
    assert proc.exitcode is None

    yield proc.start()
    yield proc.join(timeout=3.0)

    assert not proc.is_alive()
    # Can be 255 with forkserver, see https://bugs.python.org/issue30589
    assert proc.exitcode in (-signal.SIGINT, 255)

    proc = AsyncProcess(target=wait)
    yield proc.start()
    os.kill(proc.pid, signal.SIGTERM)
    yield proc.join(timeout=3.0)

    assert not proc.is_alive()
    assert proc.exitcode in (-signal.SIGTERM, 255)


@gen_test()
def test_terminate():
    proc = AsyncProcess(target=wait)
    proc.daemon = True
    yield proc.start()
    yield proc.terminate()

    yield proc.join(timeout=3.0)
    assert not proc.is_alive()
    assert proc.exitcode in (-signal.SIGTERM, 255)


@gen_test()
def test_exit_callback():
    to_child = mp_context.Queue()
    from_child = mp_context.Queue()
    evt = Event()

    @gen.coroutine
    def on_stop(_proc):
        assert _proc is proc
        yield gen.moment
        evt.set()

    # Normal process exit
    proc = AsyncProcess(target=feed, args=(to_child, from_child))
    evt.clear()
    proc.set_exit_callback(on_stop)
    proc.daemon = True

    yield proc.start()
    yield gen.sleep(0.05)
    assert proc.is_alive()
    assert not evt.is_set()

    to_child.put(None)
    yield evt.wait(timedelta(seconds=3))
    assert evt.is_set()
    assert not proc.is_alive()

    # Process terminated
    proc = AsyncProcess(target=wait)
    evt.clear()
    proc.set_exit_callback(on_stop)
    proc.daemon = True

    yield proc.start()
    yield gen.sleep(0.05)
    assert proc.is_alive()
    assert not evt.is_set()

    yield proc.terminate()
    yield evt.wait(timedelta(seconds=3))
    assert evt.is_set()


@pytest.mark.skipif(sys.platform.startswith('win'),
                    reason="num_fds not supported on windows")
@gen_test()
def test_num_fds():
    psutil = pytest.importorskip('psutil')

    # Warm up
    proc = AsyncProcess(target=exit_now)
    proc.daemon = True
    yield proc.start()
    yield proc.join()

    p = psutil.Process()
    before = p.num_fds()

    proc = AsyncProcess(target=exit_now)
    proc.daemon = True
    yield proc.start()
    yield proc.join()
    assert not proc.is_alive()
    assert proc.exitcode == 0

    start = time()
    while p.num_fds() > before:
        yield gen.sleep(0.1)
        print("fds:", before, p.num_fds())
        assert time() < start + 10
