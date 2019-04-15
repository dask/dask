from __future__ import print_function, division, absolute_import

from datetime import timedelta
import gc
import os
import signal
import sys
import threading
from time import sleep
import weakref

import pytest
from tornado import gen
from tornado.locks import Event

from distributed.metrics import time
from distributed.process import AsyncProcess
from distributed.utils import mp_context
from distributed.utils_test import gen_test, pristine_loop, nodebug


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


def threads_info(q):
    q.put(len(threading.enumerate()))
    q.put(threading.current_thread().name)


@nodebug
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
    start = time()
    while wr1() is not None and time() < start + 1:
        # Perhaps the GIL switched before _watch_process() exit,
        # help it a little
        sleep(0.001)
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
                print(
                    "frames #%d:" % i,
                    f.f_code.co_name,
                    f.f_code.co_filename,
                    sorted(f.f_locals),
                )
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

    proc = AsyncProcess(target=exit, kwargs={"q": q})
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


@pytest.mark.skipif(os.name == "nt", reason="POSIX only")
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
def test_close():
    proc = AsyncProcess(target=exit_now)
    proc.close()
    with pytest.raises(ValueError):
        yield proc.start()

    proc = AsyncProcess(target=exit_now)
    yield proc.start()
    proc.close()
    with pytest.raises(ValueError):
        yield proc.terminate()

    proc = AsyncProcess(target=exit_now)
    yield proc.start()
    yield proc.join()
    proc.close()
    with pytest.raises(ValueError):
        yield proc.join()
    proc.close()


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


@gen_test()
def test_child_main_thread():
    """
    The main thread in the child should be called "MainThread".
    """
    q = mp_context.Queue()
    proc = AsyncProcess(target=threads_info, args=(q,))
    yield proc.start()
    yield proc.join()
    n_threads = q.get()
    main_name = q.get()
    assert n_threads <= 3
    assert main_name == "MainThread"
    q.close()
    q._reader.close()
    q._writer.close()


@pytest.mark.skipif(
    sys.platform.startswith("win"), reason="num_fds not supported on windows"
)
@gen_test()
def test_num_fds():
    psutil = pytest.importorskip("psutil")

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


@gen_test()
def test_terminate_after_stop():
    proc = AsyncProcess(target=sleep, args=(0,))
    yield proc.start()
    yield gen.sleep(0.1)
    yield proc.terminate()


def _worker_process(worker_ready, child_pipe):
    # child_pipe is the write-side of the children_alive pipe held by the
    # test process. When this _worker_process exits, this file descriptor should
    # have no references remaining anywhere and be closed by the kernel. The
    # test will therefore be able to tell that this process has exited by
    # reading children_alive.

    # Signal to parent process that this process has started and made it this
    # far. This should cause the parent to exit rapidly after this statement.
    worker_ready.set()

    # The parent exiting should cause this process to os._exit from a monitor
    # thread. This sleep should never return.
    shorter_timeout = 2.5  # timeout shorter than that in the spawning test.
    sleep(shorter_timeout)

    # Unreachable if functioning correctly.
    child_pipe.send("child should have exited by now")


def _parent_process(child_pipe):
    """ Simulate starting an AsyncProcess and then dying.

    The child_alive pipe is held open for as long as the child is alive, and can
    be used to determine if it exited correctly. """

    def parent_process_coroutine():
        worker_ready = mp_context.Event()

        worker = AsyncProcess(target=_worker_process, args=(worker_ready, child_pipe))

        yield worker.start()

        # Wait for the child process to have started.
        worker_ready.wait()

        # Exit immediately, without doing any process teardown (including atexit
        # and 'finally:' blocks) as if by SIGKILL. This should cause
        # worker_process to also exit.
        os._exit(255)

    with pristine_loop() as loop:
        try:
            loop.run_sync(gen.coroutine(parent_process_coroutine), timeout=10)
        finally:
            loop.stop()

            raise RuntimeError("this should be unreachable due to os._exit")


def test_asyncprocess_child_teardown_on_parent_exit():
    r""" Check that a child process started by AsyncProcess exits if its parent
    exits.

    The motivation is to ensure that if an AsyncProcess is created and the
    creator process dies unexpectedly (e.g, via Out-of-memory SIGKILL), the
    child process and resources held by it should not be leaked.

    The child should monitor its parent and exit promptly if the parent exits.

    [test process] -> [parent using AsyncProcess (dies)] -> [worker process]
                 \                                          /
                  \________ <--   child_pipe   <-- ________/
    """
    # When child_pipe is closed, the children_alive pipe unblocks.
    children_alive, child_pipe = mp_context.Pipe(duplex=False)

    try:
        parent = mp_context.Process(target=_parent_process, args=(child_pipe,))
        parent.start()

        # Close our reference to child_pipe so that the child has the only one.
        child_pipe.close()

        # Wait for the parent to exit. By the time join returns, the child
        # process is orphaned, and should be in the process of exiting by
        # itself.
        parent.join()

        # By the time we reach here,the parent has exited. The parent only exits
        # when the child is ready to enter the sleep, so all of the slow things
        # (process startup, etc) should have happened by now, even on a busy
        # system. A short timeout should therefore be appropriate.
        short_timeout = 5.0
        # Poll is used to allow other tests to proceed after this one in case of
        # test failure.
        try:
            readable = children_alive.poll(short_timeout)
        except EnvironmentError:
            # Windows can raise BrokenPipeError. EnvironmentError is caught for
            # Python2/3 portability.
            assert sys.platform.startswith("win"), "should only raise on windows"
            # Broken pipe implies closed, which is readable.
            readable = True

        # If this assert fires, then something went wrong. Either the child
        # should write into the pipe, or it should exit and the pipe should be
        # closed (which makes it become readable).
        assert readable

        try:
            # This won't block due to the above 'assert readable'.
            result = children_alive.recv()
        except EOFError:
            pass  # Test passes.
        except EnvironmentError:
            # Windows can raise BrokenPipeError. EnvironmentError is caught for
            # Python2/3 portability.
            assert sys.platform.startswith("win"), "should only raise on windows"
            # Test passes.
        else:
            # Oops, children_alive read something. It should be closed. If
            # something was read, it's a message from the child telling us they
            # are still alive!
            raise RuntimeError("unreachable: {}".format(result))

    finally:
        # Cleanup.
        children_alive.close()
