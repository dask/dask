from __future__ import print_function, division, absolute_import

import functools
import gc
import os
import shutil
import subprocess
import sys
from time import sleep

import mock

from distributed.compatibility import Empty
from distributed.diskutils import WorkSpace
from distributed.metrics import time
from distributed.utils import mp_context
from distributed.utils_test import captured_logger, slow, new_config


def assert_directory_contents(dir_path, expected):
    expected = [os.path.join(dir_path, p) for p in expected]
    actual = [os.path.join(dir_path, p)
              for p in os.listdir(dir_path)
              if p not in ('global.lock', 'purge.lock')]
    assert sorted(actual) == sorted(expected)


def test_workdir_simple(tmpdir):
    # Test nominal operation of WorkSpace and WorkDirs
    base_dir = str(tmpdir)
    assert_contents = functools.partial(assert_directory_contents, base_dir)

    ws = WorkSpace(base_dir)
    assert_contents([])
    a = ws.new_work_dir(name='aa')
    assert_contents(['aa', 'aa.dirlock'])
    b = ws.new_work_dir(name='bb')
    assert_contents(['aa', 'aa.dirlock', 'bb', 'bb.dirlock'])
    ws._purge_leftovers()
    assert_contents(['aa', 'aa.dirlock', 'bb', 'bb.dirlock'])

    a.release()
    assert_contents(['bb', 'bb.dirlock'])
    del b
    gc.collect()
    assert_contents([])

    # Generated temporary name with a prefix
    a = ws.new_work_dir(prefix='foo-')
    b = ws.new_work_dir(prefix='bar-')
    c = ws.new_work_dir(prefix='bar-')
    assert_contents({a.dir_path, a._lock_path,
                     b.dir_path, b._lock_path,
                     c.dir_path, c._lock_path})
    assert os.path.basename(a.dir_path).startswith('foo-')
    assert os.path.basename(b.dir_path).startswith('bar-')
    assert os.path.basename(c.dir_path).startswith('bar-')
    assert b.dir_path != c.dir_path


def test_two_workspaces_in_same_directory(tmpdir):
    # If handling the same directory with two WorkSpace instances,
    # things should work ok too
    base_dir = str(tmpdir)
    assert_contents = functools.partial(assert_directory_contents, base_dir)

    ws = WorkSpace(base_dir)
    assert_contents([])
    a = ws.new_work_dir(name='aa')
    assert_contents(['aa', 'aa.dirlock'])

    ws2 = WorkSpace(base_dir)
    ws2._purge_leftovers()
    assert_contents(['aa', 'aa.dirlock'])
    b = ws.new_work_dir(name='bb')
    assert_contents(['aa', 'aa.dirlock', 'bb', 'bb.dirlock'])

    del ws
    del b
    gc.collect()
    assert_contents(['aa', 'aa.dirlock'])
    del a
    gc.collect()
    assert_contents([])


def test_workspace_process_crash(tmpdir):
    # WorkSpace should be able to clean up stale contents left by
    # crashed process
    base_dir = str(tmpdir)
    assert_contents = functools.partial(assert_directory_contents, base_dir)

    ws = WorkSpace(base_dir)

    code = """if 1:
        import signal
        import sys
        import time

        from distributed.diskutils import WorkSpace

        ws = WorkSpace(%(base_dir)r)
        a = ws.new_work_dir(name='aa')
        b = ws.new_work_dir(prefix='foo-')
        print((a.dir_path, b.dir_path))
        sys.stdout.flush()

        time.sleep(100)
        """ % dict(base_dir=base_dir)

    p = subprocess.Popen([sys.executable, '-c', code],
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                         universal_newlines=True)
    line = p.stdout.readline()
    assert p.poll() is None
    a_path, b_path = eval(line)
    assert_contents([a_path, a_path + '.dirlock', b_path, b_path + '.dirlock'])

    # The child process holds a lock so the work dirs shouldn't be removed
    ws._purge_leftovers()
    assert_contents([a_path, a_path + '.dirlock', b_path, b_path + '.dirlock'])

    # Kill the process so it's unable to clear the work dirs itself
    p.kill()
    assert p.wait()  # process returned with non-zero code
    assert_contents([a_path, a_path + '.dirlock', b_path, b_path + '.dirlock'])

    with captured_logger('distributed.diskutils', 'WARNING', propagate=False) as sio:
        ws._purge_leftovers()
    assert_contents([])
    # One log line per purged directory
    lines = sio.getvalue().splitlines()
    assert len(lines) == 2
    for p in (a_path, b_path):
        assert any(repr(p) in line for line in lines)


def test_workspace_rmtree_failure(tmpdir):
    base_dir = str(tmpdir)

    ws = WorkSpace(base_dir)
    a = ws.new_work_dir(name='aa')
    shutil.rmtree(a.dir_path)
    with captured_logger('distributed.diskutils', 'ERROR', propagate=False) as sio:
        a.release()
    lines = sio.getvalue().splitlines()
    # shutil.rmtree() may call its onerror callback several times
    assert lines
    for line in lines:
        assert line.startswith("Failed to remove %r" % (a.dir_path,))


def test_locking_disabled(tmpdir):
    base_dir = str(tmpdir)

    with new_config({'use-file-locking': False}):
        with mock.patch('distributed.diskutils.locket.lock_file') as lock_file:
            assert_contents = functools.partial(assert_directory_contents, base_dir)

            ws = WorkSpace(base_dir)
            assert_contents([])
            a = ws.new_work_dir(name='aa')
            assert_contents(['aa'])
            b = ws.new_work_dir(name='bb')
            assert_contents(['aa', 'bb'])
            ws._purge_leftovers()
            assert_contents(['aa', 'bb'])

            a.release()
            assert_contents(['bb'])
            del b
            gc.collect()
            assert_contents([])

        lock_file.assert_not_called()


def _workspace_concurrency(base_dir, purged_q, err_q, stop_evt):
    ws = WorkSpace(base_dir)
    n_purged = 0
    with captured_logger('distributed.diskutils', 'ERROR') as sio:
        while not stop_evt.is_set():
            # Add a bunch of locks, and simulate forgetting them
            try:
                purged = ws._purge_leftovers()
            except Exception as e:
                err_q.put(e)
            else:
                n_purged += len(purged)

    lines = sio.getvalue().splitlines()
    if lines:
        try:
            raise AssertionError("got %d logs, see stderr" % (len(lines,)))
        except Exception as e:
            err_q.put(e)

    purged_q.put(n_purged)


def _test_workspace_concurrency(tmpdir, timeout, max_procs):
    """
    WorkSpace concurrency test.  We merely check that no exception or
    deadlock happens.
    """
    base_dir = str(tmpdir)

    err_q = mp_context.Queue()
    purged_q = mp_context.Queue()
    stop_evt = mp_context.Event()
    ws = WorkSpace(base_dir)
    # Make sure purging only happens in the child processes
    ws._purge_leftovers = lambda: None

    # Run a bunch of child processes that will try to purge concurrently
    NPROCS = 2 if sys.platform == 'win32' else max_procs
    processes = [mp_context.Process(target=_workspace_concurrency,
                                    args=(base_dir, purged_q, err_q, stop_evt))
                 for i in range(NPROCS)]
    for p in processes:
        p.start()

    n_created = 0
    n_purged = 0
    try:
        t1 = time()
        while time() - t1 < timeout:
            # Add a bunch of locks, and simulate forgetting them.
            # The concurrent processes should try to purge them.
            for i in range(50):
                d = ws.new_work_dir(prefix='workspace-concurrency-')
                d._finalizer.detach()
                n_created += 1
            sleep(1e-2)
    finally:
        stop_evt.set()
        for p in processes:
            p.join()

    # Any errors?
    try:
        err = err_q.get_nowait()
    except Empty:
        pass
    else:
        raise err

    try:
        while True:
            n_purged += purged_q.get_nowait()
    except Empty:
        pass
    # We attempted to purge most directories at some point
    assert n_purged >= 0.5 * n_created > 0
    return n_created, n_purged


def test_workspace_concurrency(tmpdir):
    _test_workspace_concurrency(tmpdir, 2.0, 6)


@slow
def test_workspace_concurrency_intense(tmpdir):
    n_created, n_purged = _test_workspace_concurrency(tmpdir, 8.0, 16)
    assert n_created >= 100
