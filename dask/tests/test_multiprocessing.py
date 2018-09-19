from __future__ import absolute_import, division, print_function

import sys
import multiprocessing
from operator import add
import pickle
import random

import numpy as np

import pytest
from dask import compute, config, delayed
from dask.context import set_options
from dask.multiprocessing import (
    get, _dumps, get_context, remote_exception
)
from dask.utils_test import inc


def test_pickle_globals():
    """ Unrelated globals should not be included in serialized bytes """
    def unrelated_function(a):
        return np.array([a])

    def my_small_function(a, b):
        return a + b

    b = _dumps(my_small_function)
    assert b'my_small_function' in b
    assert b'unrelated_function' not in b
    assert b'numpy' not in b


def bad():
    raise ValueError("12345")


def test_errors_propagate():
    dsk = {'x': (bad,)}

    try:
        get(dsk, 'x')
    except Exception as e:
        assert isinstance(e, ValueError)
        assert "12345" in str(e)


def test_remote_exception():
    e = TypeError("hello")
    a = remote_exception(e, 'traceback-body')
    b = remote_exception(e, 'traceback-body')

    assert type(a) == type(b)
    assert isinstance(a, TypeError)
    assert 'hello' in str(a)
    assert 'Traceback' in str(a)
    assert 'traceback-body' in str(a)


def make_bad_result():
    return lambda x: x + 1


def test_unpicklable_results_generate_errors():

    dsk = {'x': (make_bad_result,)}

    try:
        get(dsk, 'x')
    except Exception as e:
        # can't use type because pickle / cPickle distinction
        assert type(e).__name__ in ('PicklingError', 'AttributeError')


class NotUnpickleable(object):
    def __getstate__(self):
        return ()

    def __setstate__(self, state):
        raise ValueError("Can't unpickle me")


def test_unpicklable_args_generate_errors():
    a = NotUnpickleable()

    def foo(a):
        return 1

    dsk = {'x': (foo, a)}

    try:
        get(dsk, 'x')
    except Exception as e:
        assert isinstance(e, ValueError)

    dsk = {'x': (foo, 'a'),
           'a': a}

    try:
        get(dsk, 'x')
    except Exception as e:
        assert isinstance(e, ValueError)


def test_reuse_pool():
    pool = multiprocessing.Pool()
    with set_options(pool=pool):
        assert get({'x': (inc, 1)}, 'x') == 2
        assert get({'x': (inc, 1)}, 'x') == 2


def test_dumps_loads():
    with set_options(func_dumps=pickle.dumps, func_loads=pickle.loads):
        assert get({'x': 1, 'y': (add, 'x', 2)}, 'y') == 3


def test_fuse_doesnt_clobber_intermediates():
    d = {'x': 1, 'y': (inc, 'x'), 'z': (add, 10, 'y')}
    assert get(d, ['y', 'z']) == (2, 12)


def test_optimize_graph_false():
    from dask.callbacks import Callback
    d = {'x': 1, 'y': (inc, 'x'), 'z': (add, 10, 'y')}
    keys = []
    with Callback(pretask=lambda key, *args: keys.append(key)):
        get(d, 'z', optimize_graph=False)
    assert len(keys) == 2


@pytest.mark.parametrize('random', [np.random, random])
def test_random_seeds(random):
    def f():
        return tuple(random.randint(0, 10000) for i in range(5))

    N = 10
    with set_options(scheduler='processes'):
        results, = compute([delayed(f, pure=False)() for i in range(N)])

    assert len(set(results)) == N


@pytest.mark.skipif(sys.platform == 'win32',
                    reason="Windows doesn't support different contexts")
@pytest.mark.skipif(sys.version_info.major == 2,
                    reason="Python 2 doesn't support different contexts")
def test_custom_context_used_python3_posix():
    """ The 'multiprocessing.context' config is used to create the pool.

    We assume default is 'fork', and therefore test for 'spawn'.  If default
    context is changed this test will need to be modified to be different than
    that.
    """
    # We check for spawn by ensuring subprocess doesn't have modules only
    # parent process should have:
    def check_for_pytest():
        import sys
        return "FAKE_MODULE_FOR_TEST" in sys.modules

    import sys
    sys.modules["FAKE_MODULE_FOR_TEST"] = 1
    try:
        with config.set({"multiprocessing.context": "spawn"}):
            result = get({"x": (check_for_pytest,)}, "x")
        assert not result
    finally:
        del sys.modules["FAKE_MODULE_FOR_TEST"]


@pytest.mark.skipif(sys.platform == 'win32',
                    reason="Windows doesn't support different contexts")
@pytest.mark.skipif(sys.version_info.major == 2,
                    reason="Python 2 doesn't support different contexts")
def test_get_context_using_python3_posix():
    """ get_context() respects configuration.

    If default context is changed this test will need to change too.
    """
    assert get_context() is multiprocessing.get_context(None)
    with config.set({"multiprocessing.context": "forkserver"}):
        assert get_context() is multiprocessing.get_context("forkserver")
    with config.set({"multiprocessing.context": "spawn"}):
        assert get_context() is multiprocessing.get_context("spawn")


@pytest.mark.skipif(sys.platform != 'win32' and sys.version_info.major > 2,
                    reason="Python 3 POSIX supports different contexts")
def test_custom_context_ignored_elsewhere():
    """ On Python 2/Windows, setting 'multiprocessing.context' doesn't explode.

    Presumption is it's not used since unsupported, but mostly we care about
    not breaking anything.
    """
    assert get({'x': (inc, 1)}, 'x') == 2
    with pytest.warns(UserWarning):
        with config.set({"multiprocessing.context": "forkserver"}):
            assert get({'x': (inc, 1)}, 'x') == 2


@pytest.mark.skipif(sys.platform != 'win32' and sys.version_info.major > 2,
                    reason="Python 3 POSIX supports different contexts")
def test_get_context_always_default():
    """ On Python 2/Windows, get_context() always returns same context."""
    assert get_context() is multiprocessing
    with pytest.warns(UserWarning):
        with config.set({"multiprocessing.context": "forkserver"}):
            assert get_context() is multiprocessing
