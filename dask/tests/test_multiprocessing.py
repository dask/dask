import sys
import multiprocessing
from operator import add
import pickle
import random

import numpy as np

import pytest
import dask
from dask import compute, delayed
from dask.multiprocessing import get, _dumps, get_context, remote_exception
from dask.utils_test import inc


try:
    import cloudpickle  # noqa: F401

    has_cloudpickle = True
except ImportError:
    has_cloudpickle = False

requires_cloudpickle = pytest.mark.skipif(
    not has_cloudpickle, reason="requires cloudpickle"
)
not_cloudpickle = pytest.mark.skipif(has_cloudpickle, reason="cloudpickle is installed")


def unrelated_function_global(a):
    return np.array([a])


def my_small_function_global(a, b):
    return a + b


def test_pickle_globals():
    """ Unrelated globals should not be included in serialized bytes """
    b = _dumps(my_small_function_global)
    assert b"my_small_function_global" in b
    assert b"unrelated_function_global" not in b
    assert b"numpy" not in b


@requires_cloudpickle
def test_pickle_locals():
    """Unrelated locals should not be included in serialized bytes
    """

    def unrelated_function_local(a):
        return np.array([a])

    def my_small_function_local(a, b):
        return a + b

    b = _dumps(my_small_function_local)
    assert b"my_small_function_global" not in b
    assert b"my_small_function_local" in b
    assert b"unrelated_function_local" not in b


def bad():
    raise ValueError("12345")


def test_errors_propagate():
    dsk = {"x": (bad,)}

    with pytest.raises(ValueError) as e:
        get(dsk, "x")
    assert "12345" in str(e.value)


def test_remote_exception():
    e = TypeError("hello")
    a = remote_exception(e, "traceback-body")
    b = remote_exception(e, "traceback-body")

    assert type(a) == type(b)
    assert isinstance(a, TypeError)
    assert "hello" in str(a)
    assert "Traceback" in str(a)
    assert "traceback-body" in str(a)


@requires_cloudpickle
def test_lambda_with_cloudpickle():
    dsk = {"x": 2, "y": (lambda x: x + 1, "x")}
    assert get(dsk, "y") == 3


@not_cloudpickle
def test_lambda_without_cloudpickle():
    dsk = {"x": 2, "y": (lambda x: x + 1, "x")}
    with pytest.raises(ModuleNotFoundError) as e:
        get(dsk, "y")
    assert "cloudpickle" in str(e.value)


def lambda_result():
    return lambda x: x + 1


@requires_cloudpickle
def test_lambda_results_with_cloudpickle():
    dsk = {"x": (lambda_result,)}
    f = get(dsk, "x")
    assert f(2) == 3


@not_cloudpickle
def test_lambda_results_without_cloudpickle():
    dsk = {"x": (lambda_result,)}
    with pytest.raises(ModuleNotFoundError) as e:
        get(dsk, "x")
    assert "cloudpickle" in str(e.value)


class NotUnpickleable(object):
    def __getstate__(self):
        return ()

    def __setstate__(self, state):
        raise ValueError("Can't unpickle me")


def test_unpicklable_args_generate_errors():
    a = NotUnpickleable()

    dsk = {"x": (bool, a)}

    with pytest.raises(ValueError):
        get(dsk, "x")

    dsk = {"x": (bool, "a"), "a": a}

    with pytest.raises(ValueError):
        get(dsk, "x")


def test_reuse_pool():
    with multiprocessing.Pool() as pool:
        with dask.config.set(pool=pool):
            assert get({"x": (inc, 1)}, "x") == 2
            assert get({"x": (inc, 1)}, "x") == 2


def test_dumps_loads():
    with dask.config.set(func_dumps=pickle.dumps, func_loads=pickle.loads):
        assert get({"x": 1, "y": (add, "x", 2)}, "y") == 3


def test_fuse_doesnt_clobber_intermediates():
    d = {"x": 1, "y": (inc, "x"), "z": (add, 10, "y")}
    assert get(d, ["y", "z"]) == (2, 12)


def test_optimize_graph_false():
    from dask.callbacks import Callback

    d = {"x": 1, "y": (inc, "x"), "z": (add, 10, "y")}
    keys = []
    with Callback(pretask=lambda key, *args: keys.append(key)):
        get(d, "z", optimize_graph=False)
    assert len(keys) == 2


@requires_cloudpickle
@pytest.mark.parametrize("random", [np.random, random])
def test_random_seeds(random):
    @delayed(pure=False)
    def f():
        return tuple(random.randint(0, 10000) for i in range(5))

    N = 10
    with dask.config.set(scheduler="processes"):
        (results,) = compute([f() for _ in range(N)])

    assert len(set(results)) == N


def check_for_pytest():
    """We check for spawn by ensuring subprocess doesn't have modules only
    parent process should have:
    """
    import sys

    return "FAKE_MODULE_FOR_TEST" in sys.modules


@pytest.mark.skipif(
    sys.platform == "win32", reason="Windows doesn't support different contexts"
)
def test_custom_context_used_python3_posix():
    """ The 'multiprocessing.context' config is used to create the pool.

    We assume default is 'fork', and therefore test for 'spawn'.  If default
    context is changed this test will need to be modified to be different than
    that.
    """
    sys.modules["FAKE_MODULE_FOR_TEST"] = 1
    try:
        with dask.config.set({"multiprocessing.context": "spawn"}):
            result = get({"x": (check_for_pytest,)}, "x")
        assert not result
    finally:
        del sys.modules["FAKE_MODULE_FOR_TEST"]


@pytest.mark.skipif(
    sys.platform == "win32", reason="Windows doesn't support different contexts"
)
def test_get_context_using_python3_posix():
    """ get_context() respects configuration.

    If default context is changed this test will need to change too.
    """
    default_context = None if sys.platform != "darwin" else "fork"
    assert get_context() is multiprocessing.get_context(default_context)
    with dask.config.set({"multiprocessing.context": "forkserver"}):
        assert get_context() is multiprocessing.get_context("forkserver")
    with dask.config.set({"multiprocessing.context": "spawn"}):
        assert get_context() is multiprocessing.get_context("spawn")


@pytest.mark.skipif(sys.platform != "win32", reason="POSIX supports different contexts")
def test_custom_context_ignored_elsewhere():
    """ On Windows, setting 'multiprocessing.context' doesn't explode.

    Presumption is it's not used since unsupported, but mostly we care about
    not breaking anything.
    """
    assert get({"x": (inc, 1)}, "x") == 2
    with pytest.warns(UserWarning):
        with dask.config.set({"multiprocessing.context": "forkserver"}):
            assert get({"x": (inc, 1)}, "x") == 2


@pytest.mark.skipif(sys.platform != "win32", reason="POSIX supports different contexts")
def test_get_context_always_default():
    """ On Python 2/Windows, get_context() always returns same context."""
    assert get_context() is multiprocessing
    with pytest.warns(UserWarning):
        with dask.config.set({"multiprocessing.context": "forkserver"}):
            assert get_context() is multiprocessing
