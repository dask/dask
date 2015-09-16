import pytest
pytest.importorskip('dill')

from dask.multiprocessing import get, dill_apply_async
from dask.context import set_options
import multiprocessing
import dill
import pickle
from operator import add
from dask.utils import raises


inc = lambda x: x + 1


def test_apply_lambda():
    p = multiprocessing.Pool()
    try:
        result = dill_apply_async(p.apply_async, lambda x: x + 1, args=[1])
        assert isinstance(result, multiprocessing.pool.ApplyResult)
        assert result.get() == 2
    finally:
        p.close()


def bad():
    raise ValueError("12345")


def test_errors_propagate():
    dsk = {'x': (bad,)}

    try:
        result = get(dsk, 'x')
    except Exception as e:
        assert isinstance(e, ValueError)
        assert "12345" in str(e)


def make_bad_result():
    return lambda x: x + 1


def test_unpicklable_results_genreate_errors():

    dsk = {'x': (make_bad_result,)}

    try:
        result = get(dsk, 'x')
    except Exception as e:
        # can't use type because pickle / cPickle distinction
        assert type(e).__name__ == 'PicklingError'


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
