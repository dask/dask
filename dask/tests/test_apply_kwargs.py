import dask
from dask.compatibility import apply
from dask import multiprocessing
from dask import threaded
from dask.distributed import Client
import pytest

def mult(*args, x, y=100):
    return x * y

@pytest.fixture(params=['distributed', 'multiprocessing', 'threaded', 'single-threaded'])
def scheduler(request):
    return request.param

def test_apply_kwargs(scheduler):
    """Test that kwargs can be passed where values are the results of other tasks if apply is used"""
    dsk = {
            'a': 10,
            'b': 100,
            'c': (apply, mult, [], {'x': 'a'}),
    }

    if scheduler == 'single-threaded':
        get = dask.get
        res = dict(zip(dsk.keys(), get(dsk, keys=list(dsk.keys()))))
    elif scheduler == 'threaded':
        get  = threaded.get
        res = dict(zip(dsk.keys(), get(dsk, list(dsk.keys()))))
    elif scheduler == 'multiprocessing':
        get  = multiprocessing.get
        res = dict(zip(dsk.keys(), get(dsk, keys=list(dsk.keys()))))
    elif scheduler == 'distributed':
        client = Client()
        get = client.get
        res = dict(zip(dsk.keys(), get(dsk, keys=list(dsk.keys()))))


    assert 'a' in res
    assert 'c' in res
    assert res['b'] == 100
    assert res['c'] == 1000


def test_apply_kwargs_to_func_node(scheduler):
    """Test that schedulers handle kwargs when a node is passed as function to apply"""
    dsk = {
            'a': 10,
            'b': mult,
            'c': (apply, 'b', [], {'x': 10}),
    }

    if scheduler == 'single-threaded':
        get = dask.get
        res = dict(zip(dsk.keys(), get(dsk, keys=list(dsk.keys()))))
    elif scheduler == 'threaded':
        get  = threaded.get
        res = dict(zip(dsk.keys(), get(dsk, list(dsk.keys()))))
    elif scheduler == 'multiprocessing':
        get  = multiprocessing.get
        res = dict(zip(dsk.keys(), get(dsk, keys=list(dsk.keys()))))
    elif scheduler == 'distributed':
        client = Client()
        get = client.get
        res = dict(zip(dsk.keys(), get(dsk, keys=list(dsk.keys()))))

    assert 'a' in res
    assert 'c' in res
    assert res['b'] == mult
    assert res['c'] == 1000

