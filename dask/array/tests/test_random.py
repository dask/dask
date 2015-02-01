import numpy as np
from dask.array.core import Array
from dask.array.random import random, exponential, normal
from dask.array.into import into


def test_random():
    a = random((10, 10), blockshape=(5, 5))
    assert isinstance(a, Array)
    assert isinstance(a.name, str) and a.name
    assert a.shape == (10, 10)
    assert a.blockdims == ((5, 5), (5, 5))

    x = set(into(np.ndarray, a).flat)

    assert len(x) > 90


def test_parametrized_random_function():
    a = exponential(1000, (10, 10), blockshape=(5, 5))
    assert isinstance(a, Array)
    assert isinstance(a.name, str) and a.name
    assert a.shape == (10, 10)
    assert a.blockdims == ((5, 5), (5, 5))

    x = into(np.ndarray, a)
    assert 10 < x.mean() < 100000

    y = set(x.flat)
    assert len(y) > 90


def test_kwargs():
    a = normal(loc=10.0, scale=0.1, size=(10, 10), blockshape=(5, 5))
    assert isinstance(a, Array)
    x = into(np.ndarray, a)
    assert 8 < x.mean() < 12


def test_kwargs_size_or_shape():
    a = normal(loc=10.0, scale=0.1, shape=(10, 10), blockshape=(5, 5))
    b = normal(loc=10.0, scale=0.1, size=(10, 10), blockshape=(5, 5))
    assert isinstance(a, Array)
    assert isinstance(b, Array)
    assert a.blockdims == b.blockdims

    assert np.array(a).shape == np.array(b).shape


def test_unique_names():
    a = random((10, 10), blockshape=(5, 5))
    b = random((10, 10), blockshape=(5, 5))

    assert a.name != b.name


def test_docs():
    assert 'exponential' in exponential.__doc__
    assert 'exponential' in exponential.__name__
