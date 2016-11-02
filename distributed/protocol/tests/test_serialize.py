from __future__ import print_function, division, absolute_import

import pickle

import numpy as np
import pytest
from toolz import identity

from distributed.protocol import (register_serialization, serialize,
        deserialize, Serialize, Serialized)
from distributed.protocol import decompress


class MyObj(object):
    def __init__(self, data):
        self.data = data

    def __getstate__(self):
        raise Exception('Not picklable')

    def __deepcopy__(self, _):
        return self


def serialize_myobj(x):
    return {}, [pickle.dumps(x.data)]

def deserialize_myobj(header, frames):
    return MyObj(pickle.loads(frames[0]))


register_serialization(MyObj, serialize_myobj, deserialize_myobj)


def test_dumps_serialize():
    for x in [123, [1, 2, 3]]:
        header, frames = serialize(x)
        assert not header
        assert len(frames) == 1

        result = deserialize(header, frames)
        assert result == x

    x = MyObj(123)
    header, frames = serialize(x)
    assert header['type']
    assert len(frames) == 1

    result = deserialize(header, frames)
    assert result.data == x.data


def test_serialize_bytes():
    b = b'123'
    header, frames = serialize(b)
    assert frames[0] is b


def test_Serialize():
    s = Serialize(123)
    assert '123' in str(s)
    assert s.data == 123

    s = Serialize((1, 2))
    assert str(s)


from distributed.utils_test import gen_cluster
from dask import delayed


@gen_cluster(client=True)
def test_object_in_graph(c, s, a, b):
    o = MyObj(123)
    v = delayed(o)
    v2 = delayed(identity)(v)

    future = c.compute(v2)
    result = yield future._result()

    assert isinstance(result, MyObj)
    assert result.data == 123


@gen_cluster(client=True)
def test_scatter(c, s, a, b):
    o = MyObj(123)
    [future] = yield c._scatter([o])
    yield c._replicate(o)
    o2 = yield c._gather(future)
    assert isinstance(o2, MyObj)
    assert o2.data == 123


@gen_cluster(client=True)
def test_inter_worker_comms(c, s, a, b):
    o = MyObj(123)
    [future] = yield c._scatter([o], workers=a.address)
    future2 = c.submit(identity, future, workers=b.address)
    o2 = yield c._gather(future2)
    assert isinstance(o2, MyObj)
    assert o2.data == 123
