from __future__ import print_function, division, absolute_import

import numpy as np
import pytest

from distributed.protocol import (serialize, deserialize, decompress, dumps,
        loads, to_serialize)
from distributed.protocol.utils import BIG_BYTES_SHARD_SIZE
from distributed.utils import tmpfile
from distributed.utils_test import slow

import distributed.protocol.numpy


def test_serialize():
    x = np.ones((5, 5))
    header, frames = serialize(x)
    assert header['type']
    assert len(frames) == 1

    if 'compression' in header:
        frames = decompress(header, frames)
    result = deserialize(header, frames)
    assert (result == x).all()


@pytest.mark.parametrize('x',
        [np.ones(5),
         np.asfortranarray(np.random.random((5, 5))),
         np.random.random(5).astype('f4'),
         np.array(['abc'], dtype='S3'),
         np.array(['abc'], dtype='U3'),
         np.array(['abc'], dtype=object),
         np.array([(1, 'abc')], dtype=[('x', 'i4'), ('s', object)]),
         np.ones(shape=(5, 6)).astype(dtype=[('total', '<f8'), ('n', '<f8')])])
def test_dumps_serialize_numpy(x):
    header, frames = serialize(x)
    if 'compression' in header:
        frames = decompress(header, frames)
    y = deserialize(header, frames)

    np.testing.assert_equal(x, y)


def test_memmap():
    with tmpfile('npy') as fn:
        with open(fn, 'wb') as f:  # touch file
            pass
        x = np.memmap(fn, shape=(5, 5), dtype='i4', mode='readwrite')
        x[:] = 5

        header, frames = serialize(x)
        if 'compression' in header:
            frames = decompress(header, frames)
        y = deserialize(header, frames)

        np.testing.assert_equal(x, y)


@slow
def test_dumps_serialize_numpy_large():
    psutil = pytest.importorskip('psutil')
    if psutil.virtual_memory().total < 4e9:
        return
    x = np.random.randint(0, 255, size=int(BIG_BYTES_SHARD_SIZE * 2)).astype('u1')
    frames = dumps([to_serialize(x)])
    [y] = loads(frames)

    np.testing.assert_equal(x, y)
