from __future__ import print_function, division, absolute_import

from zlib import crc32

import numpy as np
import pytest

from distributed.protocol import (serialize, deserialize, decompress, dumps,
        loads, to_serialize)
from distributed.protocol.utils import BIG_BYTES_SHARD_SIZE
from distributed.utils import tmpfile
from distributed.utils_test import slow
from distributed.protocol.numpy import itemsize
from distributed.protocol.compression import maybe_compress


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
         np.array(5),
         np.asfortranarray(np.random.random((5, 5))),
         np.random.random(5).astype('f4'),
         np.random.random(5).astype('>i8'),
         np.random.random(5).astype('<i8'),
         np.arange(5).astype('M8[us]'),
         np.arange(5).astype('M8[ms]'),
         np.arange(5).astype('m8'),
         np.arange(5).astype('m8[s]'),
         np.arange(5).astype('c16'),
         np.arange(5).astype('c8'),
         np.array([True, False, True]),
         np.ones(shape=5, dtype=[('a', 'i4'), ('b', 'M8[us]')]),
         np.array(['abc'], dtype='S3'),
         np.array(['abc'], dtype='U3'),
         np.array(['abc'], dtype=object),
         np.ones(shape=(5,), dtype=('f8', 32)),
         np.ones(shape=(5,), dtype=[('x', 'f8', 32)]),
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
    if psutil.virtual_memory().total < 2e9:
        return
    x = np.random.random(size=int(BIG_BYTES_SHARD_SIZE * 2 // 8)).view('u1')
    assert x.nbytes == BIG_BYTES_SHARD_SIZE * 2
    frames = dumps([to_serialize(x)])
    dtype, shape = x.dtype, x.shape
    checksum = crc32(x)
    del x
    [y] = loads(frames)

    assert (y.dtype, y.shape) == (dtype, shape)
    assert crc32(y) == checksum, "Arrays are unequal"


@pytest.mark.parametrize('dt,size', [('f8', 8),
                                     ('i4', 4),
                                     ('c16', 16),
                                     ('b', 1),
                                     ('S3', 3),
                                     ('M8[us]', 8),
                                     ('M8[s]', 8),
                                     ('U3', 12),
                                     ([('a', 'i4'), ('b', 'f8')], 12),
                                     (('i4', 100), 4),
                                     ([('a', 'i4', 100)], 8),
                                     ([('a', 'i4', 20), ('b', 'f8')], 20*4 + 8),
                                     ([('a', 'i4', 200), ('b', 'f8')], 8)])
def test_itemsize(dt, size):
    assert itemsize(np.dtype(dt)) == size


def test_compress_numpy():
    x = np.ones(10000000, dtype='i4')
    compression, compressed = maybe_compress(x.data)
    if compression:
        assert len(compressed) < x.nbytes


def test_compress_memoryview():
    mv = memoryview(b'0' * 1000000)
    compression, compressed = maybe_compress(mv)
    if compression:
        assert len(compressed) < len(mv)
