from contextlib import contextmanager

import pytest
from tornado import gen

pytest.importorskip('hdfs3')

from hdfs3 import HDFileSystem

from distributed.utils_test import gen_cluster
from distributed.utils import get_ip
from distributed.hdfs import read_binary, get_block_locations
from distributed import Executor


@contextmanager
def make_hdfs():
    hdfs = HDFileSystem(host='localhost', port=8020)
    if hdfs.exists('/tmp/test'):
        hdfs.rm('/tmp/test')
    hdfs.mkdir('/tmp/test')

    try:
        yield hdfs
    finally:
        if hdfs.exists('/tmp/test'):
            hdfs.rm('/tmp/test')


def test_get_block_locations():
    with make_hdfs() as hdfs:
        data = b'a' * int(1e8)  # todo: reduce block size to speed up test
        fn_1 = '/tmp/test/file1'
        fn_2 = '/tmp/test/file2'

        with hdfs.open(fn_1, 'w', repl=1) as f:
            f.write(data)
        with hdfs.open(fn_2, 'w', repl=1) as f:
            f.write(data)

        L =  get_block_locations(hdfs, '/tmp/test/')
        assert L == get_block_locations(hdfs, fn_1) + get_block_locations(hdfs, fn_2)
        assert L[0]['filename'] == L[1]['filename'] == fn_1
        assert L[2]['filename'] == L[3]['filename'] == fn_2


def test_get_block_locations_nested():
    with make_hdfs() as hdfs:
        data = b'a'

        for i in range(3):
            hdfs.mkdir('/tmp/test/data-%d' % i)
            for j in range(2):
                fn = '/tmp/test/data-%d/file-%d.csv' % (i, j)
                with hdfs.open(fn, 'w', repl=1) as f:
                    f.write(data)

        L =  get_block_locations(hdfs, '/tmp/test/')
        assert len(L) == 6


ip = get_ip()


@gen_cluster([(ip, 1), (ip, 2)], timeout=60)
def test_read_binary(s, a, b):
    with make_hdfs() as hdfs:
        assert hdfs._handle > 0
        data = b'a' * int(1e8)
        fn = '/tmp/test/file'

        with hdfs.open(fn, 'w', repl=1) as f:
            f.write(data)

        blocks = hdfs.get_block_locations(fn)
        assert len(blocks) > 1

        e = Executor((s.ip, s.port), start=False)
        yield e._start()

        futures = read_binary(fn, hdfs=hdfs)
        assert len(futures) == len(blocks)
        assert futures[0].executor is e
        results = yield e._gather(futures)
        assert b''.join(results) == data
        assert s.restrictions
        assert {f.key for f in futures}.issubset(s.loose_restrictions)
