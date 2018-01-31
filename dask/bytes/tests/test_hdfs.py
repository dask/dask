from __future__ import print_function, division, absolute_import

import os
import posixpath
from collections import namedtuple
from distutils.version import LooseVersion

import pytest
from toolz import concat

import dask
from dask.bytes.core import read_bytes, open_files, get_fs
from dask.compatibility import unicode

try:
    import hdfs3
except ImportError:
    hdfs3 = None

try:
    import pyarrow
    from dask.bytes.pyarrow import _MIN_PYARROW_VERSION_SUPPORTED
    PYARROW_DRIVER = LooseVersion(pyarrow.__version__) >= _MIN_PYARROW_VERSION_SUPPORTED
except ImportError:
    PYARROW_DRIVER = False
    pyarrow = None


if not os.environ.get('DASK_RUN_HDFS_TESTS', ''):
    pytestmark = pytest.mark.skip(reason="HDFS tests not configured to run")


basedir = '/tmp/test-dask'


HDFSDriver = namedtuple('HDFSDriver', ['hdfs', 'name'])


def make_fixture(params):
    @pytest.fixture(params=params)
    def _(request):
        if request.param == 'hdfs3':
            hdfs = hdfs3.HDFileSystem(host='localhost', port=8020)
        else:
            hdfs = pyarrow.hdfs.connect(host='localhost', port=8020)

        if hdfs.exists(basedir):
            hdfs.rm(basedir, recursive=True)
        hdfs.mkdir(basedir)

        yield HDFSDriver(hdfs, request.param)

        if hdfs.exists(basedir):
            hdfs.rm(basedir, recursive=True)
    return _


# These fixtures check for a minimum pyarrow version
driver = make_fixture([pytest.mark.skipif(not hdfs3, 'hdfs3',
                                          reason='hdfs3 not found'),
                       pytest.mark.skipif(not PYARROW_DRIVER, 'pyarrow',
                                          reason='required pyarrow version not found')])

pa_hdfs = make_fixture([pytest.mark.skipif(not PYARROW_DRIVER, 'pyarrow',
                                           reason='required pyarrow version not found')])

# This mark doesn't check the minimum pyarrow version.
require_pyarrow = pytest.mark.skipif(not pyarrow, reason="pyarrow not installed")
require_hdfs3 = pytest.mark.skipif(not hdfs3, reason="hdfs3 not installed")


@require_pyarrow
@require_hdfs3
def test_fs_driver_backends():
    from dask.bytes.hdfs3 import HDFS3HadoopFileSystem
    from dask.bytes.pyarrow import PyArrowHadoopFileSystem

    fs1, token1 = get_fs('hdfs')
    assert isinstance(fs1, HDFS3HadoopFileSystem)

    with dask.set_options(hdfs_driver='pyarrow'):
        fs2, token2 = get_fs('hdfs')
    assert isinstance(fs2, PyArrowHadoopFileSystem)

    assert token1 != token2

    with pytest.raises(ValueError):
        with dask.set_options(hdfs_driver='not-a-valid-driver'):
            get_fs('hdfs')


def test_read_bytes(driver):
    nfiles = 10

    data = b'a' * int(1e3)

    for fn in ['%s/file.%d' % (basedir, i) for i in range(nfiles)]:
        with driver.hdfs.open(fn, 'wb', replication=1) as f:
            f.write(data)

    with dask.set_options(hdfs_driver=driver.name):
        sample, values = read_bytes('hdfs://%s/file.*' % basedir)

    (results,) = dask.compute(values)
    assert [b''.join(r) for r in results] == nfiles * [data]


def test_read_bytes_URL(driver):
    nfiles = 10
    data = b'a' * int(1e3)

    for fn in ['%s/file.%d' % (basedir, i) for i in range(nfiles)]:
        with driver.hdfs.open(fn, 'wb', replication=1) as f:
            f.write(data)

    path = 'hdfs://localhost:8020%s/file.*' % basedir
    with dask.set_options(hdfs_driver=driver.name):
        sample, values = read_bytes(path)

    (results,) = dask.compute(values)
    assert [b''.join(r) for r in results] == nfiles * [data]


def test_read_bytes_big_file(driver):
    fn = '%s/file' % basedir

    # Write 100 MB file
    nblocks = int(1e3)
    blocksize = int(1e5)
    data = b'a' * blocksize
    with driver.hdfs.open(fn, 'wb', replication=1) as f:
        for i in range(nblocks):
            f.write(data)

    with dask.set_options(hdfs_driver=driver.name):
        sample, values = read_bytes('hdfs://' + fn, blocksize=blocksize)

    assert sample[:5] == b'aaaaa'
    assert len(values[0]) == nblocks

    (results,) = dask.compute(values[0])
    assert sum(map(len, results)) == nblocks * blocksize
    for r in results:
        assert set(r.decode('utf-8')) == {'a'}


def test_deterministic_key_names(driver):
    data = b'abc\n' * int(1e3)
    fn = '%s/file' % basedir

    with driver.hdfs.open(fn, 'wb', replication=1) as fil:
        fil.write(data)

    with dask.set_options(hdfs_driver=driver.name):
        _, x = read_bytes('hdfs://%s/*' % basedir, delimiter=b'\n', sample=False)
        _, y = read_bytes('hdfs://%s/*' % basedir, delimiter=b'\n', sample=False)
        _, z = read_bytes('hdfs://%s/*' % basedir, delimiter=b'c', sample=False)

    assert [f.key for f in concat(x)] == [f.key for f in concat(y)]
    assert [f.key for f in concat(x)] != [f.key for f in concat(z)]


def test_open_files_write(driver):
    path = 'hdfs://%s/' % basedir
    data = [b'test data %i' % i for i in range(5)]

    with dask.set_options(hdfs_driver=driver.name):
        files = open_files(path, num=len(data), mode='wb')
    for fil, b in zip(files, data):
        with fil as f:
            f.write(b)

    with dask.set_options(hdfs_driver=driver.name):
        sample, vals = read_bytes('hdfs://%s/*.part' % basedir)

    (results,) = dask.compute(list(concat(vals)))
    assert data == results


def test_read_csv(driver):
    dd = pytest.importorskip('dask.dataframe')

    with driver.hdfs.open('%s/1.csv' % basedir, 'wb') as f:
        f.write(b'name,amount,id\nAlice,100,1\nBob,200,2')

    with driver.hdfs.open('%s/2.csv' % basedir, 'wb') as f:
        f.write(b'name,amount,id\nCharlie,300,3\nDennis,400,4')

    with dask.set_options(hdfs_driver=driver.name):
        df = dd.read_csv('hdfs://%s/*.csv' % basedir)

    assert isinstance(df, dd.DataFrame)
    assert df.id.sum() == 1 + 2 + 3 + 4


def test_read_text(driver):
    db = pytest.importorskip('dask.bag')

    with driver.hdfs.open('%s/text.1.txt' % basedir, 'wb') as f:
        f.write('Alice 100\nBob 200\nCharlie 300'.encode())

    with driver.hdfs.open('%s/text.2.txt' % basedir, 'wb') as f:
        f.write('Dan 400\nEdith 500\nFrank 600'.encode())

    with dask.set_options(hdfs_driver=driver.name, get=dask.get):
        b = db.read_text('hdfs://%s/text.*.txt' % basedir)
        result = b.str.strip().str.split().map(len).compute()

    assert result == [2, 2, 2, 2, 2, 2]

    with driver.hdfs.open('%s/other.txt' % basedir, 'wb') as f:
        f.write('a b\nc d'.encode())

    with dask.set_options(hdfs_driver=driver.name, get=dask.get):
        b = db.read_text('hdfs://%s/other.txt' % basedir)
        result = b.str.split().flatten().compute()

    assert result == ['a', 'b', 'c', 'd']


def test_read_text_unicode(driver):
    db = pytest.importorskip('dask.bag')

    data = b'abcd\xc3\xa9'
    fn = '%s/data.txt' % basedir
    with driver.hdfs.open(fn, 'wb') as f:
        f.write(b'\n'.join([data, data]))

    with dask.set_options(hdfs_driver=driver.name):
        f = db.read_text('hdfs://' + fn, collection=False)

    result = f[0].compute()
    assert len(result) == 2
    assert list(map(unicode.strip, result)) == [data.decode('utf-8')] * 2
    assert len(result[0].strip()) == 5


@require_pyarrow
@require_hdfs3
def test_pyarrow_compat():
    from dask.bytes.hdfs3 import HDFS3HadoopFileSystem
    dhdfs = HDFS3HadoopFileSystem()
    pa_hdfs = dhdfs._get_pyarrow_filesystem()
    assert isinstance(pa_hdfs, pyarrow.filesystem.FileSystem)


@require_pyarrow
def test_parquet_pyarrow(driver):
    dd = pytest.importorskip('dask.dataframe')
    import pandas as pd
    import numpy as np

    fn = '%s/test.parquet' % basedir
    hdfs_fn = 'hdfs://%s' % fn
    df = pd.DataFrame(np.random.normal(size=(1000, 4)),
                      columns=list('abcd'))
    ddf = dd.from_pandas(df, npartitions=4)

    with dask.set_options(hdfs_driver=driver.name):
        ddf.to_parquet(hdfs_fn, engine='pyarrow')

    assert len(driver.hdfs.ls(fn))  # Files are written

    with dask.set_options(hdfs_driver=driver.name):
        ddf2 = dd.read_parquet(hdfs_fn, engine='pyarrow')

    assert len(ddf2) == 1000  # smoke test on read


def test_pyarrow_glob(pa_hdfs):
    from dask.bytes.pyarrow import PyArrowHadoopFileSystem
    hdfs = PyArrowHadoopFileSystem.from_pyarrow(pa_hdfs.hdfs)

    tree = {basedir: (['c', 'c2'], ['a', 'a1', 'a2', 'a3', 'b1']),
            basedir + '/c': (['d'], ['x1', 'x2']),
            basedir + '/c2': (['d'], ['x1', 'x2']),
            basedir + '/c/d': ([], ['x3'])}

    hdfs.mkdirs(basedir + '/c/d/')
    for fn in (posixpath.join(dirname, f)
               for (dirname, (_, fils)) in tree.items()
               for f in fils):
        with hdfs.open(fn, mode='wb') as f2:
            f2.write(b'000')

    assert (set(hdfs.glob(basedir + '/a*')) ==
            {basedir + p for p in ['/a', '/a1', '/a2', '/a3']})

    assert (set(hdfs.glob(basedir + '/c/*')) ==
            {basedir + p for p in ['/c/x1', '/c/x2', '/c/d']})

    assert (set(hdfs.glob(basedir + '/*/x*')) ==
            {basedir + p for p in ['/c/x1', '/c/x2', '/c2/x1', '/c2/x2']})
    assert (set(hdfs.glob(basedir + '/*/x1')) ==
            {basedir + p for p in ['/c/x1', '/c2/x1']})

    assert hdfs.glob(basedir + '/c') == [basedir + '/c']
    assert hdfs.glob(basedir + '/c/') == [basedir + '/c/']
    assert hdfs.glob(basedir + '/a') == [basedir + '/a']

    assert hdfs.glob('/this-path-doesnt-exist') == []
    assert hdfs.glob(basedir + '/missing/') == []
    assert hdfs.glob(basedir + '/missing/x1') == []
    assert hdfs.glob(basedir + '/missing/*') == []

    assert (set(hdfs.glob(basedir + '/*')) ==
            {basedir + p for p in ['/a', '/a1', '/a2', '/a3', '/b1', '/c', '/c2']})
