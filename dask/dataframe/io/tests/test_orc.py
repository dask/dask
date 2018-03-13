import os
import pytest
import shutil

from dask.dataframe import read_orc
from dask.dataframe.utils import assert_eq
import dask.dataframe as dd

pytest.importorskip('pyarrow.orc')
url = ('https://github.com/apache/orc/blob/master/examples/'
       'TestOrcFile.testDate1900.orc?raw=true')


@pytest.mark.network
def test_orc_with_backend():
    d = read_orc(url)
    assert d.columns.tolist() == ['time', 'date']
    assert len(d) == 70000


@pytest.fixture()
def orc_files(tmpdir):
    requests = pytest.importorskip('requests')
    data = requests.get(url).content
    d = str(tmpdir)
    files = [os.path.join(d, fn) for fn in ['test1.orc', 'test2.orc']]
    for fn in files:
        with open(fn, 'wb') as f:
            f.write(data)
    try:
        yield files
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_orc_single(orc_files):
    fn = orc_files[0]
    d = read_orc(fn)
    assert len(d) == 70000
    assert d.npartitions == 8
    d2 = read_orc(fn, columns=['time', 'date'])
    assert_eq(d, d2)
    with pytest.raises(ValueError) as e:
        read_orc(fn, columns=['time', 'nonexist'])
    assert 'nonexist' in str(e)


def test_orc_multiple(orc_files):
    d = read_orc(orc_files[0])
    d2 = read_orc(orc_files)
    assert_eq(d2, dd.concat([d, d]), check_index=False)
    d2 = read_orc(os.path.dirname(orc_files[0]) + '/')
    assert_eq(d2, dd.concat([d, d]), check_index=False)
