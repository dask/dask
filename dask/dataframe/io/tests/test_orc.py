import os
import shutil
import tempfile
from distutils.version import LooseVersion

import pytest

from dask.dataframe import read_orc
from dask.dataframe.utils import assert_eq
import dask.dataframe as dd
from dask.dataframe.optimize import optimize_dataframe_getitem

pytest.importorskip("pyarrow.orc")

# Skip for broken ORC reader
import pyarrow as pa

pytestmark = pytest.mark.skipif(
    LooseVersion(pa.__version__) == "0.10.0",
    reason=(
        "PyArrow 0.10.0 release broke the ORC reader, see "
        "https://issues.apache.org/jira/browse/ARROW-3009"
    ),
)


url = (
    "https://www.googleapis.com/download/storage/v1/b/anaconda-public-data/o"
    "/orc%2FTestOrcFile.testDate1900.orc?generation=1522611448751555&alt="
    "media"
)
columns = ["time", "date"]


@pytest.mark.network
def test_orc_with_backend():
    pytest.importorskip("requests")
    d = read_orc(url)
    assert set(d.columns) == {"time", "date"}  # order is not guaranteed
    assert len(d) == 70000


@pytest.fixture(scope="module")
def orc_files():
    requests = pytest.importorskip("requests")
    data = requests.get(url).content
    d = tempfile.mkdtemp()
    files = [os.path.join(d, fn) for fn in ["test1.orc", "test2.orc"]]
    for fn in files:
        with open(fn, "wb") as f:
            f.write(data)
    try:
        yield files
    finally:
        shutil.rmtree(d, ignore_errors=True)


def test_orc_single(orc_files):
    fn = orc_files[0]
    d = read_orc(fn)
    assert len(d) == 70000
    assert d.npartitions == 8
    d2 = read_orc(fn, columns=["time", "date"])
    assert_eq(d[columns], d2[columns])
    with pytest.raises(ValueError, match="nonexist"):
        read_orc(fn, columns=["time", "nonexist"])

    # Check that `optimize_dataframe_getitem` changes the
    # `columns` attribute of the "read-orc" layer
    d3 = d[columns]
    keys = [(d3._name, i) for i in range(d3.npartitions)]
    graph = optimize_dataframe_getitem(d3.__dask_graph__(), keys)
    key = [k for k in graph.layers.keys() if k.startswith("subset-read-orc-")][0]
    assert set(graph.layers[key].columns) == set(columns)


def test_orc_multiple(orc_files):
    d = read_orc(orc_files[0])
    d2 = read_orc(orc_files)
    assert_eq(d2[columns], dd.concat([d, d])[columns], check_index=False)
    d2 = read_orc(os.path.dirname(orc_files[0]) + "/*.orc")
    assert_eq(d2[columns], dd.concat([d, d])[columns], check_index=False)
