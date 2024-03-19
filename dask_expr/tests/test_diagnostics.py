from __future__ import annotations

import os

import pytest
from distributed.utils_test import *  # noqa

from dask_expr import from_pandas
from dask_expr.tests._util import _backend_library

# Set DataFrame backend for this module
pd = _backend_library()


@pytest.fixture
def pdf():
    pdf = pd.DataFrame({"x": range(100)})
    pdf["y"] = pdf.x // 7  # Not unique; duplicates span different partitions
    yield pdf


@pytest.fixture
def df(pdf):
    yield from_pandas(pdf, npartitions=10)


def test_analyze(df, client, tmpdir):
    pytest.importorskip("crick")
    filename = str(tmpdir / "analyze")
    expr = df.groupby(df.columns[1]).apply(lambda x: x)
    digraph = expr.analyze(filename=filename)
    assert os.path.exists(filename + ".svg")
    for exp in expr.optimize().walk():
        assert any(exp._name in el for el in digraph.body)
