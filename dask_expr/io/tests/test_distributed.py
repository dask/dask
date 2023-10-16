from __future__ import annotations

import pytest

from dask_expr.tests._util import _backend_library

distributed = pytest.importorskip("distributed")

from distributed import Client, LocalCluster
from distributed.utils_test import client as c  # noqa F401

import dask_expr as dx

lib = _backend_library()


def test_io_fusion_merge(tmpdir):
    pdf = lib.DataFrame({c: range(100) for c in "abcdefghij"})
    with LocalCluster(processes=False, n_workers=2) as cluster:
        with Client(cluster) as client:  # noqa: F841
            dx.from_pandas(pdf, 10).to_parquet(tmpdir)
            df = dx.read_parquet(tmpdir).merge(
                dx.read_parquet(tmpdir).add_suffix("_x"), left_on="a", right_on="a_x"
            )[["a_x", "b_x", "b"]]
            out = df.compute()
    lib.testing.assert_frame_equal(
        out.sort_values(by="a_x", ignore_index=True),
        pdf.merge(pdf.add_suffix("_x"), left_on="a", right_on="a_x")[
            ["a_x", "b_x", "b"]
        ],
    )
