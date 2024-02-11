from __future__ import annotations

import pytest

from dask.base import normalize_token, tokenize


def test_tokenize_touching_data():
    ak = pytest.importorskip("awkward")

    a = ak.Array([1, 2, 3, 4, 5])
    layout, report = ak.typetracer.typetracer_with_report(a.layout.form_with_key())
    b = ak.Array(layout)
    d = {"key": b}

    _ = normalize_token(d)
    assert len(report.data_touched) == 0 and len(report.shape_touched) == 0

    _ = tokenize(d)
    assert len(report.data_touched) == 0 and len(report.shape_touched) == 0
