"""Tests for expression tree visualization."""

import pytest

pytest.importorskip("rich")


def test_array_expr_repr_html():
    """Array expr._repr_html_() shows expression tree."""
    da = pytest.importorskip("dask.array")
    if not hasattr(da.ones((2,), chunks=1), "expr"):
        pytest.skip("array query-planning not enabled")
    x = da.ones((10, 10), chunks=5) + 1
    html = x.expr._repr_html_()
    assert "Operation" in html
    assert "Shape" in html
    assert "Add" in html or "add" in html.lower()


def test_array_expr_pprint(capsys):
    """Array expr.pprint() outputs expression tree."""
    da = pytest.importorskip("dask.array")
    if not hasattr(da.ones((2,), chunks=1), "expr"):
        pytest.skip("array query-planning not enabled")
    x = da.ones((10, 10), chunks=5) + 1
    x.expr.pprint()
    captured = capsys.readouterr()
    assert "Operation" in captured.out
    assert "Add" in captured.out or "add" in captured.out.lower()


def test_dataframe_collection_repr_html():
    """DataFrame collection._repr_html_() shows data preview, not expr tree."""
    pd = pytest.importorskip("pandas")
    dd = pytest.importorskip("dask.dataframe")
    df = dd.from_pandas(pd.DataFrame({"x": [1, 2, 3]}), npartitions=2)
    html = df._repr_html_()
    # Should show data structure, not expression tree
    assert "Dask DataFrame Structure" in html
    # Should NOT show expression tree format
    assert "Parts" not in html


def test_dataframe_expr_repr_html():
    """DataFrame expr._repr_html_() shows expression tree."""
    pd = pytest.importorskip("pandas")
    dd = pytest.importorskip("dask.dataframe")
    df = dd.from_pandas(pd.DataFrame({"x": [1, 2, 3]}), npartitions=2)
    html = df.expr._repr_html_()
    assert "Operation" in html
    assert "Parts" in html
    assert "FromPandas" in html


def test_dataframe_expr_pprint(capsys):
    """DataFrame expr.pprint() outputs expression tree."""
    pd = pytest.importorskip("pandas")
    dd = pytest.importorskip("dask.dataframe")
    df = dd.from_pandas(pd.DataFrame({"x": [1, 2, 3]}), npartitions=2)
    df.expr.pprint()
    captured = capsys.readouterr()
    assert "Operation" in captured.out
    assert "FromPandas" in captured.out
