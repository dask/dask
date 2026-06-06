"""Tests for XSS prevention via Jinja2 autoescape."""

from __future__ import annotations

import pytest

jinja2 = pytest.importorskip("jinja2")

from dask.widgets import get_template


def test_highlevelgraph_layer_escapes_user_input():
    """Verify that user-controlled strings are HTML-escaped."""
    template = get_template("highlevelgraph_layer.html.j2")
    payload = "<script>alert(1)</script>"
    rendered = template.render(
        materialized=True,
        shortname=payload,
        layer_index=0,
        highlevelgraph_key=payload,
        info={payload: payload},
        dependencies=[payload],
        svg_repr="",
    )
    assert payload not in rendered
    assert "&lt;script&gt;" in rendered


def test_dataframe_escapes_name():
    """Verify that dataframe names are HTML-escaped."""
    template = get_template("dataframe.html.j2")
    payload = "<script>alert(1)</script>"
    rendered = template.render(
        data="<table></table>",
        name=payload,
        layers="1 expression",
    )
    assert payload not in rendered
    assert "script&gt;alert(1)&lt;/script" in rendered
