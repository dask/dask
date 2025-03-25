from __future__ import annotations

import pytest

from dask._expr import Expr, HLGExpr
from dask.tokenize import tokenize


class MyExpr(Expr):
    _parameters = ["foo", "bar"]


class MyExpr2(MyExpr):
    # A subclass that inherits parameters
    pass


def test_setattr():
    e = MyExpr(foo=1, bar=2)
    e.bar = 3
    assert e.bar == 3
    with pytest.raises(AttributeError):
        e.baz = 4


def test_setattr2():
    e = MyExpr2(foo=1, bar=2)
    e.bar = 3
    assert e.bar == 3
    with pytest.raises(AttributeError):
        e.baz = 4


def test_hlgexpr():
    # Ensure tokens are different for different high-level graphs The current
    # implementation actually ensures that no HLGExpr are tokenizing equally.
    # Technically, we do not need such a strong guarantee. but tokenizing a full
    # HLG reliably is tricky and we do not require the reproducibility for
    # HLGExpr since they do not undergo the same kind of optimization as the
    # rest of the graph.
    from dask.highlevelgraph import HighLevelGraph

    dsk = HighLevelGraph.from_collections("x", {"foo": None})
    dsk2 = HighLevelGraph.from_collections("x", {"bar": None})
    dsk3 = HighLevelGraph.from_collections("y", {"foo": None})
    assert tokenize(HLGExpr(dsk)) != tokenize(HLGExpr(dsk2))
    assert tokenize(HLGExpr(dsk)) != tokenize(HLGExpr(dsk3))
    assert tokenize(HLGExpr(dsk2)) != tokenize(HLGExpr(dsk3))
