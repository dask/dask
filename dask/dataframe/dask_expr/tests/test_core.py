from __future__ import annotations

import pytest

from dask.dataframe.dask_expr._core import Expr


class ExprB(Expr):
    def _simplify_down(self):
        return ExprA()


class ExprA(Expr):
    def _simplify_down(self):
        return ExprB()


def test_endless_simplify():
    expr = ExprA()
    with pytest.raises(RuntimeError, match="converge"):
        expr.simplify()
