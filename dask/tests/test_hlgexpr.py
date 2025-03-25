from __future__ import annotations

import pickle

from dask._expr import HLGExpr
from dask.tokenize import tokenize


def test_tokenize():
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

    # Roundtrip preserves the tokens
    for expr in [HLGExpr(dsk), HLGExpr(dsk2), HLGExpr(dsk3)]:
        assert tokenize(pickle.loads(pickle.dumps(expr))) == tokenize(expr)
