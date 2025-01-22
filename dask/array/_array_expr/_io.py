from __future__ import annotations

import functools

from dask.array._array_expr._expr import ArrayExpr
from dask.tokenize import _tokenize_deterministic


class IO(ArrayExpr):
    pass


class FromGraph(IO):
    _parameters = ["layer", "_meta", "chunks", "keys", "name_prefix"]

    @functools.cached_property
    def _meta(self):
        return self.operand("_meta")

    def chunks(self):
        return self.operand("chunks")

    @functools.cached_property
    def _name(self):
        return (
            self.operand("name_prefix") + "-" + _tokenize_deterministic(*self.operands)
        )

    def _layer(self):
        dsk = dict(self.operand("layer"))
        # The name may not actually match the layers name therefore rewrite this
        # using an alias
        for part, k in enumerate(self.operand("keys")):
            dsk[(self._name, part)] = k
        return dsk
