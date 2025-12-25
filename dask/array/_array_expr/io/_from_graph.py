from __future__ import annotations

import functools

from dask import istask
from dask.array._array_expr._expr import ArrayExpr


class FromGraph(ArrayExpr):
    _parameters = ["layer", "_meta", "chunks", "keys", "name_prefix"]

    @functools.cached_property
    def _meta(self):
        return self.operand("_meta")

    @functools.cached_property
    def chunks(self):
        return self.operand("chunks")

    @functools.cached_property
    def _name(self):
        return self.operand("name_prefix") + "-" + self.deterministic_token

    def _layer(self):
        dsk = dict(self.operand("layer"))
        # The name may not actually match the layer's name therefore rewrite this
        # using an alias. After persist() with optimization/fusion, layer keys
        # may have completely different names (e.g., 'store-add-ones' instead of
        # 'store-map'). Rename all keys to use self._name since the layer only
        # contains this expression's computed data.
        for k in list(dsk.keys()):
            if not isinstance(k, tuple):
                raise TypeError(f"Expected tuple, got {type(k)}")
            orig = dsk[k]
            if not istask(orig):
                del dsk[k]
                dsk[(self._name, *k[1:])] = orig
            else:
                dsk[(self._name, *k[1:])] = k
        return dsk
