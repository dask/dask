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
        # Build set of keys that belong to our layer (the recorded output keys)
        our_keys = set(self.operand("keys"))

        result = {}
        for k in list(dsk.keys()):
            if not isinstance(k, tuple):
                raise TypeError(f"Expected tuple, got {type(k)}")
            orig = dsk[k]
            if k in our_keys:
                # This is one of our output keys - rename to use self._name
                if not istask(orig):
                    # Simple alias (e.g., blocks -> arange key)
                    result[(self._name, *k[1:])] = orig
                else:
                    # Task - create alias and keep original task
                    result[(self._name, *k[1:])] = k
                    result[k] = orig
            else:
                # Dependency key - keep as-is without renaming
                result[k] = orig
        return result
