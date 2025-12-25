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
        layer = self.operand("layer")
        our_keys = set(self.operand("keys"))
        is_hlg = hasattr(layer, "layers")

        # Persist case: layer is a dict of computed values with potentially
        # different keys (optimization can change key names). Just rename.
        if not is_hlg:
            layer_keys = {k for k in layer if isinstance(k, tuple)}
            if layer_keys and not (layer_keys & our_keys):
                return {
                    (self._name, *k[1:]) if isinstance(k, tuple) else k: v
                    for k, v in layer.items()
                }

        # HLG case (e.g., from BlockView): contains tasks and dependencies.
        # Rename output keys and preserve dependency structure.
        dsk = dict(layer)
        result = {}
        for k, v in dsk.items():
            if k in our_keys:
                new_key = (self._name, *k[1:])
                if istask(v):
                    result[new_key] = k  # Alias to original
                    result[k] = v  # Keep original task
                else:
                    result[new_key] = v  # Simple rename
            else:
                result[k] = v  # Dependency - keep as-is
        return result
