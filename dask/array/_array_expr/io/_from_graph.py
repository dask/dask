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
        # using an alias. Use the actual keys from the layer dict since they may
        # differ from self.operand("keys") (e.g., after persist() with optimization).
        #
        # Get expected layer name prefixes from recorded keys.
        # After persist with optimization, the exact token may differ but the
        # prefix (e.g., 'store-map') should match.
        recorded_prefixes = {
            (
                k[0].rsplit("-", 1)[0]
                if isinstance(k, tuple) and isinstance(k[0], str) and "-" in k[0]
                else k[0]
            )
            for k in self.operand("keys")
            if isinstance(k, tuple)
        }

        for k in list(dsk.keys()):
            if not isinstance(k, tuple):
                raise TypeError(f"Expected tuple, got {type(k)}")
            # Only process keys from our layer, skip dependency keys
            # Match by prefix to handle token changes after optimization
            key_prefix = (
                k[0].rsplit("-", 1)[0]
                if isinstance(k[0], str) and "-" in k[0]
                else k[0]
            )
            if key_prefix not in recorded_prefixes:
                continue
            orig = dsk[k]
            if not istask(orig):
                del dsk[k]
                dsk[(self._name, *k[1:])] = orig
            else:
                dsk[(self._name, *k[1:])] = k
        return dsk
