"""
Dispatch in dask.array.

Also see backends.py
"""

from ..utils import Dispatch

concatenate_lookup = Dispatch("concatenate")
tensordot_lookup = Dispatch("tensordot")
einsum_lookup = Dispatch("einsum")
empty_lookup = Dispatch("empty")
divide_lookup = Dispatch("divide")
percentile_dispatch = Dispatch("percentile_dispatch")


def _percentile(a, q, interpolation="linear"):
    return percentile_dispatch(a, q, interpolation)
