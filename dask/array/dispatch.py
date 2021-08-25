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
percentile_lookup = Dispatch("percentile")


# def _percentile(a, q, interpolation="linear"):
#    return percentile_lookup(a, q, interpolation)
