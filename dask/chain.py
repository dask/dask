from operator import add

from .delayed import delayed


def _sequence(a, b, combine=None):
    """Given two delayeds, return a new delayed that combines their values but is guaranteed to execute them in sequence
    (not in parallel)"""
    combine = combine or add  # default to __add__

    # Helper which receives a computed `a`, and in turn computes `b`
    def _combine(_a):
        return combine(_a, b.compute())

    name = f"combine_{b._key}"
    return delayed(_combine)(a, dask_key_name=name)


def _chain(delayeds):
    """Construct a Delayed representing a list of delayed values, but with linear dependency graph (the constituent
    Delayeds are evaluated, in serial, in the order they appear in the `delayeds` parameter."""
    return _chain_rec(delayeds, init=[], combine=lambda l, r: l + [r], first=True,)


def _chain_rec(delayeds, init=None, combine=None, first=False):
    if len(delayeds) == 0:
        return delayeds
    delayeds = list(delayeds)
    if first:
        delayeds = [delayed(init, "init")] + delayeds
    elif len(delayeds) == 1:
        return delayeds[0]

    (_first, _second, *rest) = delayeds
    return _chain_rec(
        [_sequence(_first, _second, combine=combine,)] + rest, combine=combine,
    )
