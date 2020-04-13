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


@delayed
def _extra_deps(func, *args, extras=None, **kwargs):
    return func(*args, **kwargs)
