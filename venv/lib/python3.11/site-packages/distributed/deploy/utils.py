from __future__ import annotations

import math

from dask.system import CPU_COUNT


def _factors(n: int) -> set[int]:
    """Return the factors of an integer
    https://stackoverflow.com/a/6800214/616616
    """
    seq = ([i, n // i] for i in range(1, int(pow(n, 0.5) + 1)) if n % i == 0)
    return {j for item in seq for j in item}


def nprocesses_nthreads(n=CPU_COUNT):
    """
    The default breakdown of processes and threads for a given number of cores

    Parameters
    ----------
    n : int
        Number of available cores

    Examples
    --------
    >>> nprocesses_nthreads(4)
    (4, 1)
    >>> nprocesses_nthreads(32)
    (8, 4)

    Returns
    -------
    nprocesses, nthreads
    """
    if n <= 4:
        processes = n
    else:
        processes = min(f for f in _factors(n) if f >= math.sqrt(n))
    threads = n // processes
    return (processes, threads)
