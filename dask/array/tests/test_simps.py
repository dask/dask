from __future__ import annotations

import numpy as np
import pytest

import dask.array as da
from dask.array.utils import assert_eq

pytest.importorskip("scipy", minversion="1.11")

from scipy.integrate import simpson as sp_simps


def _close(dask_val, true_val):
    assert_eq(dask_val, true_val)


@pytest.mark.parametrize("n", [9, 10, 11, 12])
def test_uniform_dx(n):
    rng = np.random.default_rng(0)
    y = rng.random(n)
    dx = 0.2
    darr = da.from_array(y, chunks=4)
    _close(da.simpson(darr, dx=dx), sp_simps(y, x=None, dx=dx))


def test_irregular_x():
    rng = np.random.default_rng(1)
    n = 15
    y = rng.random(n)
    x = np.cumsum(rng.random(n))
    darr, xarr = map(lambda a: da.from_array(a, chunks=5), (y, x))
    _close(da.simpson(darr, x=xarr), sp_simps(y, x=x))


@pytest.mark.parametrize("policy", ["avg", "first", "last"])
def test_even_policies(policy):
    y = np.linspace(0, 1, 12)
    darr = da.from_array(y, chunks=6)
    _close(da.simps(darr, dx=0.1, even=policy), sp_simps(y, dx=0.1, even=policy))
    _close(da.simpson(darr, dx=0.1, even=policy), sp_simps(y, dx=0.1, even=policy))
