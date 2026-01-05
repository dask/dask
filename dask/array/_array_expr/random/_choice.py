from __future__ import annotations

from itertools import product
from numbers import Integral

import numpy as np

from dask._task_spec import TaskRef
from dask.array._array_expr._collection import Array
from dask.array._array_expr.core._conversion import asarray
from dask.array._array_expr.io import IO
from dask.array.backends import array_creation_dispatch
from dask.array.core import normalize_chunks
from dask.array.utils import asarray_safe
from dask.utils import cached_property, random_state_data

from ._expr import _spawn_bitgens
from ._generator import Generator
from ._random_state import RandomState


def _choice_rng(state_data, a, size, replace, p, axis, shuffle):
    from ._expr import _rng_from_bitgen

    state = _rng_from_bitgen(state_data)
    return state.choice(a, size=size, replace=replace, p=p, axis=axis, shuffle=shuffle)


def _choice_rs(state_data, a, size, replace, p):
    state = array_creation_dispatch.RandomState(state_data)
    return state.choice(a, size=size, replace=replace, p=p)


def _choice_validate_params(state, a, size, replace, p, axis, chunks):
    """Validate and normalize parameters for choice.

    Returns expressions for array/p (or int/None) so they participate in lowering.
    """
    # Normalize and validate `a`
    if isinstance(a, Integral):
        if isinstance(state, Generator):
            if state._backend_name == "cupy":
                raise NotImplementedError(
                    "`choice` not supported for cupy-backed `Generator`."
                )
            meta = state._backend.random.default_rng().choice(1, size=(), p=None)
        elif isinstance(state, RandomState):
            # On windows the output dtype differs if p is provided or
            # # absent, see https://github.com/numpy/numpy/issues/9867
            dummy_p = state._backend.array([1]) if p is not None else p
            meta = state._backend.random.RandomState().choice(1, size=(), p=dummy_p)
        else:
            raise ValueError("Unknown generator class")
        len_a = a
        if a < 0:
            raise ValueError("a must be greater than 0")
        a_expr = None  # No expression for int a
    else:
        a = asarray(a)
        a = a.rechunk(a.shape)
        meta = a._meta
        if a.ndim != 1:
            raise ValueError("a must be one dimensional")
        len_a = len(a)
        a_expr = a.expr  # Store expression so it gets lowered

    # Normalize and validate `p`
    p_expr = None
    if p is not None:
        if not isinstance(p, Array):
            # If p is not a dask array, first check the sum is close
            # to 1 before converting.
            p = asarray_safe(p, like=p)
            if not np.isclose(p.sum(), 1, rtol=1e-7, atol=0):
                raise ValueError("probabilities do not sum to 1")
            p = asarray(p)
        else:
            p = p.rechunk(p.shape)

        if p.ndim != 1:
            raise ValueError("p must be one dimensional")
        if len(p) != len_a:
            raise ValueError("a and p must have the same size")

        p_expr = p.expr  # Store expression so it gets lowered

    if size is None:
        size = ()
    elif not isinstance(size, (tuple, list)):
        size = (size,)

    if axis != 0:
        raise ValueError("axis must be 0 since a is one dimensional")

    chunks = normalize_chunks(chunks, size, dtype=np.float64)
    if not replace and len(chunks[0]) > 1:
        err_msg = (
            "replace=False is not currently supported for "
            "dask.array.choice with multi-chunk output "
            "arrays"
        )
        raise NotImplementedError(err_msg)

    # For int a, return the int value; for array a, return None (use a_expr)
    a_val = a if isinstance(a, Integral) else None
    return a_val, a_expr, size, replace, p_expr, axis, chunks, meta


class RandomChoice(IO):
    _parameters = [
        "a_val",  # int value of a (or None if a is an array)
        "a_expr",  # expression for a (or None if a is an int)
        "chunks",
        "_meta",
        "_state",
        "replace",
        "p_expr",  # expression for p (or None)
        "axis",
        "shuffle",
    ]
    _defaults = {"axis": None, "shuffle": None}
    _funcname = "da.random.choice-"

    @cached_property
    def chunks(self):
        return self.operand("chunks")

    @cached_property
    def sizes(self):
        return list(product(*self.chunks))

    @cached_property
    def state_data(self):
        return random_state_data(len(self.sizes), self._state)

    @cached_property
    def _meta(self):
        return self.operand("_meta")

    # No custom dependencies() needed - base class finds Expr operands automatically
    # (a_expr and p_expr are included when they're expressions, excluded when None)

    @property
    def _a_arg(self):
        """Value to pass to choice: int or TaskRef to single-chunk array."""
        if self.a_val is not None:
            return self.a_val
        return TaskRef((self.a_expr._name, 0))

    @property
    def _p_arg(self):
        """Value to pass to choice: None or TaskRef to single-chunk array."""
        if self.p_expr is None:
            return None
        return TaskRef((self.p_expr._name, 0))

    def _layer(self) -> dict:
        keys = product([self._name], *[range(len(bd)) for bd in self.chunks])
        return {
            k: (_choice_rs, state, self._a_arg, size, self.replace, self._p_arg)
            for k, state, size in zip(keys, self.state_data, self.sizes)
        }


class RandomChoiceGenerator(RandomChoice):
    # Keep axis and shuffle as required parameters (no defaults)
    _defaults = {}

    @cached_property
    def state_data(self):
        return _spawn_bitgens(self._state, len(self.sizes))

    def _layer(self) -> dict:
        keys = product([self._name], *[range(len(bd)) for bd in self.chunks])
        return {
            k: (
                _choice_rng,
                bitgen,
                self._a_arg,
                size,
                self.replace,
                self._p_arg,
                self.axis,
                self.shuffle,
            )
            for k, bitgen, size in zip(keys, self.state_data, self.sizes)
        }
