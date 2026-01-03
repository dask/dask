from __future__ import annotations

import importlib
from itertools import product

import numpy as np

from dask._task_spec import Dict as TaskDict
from dask._task_spec import Task, TaskRef
from dask._task_spec import Tuple as TaskTuple
from dask.array._array_expr.io import IO
from dask.array.backends import array_creation_dispatch
from dask.array.core import broadcast_shapes, normalize_chunks
from dask.tokenize import tokenize
from dask.utils import cached_property, random_state_data, typename

from ._generator import Generator
from ._random_state import RandomState


def _rng_from_bitgen(bitgen):
    # Assumes typename(bitgen) starts with importable
    # library name (e.g. "numpy" or "cupy")
    backend_name = typename(bitgen).split(".")[0]
    backend_lib = importlib.import_module(backend_name)
    return backend_lib.random.default_rng(bitgen)


def _spawn_bitgens(bitgen, n_bitgens):
    seeds = bitgen._seed_seq.spawn(n_bitgens)
    bitgens = [type(bitgen)(seed) for seed in seeds]
    return bitgens


def _apply_random_func(rng, funcname, bitgen, size, args, kwargs):
    """Apply random module method with seed"""
    if isinstance(bitgen, np.random.SeedSequence):
        bitgen = rng(bitgen)
    rng = _rng_from_bitgen(bitgen)
    func = getattr(rng, funcname)
    return func(*args, size=size, **kwargs)


def _apply_random(RandomState, funcname, state_data, size, args, kwargs):
    """Apply RandomState method with seed"""
    if RandomState is None:
        RandomState = array_creation_dispatch.RandomState
    state = RandomState(state_data)
    func = getattr(state, funcname)
    return func(*args, size=size, **kwargs)


class Random(IO):
    _parameters = [
        "rng",
        "distribution",
        "size",
        "chunks",
        "extra_chunks",
        "args",
        "kwargs",
    ]
    _defaults = {"extra_chunks": ()}
    _is_blockwise_fusable = True

    @cached_property
    def kwargs(self):
        return self.operand("kwargs")

    @cached_property
    def _base_chunks(self):
        """Chunks for the size dimensions, excluding extra_chunks."""
        size = self.operand("size")
        chunks = self.operand("chunks")
        shape = broadcast_shapes(size) if size is not None else ()
        return normalize_chunks(
            chunks,
            shape,
            dtype=self.kwargs.get("dtype", np.float64),
        )

    @property
    def chunks(self):
        return self._base_chunks + self.extra_chunks

    @cached_property
    def _info(self):
        sizes = list(product(*self._base_chunks))
        if isinstance(self.rng, Generator):
            bitgens = _spawn_bitgens(self.rng._bit_generator, len(sizes))
            bitgen_token = tokenize(bitgens)
            bitgens = [_bitgen._seed_seq for _bitgen in bitgens]
            func_applier = _apply_random_func
            gen = type(self.rng._bit_generator)
        elif isinstance(self.rng, RandomState):
            bitgens = random_state_data(len(sizes), self.rng._numpy_state)
            bitgen_token = tokenize(bitgens)
            func_applier = _apply_random
            gen = self.rng._RandomState
        else:
            raise TypeError(
                "Unknown object type: Not a Generator and Not a RandomState"
            )
        token = tokenize(bitgen_token, self.size, self.chunks, self.args, self.kwargs)
        name = f"{self.distribution}-{token}"

        return bitgens, name, sizes, gen, func_applier

    @property
    def _name(self):
        return self._info[1]

    @property
    def bitgens(self):
        return self._info[0]

    def _layer(self):
        result = {}
        for block_id in product(*[range(len(bd)) for bd in self.chunks]):
            key = (self._name, *block_id)
            result[key] = self._task(key, block_id)
        return result

    def _block_id_to_flat_index(self, block_id: tuple[int, ...]) -> int:
        """Convert N-D block_id to flat index for bitgens/sizes lookup."""
        # Only use base_chunks dimensions (exclude extra_chunks which are always 0)
        base_block_id = block_id[: len(self._base_chunks)]
        flat_idx = 0
        stride = 1
        for i in reversed(range(len(base_block_id))):
            flat_idx += base_block_id[i] * stride
            stride *= len(self._base_chunks[i])
        return flat_idx

    def _task(self, key, block_id: tuple[int, ...]) -> Task:
        """Generate task for a specific output block."""
        from dask.array._array_expr._expr import ArrayExpr

        bitgens, name, sizes, gen, func_applier = self._info
        flat_idx = self._block_id_to_flat_index(block_id)

        # Convert array expressions to TaskRefs
        task_args = []
        has_array_args = False
        for arg in self.args:
            if isinstance(arg, ArrayExpr):
                # Reference the corresponding block of the dependency
                task_args.append(TaskRef((arg._name,) + block_id))
                has_array_args = True
            else:
                task_args.append(arg)

        task_kwargs = {}
        has_array_kwargs = False
        for k, v in self.kwargs.items():
            if isinstance(v, ArrayExpr):
                task_kwargs[k] = TaskRef((v._name,) + block_id)
                has_array_kwargs = True
            else:
                task_kwargs[k] = v

        # Use TaskTuple/TaskDict to properly track dependencies inside containers
        args_container = TaskTuple(*task_args) if has_array_args else tuple(task_args)
        kwargs_container = TaskDict(task_kwargs) if has_array_kwargs else task_kwargs

        return Task(
            key,
            func_applier,
            gen,
            self.distribution,
            bitgens[flat_idx],
            sizes[flat_idx],
            args_container,
            kwargs_container,
        )

    def dependencies(self):
        """Return array expression dependencies."""
        from dask.array._array_expr._expr import ArrayExpr

        deps = []
        for arg in self.args:
            if isinstance(arg, ArrayExpr):
                deps.append(arg)
        for v in self.kwargs.values():
            if isinstance(v, ArrayExpr):
                deps.append(v)
        return deps

    def _input_block_id(self, dep, block_id: tuple[int, ...]) -> tuple[int, ...]:
        """Map output block_id to input block_id for dependencies."""
        return block_id

    @cached_property
    def _meta(self):
        from dask.array._array_expr._expr import ArrayExpr

        bitgens, name, sizes, gen, func_applier = self._info
        size = self.operand("size")
        meta_size = (0,) * len(size) if size is not None else ()

        # Convert array arguments to scalars for meta computation
        def to_scalar(x):
            if isinstance(x, ArrayExpr):
                # Array expression - use dtype to create a scalar
                return x.dtype.type(1)
            elif isinstance(x, np.ndarray):
                # Numpy array
                return x.flat[0]
            return x

        meta_args = tuple(to_scalar(arg) for arg in self.args)
        meta_kwargs = {k: to_scalar(v) for k, v in self.kwargs.items()}

        return func_applier(
            gen,
            self.distribution,
            bitgens[0],
            meta_size,
            meta_args,
            meta_kwargs,
        )


# Distribution-specific subclasses with explicit parameters
# These avoid storing array dependencies inside tuple operands


class RandomNormal(Random):
    """Normal distribution with explicit loc and scale parameters."""

    _parameters = ["rng", "size", "chunks", "extra_chunks", "loc", "scale"]
    _defaults = {"extra_chunks": (), "loc": 0.0, "scale": 1.0}
    distribution = "normal"

    @property
    def args(self):
        return (self.loc, self.scale)

    @property
    def kwargs(self):
        return {}


class RandomPoisson(Random):
    """Poisson distribution with explicit lam parameter."""

    _parameters = ["rng", "size", "chunks", "extra_chunks", "lam"]
    _defaults = {"extra_chunks": (), "lam": 1.0}
    distribution = "poisson"

    @property
    def args(self):
        return (self.lam,)

    @property
    def kwargs(self):
        return {}
