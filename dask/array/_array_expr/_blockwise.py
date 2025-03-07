from __future__ import annotations

import numbers

import numpy as np
import tlz as toolz

from dask.array._array_expr._expr import ArrayExpr, unify_chunks_expr
from dask.array._array_expr._utils import compute_meta
from dask.array.core import normalize_arg
from dask.array.utils import meta_from_array
from dask.blockwise import _blockwise_unpack_collections_task_spec
from dask.blockwise import blockwise as core_blockwise
from dask.layers import ArrayBlockwiseDep
from dask.tokenize import _tokenize_deterministic
from dask.utils import cached_property, funcname


class Blockwise(ArrayExpr):
    _parameters = [
        "func",
        "out_ind",
        "name",
        "token",
        "dtype",
        "adjust_chunks",
        "new_axes",
        "align_arrays",
        "concatenate",
        "_meta_provided",
        "kwargs",
        "_nan_chunks",
    ]
    _defaults = {
        "name": None,
        "token": None,
        "dtype": None,
        "adjust_chunks": None,
        "new_axes": None,
        "align_arrays": True,
        "concatenate": None,
        "_meta_provided": None,
        "kwargs": None,
        "_nan_chunks": False,
    }

    @cached_property
    def args(self):
        return self.operands[len(self._parameters) :]

    @cached_property
    def _meta_provided(self):
        # We catch recursion errors if key starts with _meta, so define
        # explicitly here
        return self.operand("_meta_provided")

    @cached_property
    def _meta(self):
        if self._meta_provided is not None:
            return self._meta_provided
        else:
            result = compute_meta(
                self.func, self.operand("dtype"), *self.args[::2], **self.kwargs
            )
            if result is None:
                return meta_from_array(
                    None, ndim=self.ndim, dtype=self.operand("dtype")
                )
            return result

    @cached_property
    def dtype(self):
        return super().dtype

    @cached_property
    def chunks(self):
        if self.align_arrays:
            chunkss, arrays, _ = unify_chunks_expr(*self.args)
        else:
            arginds = [
                (a, i) for (a, i) in toolz.partition(2, self.args) if i is not None
            ]
            chunkss = {}
            # For each dimension, use the input chunking that has the most blocks;
            # this will ensure that broadcasting works as expected, and in
            # particular the number of blocks should be correct if the inputs are
            # consistent.
            for arg, ind in arginds:
                for c, i in zip(arg.chunks, ind):
                    if i not in chunkss or len(c) > len(chunkss[i]):
                        chunkss[i] = c

        for k, v in self.new_axes.items():
            if not isinstance(v, tuple):
                v = (v,)
            chunkss[k] = v

        chunks = [chunkss[i] for i in self.out_ind]
        if self.adjust_chunks:
            for i, ind in enumerate(self.out_ind):
                if ind in self.adjust_chunks:
                    if callable(self.adjust_chunks[ind]):
                        chunks[i] = tuple(map(self.adjust_chunks[ind], chunks[i]))
                    elif isinstance(self.adjust_chunks[ind], numbers.Integral):
                        chunks[i] = tuple(self.adjust_chunks[ind] for _ in chunks[i])
                    elif isinstance(self.adjust_chunks[ind], (tuple, list)):
                        if len(self.adjust_chunks[ind]) != len(chunks[i]):
                            raise ValueError(
                                f"Dimension {i} has {len(chunks[i])} blocks, adjust_chunks "
                                f"specified with {len(self.adjust_chunks[ind])} blocks"
                            )
                        chunks[i] = tuple(self.adjust_chunks[ind])
                    else:
                        raise NotImplementedError(
                            "adjust_chunks values must be callable, int, or tuple"
                        )
            chunks = tuple(chunks)
        if self._nan_chunks:
            return tuple((np.nan,) * len(c) for c in chunks)
        return tuple(map(tuple, chunks))

    @cached_property
    def dtype(self):
        return self.operand("dtype")

    @property
    def deterministic_token(self):
        if not self._determ_token:
            # TODO: Is there an actual need to overwrite this?
            self._determ_token = _tokenize_deterministic(
                self.func, self.out_ind, self.dtype, *self.args, **self.kwargs
            )
        return self._determ_token

    @cached_property
    def _name(self):
        if "name" in self._parameters and self.operand("name"):
            return self.operand("name")
        else:
            return (
                f"{self.token or funcname(self.func).strip('_')}-"
                + self.deterministic_token
            )

    def _layer(self):
        arginds = [(a, i) for (a, i) in toolz.partition(2, self.args)]

        numblocks = {}
        dependencies = []
        arrays = []

        # Normalize arguments
        argindsstr = []

        for arg, ind in arginds:
            if ind is None:
                arg = normalize_arg(arg)
                arg, collections = _blockwise_unpack_collections_task_spec(arg)
                dependencies.extend(collections)
            else:
                if (
                    hasattr(arg, "ndim")
                    and hasattr(ind, "__len__")
                    and arg.ndim != len(ind)
                ):
                    raise ValueError(
                        "Index string %s does not match array dimension %d"
                        % (ind, arg.ndim)
                    )
                # TODO(expr): this class is a confusing crutch to pass arguments to the
                #  graph, we should write them directly into the graph
                if not isinstance(arg, ArrayBlockwiseDep):
                    numblocks[arg.name] = arg.numblocks
                    arrays.append(arg)
                    arg = arg.name
            argindsstr.extend((arg, ind))

        # Normalize keyword arguments
        kwargs2 = {}
        for k, v in self.kwargs.items():
            v = normalize_arg(v)
            v, collections = _blockwise_unpack_collections_task_spec(v)
            dependencies.extend(collections)
            kwargs2[k] = v

        # TODO(expr): Highlevelgraph :(
        graph = core_blockwise(
            self.func,
            self._name,
            self.out_ind,
            *argindsstr,
            numblocks=numblocks,
            dependencies=dependencies,
            new_axes=self.new_axes,
            concatenate=self.concatenate,
            **kwargs2,
        )
        return dict(graph)

    def _lower(self):
        if self.align_arrays:
            _, arrays, changed = unify_chunks_expr(*self.args)
            if changed:
                args = []
                for idx, arr in zip(self.args[1::2], arrays):
                    args.extend([arr, idx])
                return type(self)(*self.operands[: len(self._parameters)], *args)


class Transpose(Blockwise):
    _parameters = ["array", "axes"]
    func = staticmethod(np.transpose)
    align_arrays = False
    adjust_chunks = None
    concatenate = None
    token = "transpose"
    _nan_chunks = False

    @property
    def new_axes(self):
        return {}

    @property
    def name(self):
        return self._name

    @property
    def _meta_provided(self):
        return self.array._meta

    @property
    def dtype(self):
        return self._meta.dtype

    @property
    def out_ind(self):
        return self.axes

    @property
    def kwargs(self):
        return {"axes": self.axes}

    @property
    def args(self):
        return (self.array, tuple(range(self.array.ndim)))
