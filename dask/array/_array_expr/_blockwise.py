from __future__ import annotations

import numbers
from collections.abc import Iterable

import numpy as np
import tlz as toolz

from dask import is_dask_collection
from dask.array._array_expr._expr import ArrayExpr, unify_chunks_expr
from dask.array._array_expr._utils import compute_meta
from dask.array.core import (
    _elemwise_handle_where,
    _enforce_dtype,
    apply_infer_dtype,
    broadcast_shapes,
    is_scalar_for_elemwise,
    normalize_arg,
)
from dask.array.utils import meta_from_array
from dask.blockwise import blockwise as core_blockwise
from dask.delayed import unpack_collections
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
            # Handle tuple metas for multi-output functions (e.g., from apply_gufunc)
            if isinstance(self._meta_provided, (tuple, list)):
                return tuple(
                    meta_from_array(m, ndim=m.ndim, dtype=m.dtype)
                    for m in self._meta_provided
                )
            return meta_from_array(
                self._meta_provided, ndim=self.ndim, dtype=self._meta_provided.dtype
            )
        else:
            return compute_meta(
                self.func, self.operand("dtype"), *self.args[::2], **self.kwargs
            )

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
        return tuple(map(tuple, chunks))

    @cached_property
    def dtype(self):
        return super().dtype

    def __dask_tokenize__(self):
        if not self._determ_token:
            # TODO: Is there an actual need to overwrite this?
            self._determ_token = _tokenize_deterministic(
                self.func,
                self.out_ind,
                self.dtype,
                self.adjust_chunks,
                self.new_axes,
                self.align_arrays,
                self.concatenate,
                *self.args,
                **self.kwargs,
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
                arg, collections = unpack_collections(arg)
                dependencies.extend(collections)
            else:
                if (
                    hasattr(arg, "ndim")
                    and hasattr(ind, "__len__")
                    and arg.ndim != len(ind)
                ):
                    raise ValueError(
                        f"Index string {ind} does not match array dimension {arg.ndim}"
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
            v, collections = unpack_collections(v)
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
        result = dict(graph)
        # Merge in dependency graphs (from delayed objects, etc.)
        for dep in dependencies:
            if is_dask_collection(dep):
                result.update(dep.__dask_graph__())
        return result

    def _lower(self):
        if self.align_arrays:
            _, arrays, changed = unify_chunks_expr(*self.args)
            if changed:
                args = []
                for idx, arr in zip(self.args[1::2], arrays):
                    args.extend([arr, idx])
                return type(self)(*self.operands[: len(self._parameters)], *args)


class Elemwise(Blockwise):
    _parameters = ["op", "dtype", "name", "where", "out", "_user_kwargs"]
    _defaults = {
        "dtype": None,
        "name": None,
        "where": True,
        "out": None,
        "_user_kwargs": None,
    }
    align_arrays = True
    new_axes: dict = {}
    adjust_chunks = None
    concatenate = None

    @property
    def user_kwargs(self):
        return self.operand("_user_kwargs") or {}

    @cached_property
    def _meta(self):
        # When where is not True, _info[0] is _elemwise_handle_where which
        # expects args to end with (where, out)
        args = list(self.elemwise_args)
        if self.where is not True:
            args.extend([self.where, self.out])
        return compute_meta(self._info[0], self.dtype, *args, **self.kwargs)

    @property
    def elemwise_args(self):
        return self.operands[len(self._parameters) :]

    @property
    def out_ind(self):
        shapes = []
        for arg in self.elemwise_args:
            shape = getattr(arg, "shape", ())
            if any(is_dask_collection(x) for x in shape):
                # Want to exclude Delayed shapes and dd.Scalar
                shape = ()
            shapes.append(shape)
        if isinstance(self.where, ArrayExpr):
            shapes.append(self.where.shape)
        if isinstance(self.out, ArrayExpr):
            shapes.append(self.out.shape)

        shapes = [s if isinstance(s, Iterable) else () for s in shapes]
        out_ndim = len(
            broadcast_shapes(*shapes)
        )  # Raises ValueError if dimensions mismatch
        return tuple(range(out_ndim))[::-1]

    @cached_property
    def _info(self):
        if self.operand("dtype") is not None:
            need_enforce_dtype = True
            dtype = self.operand("dtype")
        else:
            # We follow NumPy's rules for dtype promotion, which special cases
            # scalars and 0d ndarrays (which it considers equivalent) by using
            # their values to compute the result dtype:
            # https://github.com/numpy/numpy/issues/6240
            # We don't inspect the values of 0d dask arrays, because these could
            # hold potentially very expensive calculations. Instead, we treat
            # them just like other arrays, and if necessary cast the result of op
            # to match.
            vals = [
                (
                    np.empty((1,) * max(1, a.ndim), dtype=a.dtype)
                    if not is_scalar_for_elemwise(a)
                    else a
                )
                for a in self.elemwise_args
            ]
            try:
                dtype = apply_infer_dtype(
                    self.op, vals, self.user_kwargs, "elemwise", suggest_dtype=False
                )
            except Exception:
                raise NotImplementedError
            need_enforce_dtype = any(
                not is_scalar_for_elemwise(a) and a.ndim == 0
                for a in self.elemwise_args
            )

        blockwise_kwargs = {}
        op = self.op
        if self.where is not True:
            blockwise_kwargs["elemwise_where_function"] = op
            op = _elemwise_handle_where

        if need_enforce_dtype:
            blockwise_kwargs.update(
                {
                    "enforce_dtype": dtype,
                    "enforce_dtype_function": op,
                }
            )
            op = _enforce_dtype

        return op, dtype, blockwise_kwargs

    @property
    def func(self):
        return self._info[0]

    @property
    def dtype(self):
        return self._info[1]

    @property
    def kwargs(self):
        # Merge user kwargs with internal kwargs (dtype enforcement, where handling)
        return {**self.user_kwargs, **self._info[2]}

    @property
    def token(self):
        return funcname(self.op).strip("_")

    @property
    def args(self):
        # for Blockwise rather than Elemwise
        # When where is an array, append [where, out] for _elemwise_handle_where
        extra_args = []
        if self.where is not True:
            extra_args.append(self.where)
            extra_args.append(self.out)
        return tuple(
            toolz.concat(
                (
                    a,
                    (
                        tuple(range(a.ndim)[::-1])
                        if not is_scalar_for_elemwise(a)
                        else None
                    ),
                )
                for a in self.elemwise_args + extra_args
            )
        )

    def _lower(self):
        # Override Blockwise._lower to handle Elemwise's different operand structure.
        # Elemwise stores just arrays in operands, but args generates (array, indices) pairs.
        # After unifying chunks, we only pass the unified arrays (not indices) to the constructor.
        if self.align_arrays:
            _, arrays, changed = unify_chunks_expr(*self.args)
            if changed:
                # Only pass the unified arrays, not the indices
                # When where is an array, the last two arrays are where and out
                if self.where is not True:
                    new_elemwise_args = arrays[:-2]
                    new_where = arrays[-2]
                    new_out = arrays[-1]
                else:
                    new_elemwise_args = arrays
                    new_where = True
                    new_out = None
                return Elemwise(
                    self.op,
                    self.operand("dtype"),
                    self.operand("name"),
                    new_where,
                    new_out,
                    self.operand("_user_kwargs"),
                    *new_elemwise_args,
                )


