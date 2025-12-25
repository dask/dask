from __future__ import annotations

import functools
from itertools import product

import numpy as np

from dask.array._array_expr.io._base import IO
from dask.array.core import (
    getter,
    getter_nofancy,
    graph_from_arraylike,
    normalize_chunks,
    slices_from_chunks,
)
from dask.array.utils import meta_from_array
from dask.utils import SerializableLock


class FromArray(IO):
    _parameters = [
        "array",
        "_chunks",
        "lock",
        "getitem",
        "inline_array",
        "meta",
        "asarray",
        "fancy",
        "_name_override",
        "_region",  # Slice region for pushdown (tuple of slices or None)
    ]
    _defaults = {
        "_chunks": "auto",
        "getitem": None,
        "inline_array": False,
        "meta": None,
        "asarray": None,
        "fancy": True,
        "lock": False,
        "_name_override": None,
        "_region": None,
    }
    # FromArray reads static data, so rechunk can be pushed in safely
    _can_rechunk_pushdown = True
    # Slicing can be pushed into FromArray by slicing the source array
    _slice_pushdown = True

    @functools.cached_property
    def _name(self):
        # _name_override is a prefix, deterministic token is always appended
        prefix = self.operand("_name_override") or "fromarray"
        return f"{prefix}-{self.deterministic_token}"

    @functools.cached_property
    def _effective_shape(self):
        """Shape after applying region slice."""
        region = self.operand("_region")
        if region is None:
            return self.array.shape
        # Compute shape from region slices
        return tuple(
            len(range(*slc.indices(dim_size)))
            for slc, dim_size in zip(region, self.array.shape)
        )

    @functools.cached_property
    def chunks(self):
        # Normalize chunks lazily - keeps repr compact with user-provided chunks
        # Pass previous_chunks from underlying array (h5py, zarr) for alignment
        previous_chunks = getattr(self.array, "chunks", None)
        # Handle zarr 3.x shards attribute for write alignment
        if (
            hasattr(self.array, "shards")
            and self.array.shards is not None
            and self.operand("_chunks") == "auto"
        ):
            previous_chunks = self.array.shards
        return normalize_chunks(
            self.operand("_chunks"),
            self._effective_shape,
            dtype=self.array.dtype,
            previous_chunks=previous_chunks,
        )

    @functools.cached_property
    def _meta(self):
        if self.operand("meta") is not None:
            return meta_from_array(self.operand("meta"), dtype=self.array.dtype)
        return meta_from_array(self.array, dtype=getattr(self.array, "dtype", None))

    @functools.cached_property
    def asarray_arg(self):
        if self.operand("asarray") is None:
            return not hasattr(self.array, "__array_function__")
        else:
            return self.operand("asarray")

    def _layer(self):
        lock = self.operand("lock")
        region = self.operand("_region")
        # Note: lock=True is already normalized to SerializableLock() in from_array()

        is_ndarray = type(self.array) in (np.ndarray, np.ma.core.MaskedArray)
        is_single_block = all(len(c) == 1 for c in self.chunks)

        # Get slices for chunks (based on effective shape after region)
        slices = slices_from_chunks(self.chunks)

        # If region is set, offset all slices by the region start
        if region is not None:
            region_starts = tuple(
                slc.indices(dim_size)[0]
                for slc, dim_size in zip(region, self.array.shape)
            )
            slices = [
                tuple(
                    slice(s.start + offset, s.stop + offset, s.step)
                    for s, offset in zip(slc, region_starts)
                )
                for slc in slices
            ]

        # Always use the getter for h5py etc. Not using isinstance(x, np.ndarray)
        # because np.matrix is a subclass of np.ndarray.
        if is_ndarray and not is_single_block and not lock:
            # eagerly slice numpy arrays to prevent memory blowup
            # GH5367, GH5601
            keys = product([self._name], *(range(len(bds)) for bds in self.chunks))
            values = [self.array[slc] for slc in slices]
            dsk = dict(zip(keys, values))
        elif is_ndarray and is_single_block and not lock:
            # Single block - slice with region (or full array) and copy
            if region is not None:
                dsk = {
                    (self._name,) + (0,) * self.array.ndim: self.array[region].copy()
                }
            else:
                dsk = {(self._name,) + (0,) * self.array.ndim: self.array.copy()}
        else:
            getitem = self.operand("getitem")
            if getitem is None:
                if self.operand("fancy"):
                    getitem = getter
                else:
                    getitem = getter_nofancy

            # For non-numpy arrays with region, we need custom graph generation
            # to apply the offset slices
            if region is not None:
                keys = list(
                    product([self._name], *(range(len(bds)) for bds in self.chunks))
                )
                if self.inline_array:
                    dsk = {
                        k: (getitem, self.array, slc, self.asarray_arg, lock)
                        for k, slc in zip(keys, slices)
                    }
                else:
                    # Put array in graph once, reference by key
                    arr_key = ("array-" + self._name,)
                    dsk = {arr_key: self.array}
                    dsk.update(
                        {
                            k: (getitem, arr_key, slc, self.asarray_arg, lock)
                            for k, slc in zip(keys, slices)
                        }
                    )
            else:
                dsk = graph_from_arraylike(
                    self.array,
                    chunks=self.chunks,
                    shape=self.array.shape,
                    name=self._name,
                    lock=lock,
                    getitem=getitem,
                    asarray=self.asarray_arg,
                    inline_array=self.inline_array,
                    dtype=self.array.dtype,
                )
        return dict(dsk)  # this comes as a legacy HLG for now

    def __str__(self):
        return "FromArray(...)"

    def __dask_tokenize__(self):
        from dask.tokenize import _tokenize_deterministic

        # Handle non-serializable locks by using their id()
        # Locks are identity-based objects, so using id() is semantically correct
        lock = self.operand("lock")
        if lock and not isinstance(lock, (bool, SerializableLock)):
            lock_token = ("lock-id", id(lock))
        else:
            lock_token = lock

        operands = [
            lock_token if p == "lock" else self.operand(p) for p in self._parameters
        ]
        return _tokenize_deterministic(type(self), *operands)

    def _simplify_up(self, parent, dependents):
        """Allow slice operations to push into FromArray."""
        from dask.array._array_expr.slicing import SliceSlicesIntegers

        if isinstance(parent, SliceSlicesIntegers):
            return self._accept_slice(parent)
        return None

    def _accept_slice(self, slice_expr):
        """Accept a slice by setting region (deferred slice).

        Pushes the slice into the FromArray expression by recording it as a region,
        which is then applied during layer generation.
        """
        from numbers import Integral

        from dask.array._array_expr.slicing._basic import (
            SliceSlicesIntegers,
            _compose_slices,
            _compute_sliced_chunks,
        )

        index = slice_expr.index

        # Only handle slices and integers (no None/newaxis, no fancy indexing)
        if any(idx is None for idx in index):
            return None
        if any(not isinstance(idx, (slice, Integral)) for idx in index):
            return None
        # Don't push non-unit step slices - _layer doesn't handle them correctly
        if any(
            isinstance(idx, slice) and idx.step is not None and idx.step != 1
            for idx in index
        ):
            return None

        source = self.array
        old_chunks = self.chunks  # Use normalized chunks property
        old_region = self.operand("_region")

        # Pad index to full dimensions
        full_index = index + (slice(None),) * (source.ndim - len(index))

        # Check if any integers are present - they need special handling
        has_integers = any(isinstance(idx, Integral) for idx in full_index)

        # Convert integers to 1-element slices for region calculation
        region_index = tuple(
            slice(idx, idx + 1) if isinstance(idx, Integral) else idx
            for idx in full_index
        )

        # Compute new region by combining with existing region
        if old_region is not None:
            # Compose slices: new slice is relative to old region
            new_region = tuple(
                _compose_slices(old_slc, new_slc, dim_size)
                for old_slc, new_slc, dim_size in zip(
                    old_region, region_index, source.shape
                )
            )
        else:
            new_region = region_index

        # Compute new chunks - use same chunk sizes but clipped to new shape
        new_chunks = tuple(
            _compute_sliced_chunks(dim_chunks, slc, dim_size)
            for dim_chunks, slc, dim_size in zip(
                old_chunks, region_index, self._effective_shape
            )
        )

        # Create new FromArray with region (deferred slice)
        new_io = FromArray(
            source,  # Keep original source, don't slice
            new_chunks,
            lock=self.operand("lock"),
            getitem=self.operand("getitem"),
            inline_array=self.inline_array,
            meta=self.operand("meta"),
            asarray=self.operand("asarray"),
            fancy=self.operand("fancy"),
            _name_override=self.operand("_name_override"),
            _region=new_region,
        )

        # If integers were present, apply them to extract elements
        if has_integers:
            # Build index with 0s for integer dims (they're now size-1)
            extract_index = tuple(
                0 if isinstance(idx, Integral) else slice(None) for idx in full_index
            )
            return SliceSlicesIntegers(new_io, extract_index, False)

        return new_io
