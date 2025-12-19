from __future__ import annotations

import functools
import os
import pickle
import uuid
from collections.abc import Collection
from itertools import product
from threading import Lock
from typing import TYPE_CHECKING, cast

import numpy as np

from dask import core, istask
from dask.array._array_expr._expr import ArrayExpr
from dask.array.core import (
    ArrayLike,
    getter,
    getter_nofancy,
    graph_from_arraylike,
    load_chunk,
    load_store_chunk,
    normalize_chunks,
    slices_from_chunks,
)
from dask.array.utils import meta_from_array
from dask.base import compute_as_if_collection, named_schedulers
from dask.highlevelgraph import HighLevelGraph
from dask.layers import ArraySliceDep
from dask.utils import SerializableLock

if TYPE_CHECKING:
    from dask.delayed import Delayed


class IO(ArrayExpr):
    pass


class FromDelayed(IO):
    """Expression for creating an array from a delayed value."""

    _parameters = ["value", "shape", "dtype", "_meta", "_name_prefix"]
    _defaults = {"dtype": None, "_meta": None, "_name_prefix": None}

    @functools.cached_property
    def _meta(self):
        meta = self.operand("_meta")
        dtype = self.operand("dtype")
        shape = self.operand("shape")
        if meta is not None:
            if dtype is None:
                dtype = getattr(meta, "dtype", None)
            return meta_from_array(meta, dtype=dtype)
        if dtype is not None:
            return np.empty((0,) * len(shape), dtype=dtype)
        return np.empty((0,) * len(shape))

    @functools.cached_property
    def chunks(self):
        return tuple((d,) for d in self.operand("shape"))

    @functools.cached_property
    def _name(self):
        prefix = self.operand("_name_prefix")
        if prefix:
            return prefix
        return "from-delayed-" + self.deterministic_token

    def _layer(self):
        from dask._task_spec import Alias
        from dask.base import is_dask_collection

        value = self.operand("value")
        shape = self.operand("shape")
        key = (self._name,) + (0,) * len(shape)
        task = Alias(key=key, target=value.key)
        result = {key: task}
        # Include the delayed value's graph
        if is_dask_collection(value):
            result.update(value.__dask_graph__())
        return result


class FromNpyStack(IO):
    """Expression for loading an array from a stack of .npy files."""

    _parameters = ["dirname", "mmap_mode"]
    _defaults = {"mmap_mode": "r"}

    @functools.cached_property
    def _info(self):
        """Load and cache the info file."""
        dirname = self.operand("dirname")
        with open(os.path.join(dirname, "info"), "rb") as f:
            return pickle.load(f)

    @functools.cached_property
    def _meta(self):
        info = self._info
        return np.empty((0,) * len(info["chunks"]), dtype=info["dtype"])

    @functools.cached_property
    def chunks(self):
        return self._info["chunks"]

    @functools.cached_property
    def _name(self):
        return "from-npy-stack-" + self.deterministic_token

    def _layer(self):
        dirname = self.operand("dirname")
        mmap_mode = self.operand("mmap_mode")
        info = self._info
        chunks = info["chunks"]
        axis = info["axis"]

        keys = list(product([self._name], *[range(len(c)) for c in chunks]))
        values = [
            (np.load, os.path.join(dirname, f"{i}.npy"), mmap_mode)
            for i in range(len(chunks[axis]))
        ]
        return dict(zip(keys, values))


class FromGraph(IO):
    _parameters = ["layer", "_meta", "chunks", "keys", "name_prefix"]

    @functools.cached_property
    def _meta(self):
        return self.operand("_meta")

    @functools.cached_property
    def chunks(self):
        return self.operand("chunks")

    @functools.cached_property
    def _name(self):
        return self.operand("name_prefix") + "-" + self.deterministic_token

    def _layer(self):
        dsk = dict(self.operand("layer"))
        # The name may not actually match the layer's name therefore rewrite this
        # using an alias. Use the actual keys from the layer dict since they may
        # differ from self.operand("keys") (e.g., after persist() with optimization).
        #
        # Only process keys from our layer, skip dependency keys from HLG.
        our_layer_names = {k[0] for k in self.operand("keys") if isinstance(k, tuple)}

        for k in list(dsk.keys()):
            if not isinstance(k, tuple):
                raise TypeError(f"Expected tuple, got {type(k)}")
            # Only process keys from our layer, skip dependency keys
            if k[0] not in our_layer_names:
                continue
            orig = dsk[k]
            if not istask(orig):
                del dsk[k]
                dsk[(self._name, *k[1:])] = orig
            else:
                dsk[(self._name, *k[1:])] = k
        return dsk


class FromArray(IO):
    _parameters = [
        "array",
        "chunks",
        "lock",
        "getitem",
        "inline_array",
        "meta",
        "asarray",
        "fancy",
        "_name_override",
    ]
    _defaults = {
        "getitem": None,
        "inline_array": False,
        "meta": None,
        "asarray": None,
        "fancy": True,
        "lock": False,
        "_name_override": None,
    }

    @functools.cached_property
    def _name(self):
        name_override = self.operand("_name_override")
        if name_override is not None:
            return name_override
        return f"fromarray-{self.deterministic_token}"

    def __dask_tokenize__(self):
        # When _name_override is provided, use it as the token to avoid
        # tokenizing potentially non-serializable objects like custom locks.
        # The name_override already contains a unique token computed from
        # all the parameters in from_array().
        name_override = self.operand("_name_override")
        if name_override is not None:
            return (type(self).__name__, name_override)
        # Fall back to default behavior
        return super().__dask_tokenize__()

    @property
    def chunks(self):
        # chunks are already normalized when passed from from_array()
        return self.operand("chunks")

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
        # Note: lock=True is already normalized to SerializableLock() in from_array()

        is_ndarray = type(self.array) in (np.ndarray, np.ma.core.MaskedArray)
        is_single_block = all(len(c) == 1 for c in self.chunks)
        # Always use the getter for h5py etc. Not using isinstance(x, np.ndarray)
        # because np.matrix is a subclass of np.ndarray.
        if is_ndarray and not is_single_block and not lock:
            # eagerly slice numpy arrays to prevent memory blowup
            # GH5367, GH5601
            slices = slices_from_chunks(self.chunks)
            keys = product([self._name], *(range(len(bds)) for bds in self.chunks))
            values = [self.array[slc] for slc in slices]
            dsk = dict(zip(keys, values))
        elif is_ndarray and is_single_block:
            # No slicing needed
            dsk = {(self._name,) + (0,) * self.array.ndim: self.array}
        else:
            getitem = self.operand("getitem")
            if getitem is None:
                if self.operand("fancy"):
                    getitem = getter
                else:
                    getitem = getter_nofancy

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


def get_scheduler_lock(collection, scheduler):
    """Get an appropriate lock for the given collection and scheduler."""
    if scheduler is None:
        scheduler = collection.__dask_scheduler__
    actual_get = named_schedulers.get(scheduler, scheduler)
    # Only use locks for non-distributed schedulers
    if actual_get is named_schedulers.get("synchronous", None):
        return False
    return SerializableLock()


def store(
    sources,
    targets,
    lock: bool | Lock = True,
    regions: tuple[slice, ...] | Collection[tuple[slice, ...]] | None = None,
    compute: bool = True,
    return_stored: bool = False,
    load_stored: bool | None = None,
    **kwargs,
):
    """Store dask arrays in array-like objects, overwrite data in target

    This stores dask arrays into object that supports numpy-style setitem
    indexing.  It stores values chunk by chunk so that it does not have to
    fill up memory.  For best performance you can align the block size of
    the storage target with the block size of your array.

    If your data fits in memory then you may prefer calling
    ``np.array(myarray)`` instead.

    Parameters
    ----------

    sources: Array or collection of Arrays
    targets: array-like or Delayed or collection of array-likes and/or Delayeds
        These should support setitem syntax ``target[10:20] = ...``.
        If sources is a single item, targets must be a single item; if sources is a
        collection of arrays, targets must be a matching collection.
    lock: boolean or threading.Lock, optional
        Whether or not to lock the data stores while storing.
        Pass True (lock each file individually), False (don't lock) or a
        particular :class:`threading.Lock` object to be shared among all writes.
    regions: tuple of slices or collection of tuples of slices, optional
        Each ``region`` tuple in ``regions`` should be such that
        ``target[region].shape = source.shape``
        for the corresponding source and target in sources and targets,
        respectively. If this is a tuple, the contents will be assumed to be
        slices, so do not provide a tuple of tuples.
    compute: boolean, optional
        If true compute immediately; return :class:`dask.delayed.Delayed` otherwise.
    return_stored: boolean, optional
        Optionally return the stored result (default False).
    load_stored: boolean, optional
        Optionally return the stored result, loaded in to memory (default None).
        If None, ``load_stored`` is True if ``return_stored`` is True and
        ``compute`` is False. *This is an advanced option.*
        When False, store will return the appropriate ``target`` for each chunk that is stored.
        Directly computing this result is not what you want.
        Instead, you can use the returned ``target`` to execute followup operations to the store.
    kwargs:
        Parameters passed to compute/persist (only used if compute=True)

    Returns
    -------

    If return_stored=True
        tuple of Arrays
    If return_stored=False and compute=True
        None
    If return_stored=False and compute=False
        Delayed

    Examples
    --------

    >>> import h5py  # doctest: +SKIP
    >>> f = h5py.File('myfile.hdf5', mode='a')  # doctest: +SKIP
    >>> dset = f.create_dataset('/data', shape=x.shape,
    ...                                  chunks=x.chunks,
    ...                                  dtype='f8')  # doctest: +SKIP

    >>> store(x, dset)  # doctest: +SKIP

    Alternatively store many arrays at the same time

    >>> store([x, y, z], [dset1, dset2, dset3])  # doctest: +SKIP
    """
    from dask.array._array_expr._collection import Array
    from dask.array._array_expr._map_blocks import map_blocks
    from dask.base import persist

    if isinstance(sources, Array):
        sources = [sources]
        targets = [targets]
    targets = cast("Collection[ArrayLike | Delayed]", targets)

    if any(not isinstance(s, Array) for s in sources):
        raise ValueError("All sources must be dask array objects")

    if len(sources) != len(targets):
        raise ValueError(
            f"Different number of sources [{len(sources)}] and targets [{len(targets)}]"
        )

    if isinstance(regions, tuple) or regions is None:
        regions_list = [regions] * len(sources)
    else:
        regions_list = list(regions)
        if len(sources) != len(regions_list):
            raise ValueError(
                f"Different number of sources [{len(sources)}] and "
                f"targets [{len(targets)}] than regions [{len(regions_list)}]"
            )
    del regions

    if load_stored is None:
        load_stored = return_stored and not compute

    if lock is True:
        lock = get_scheduler_lock(Array, kwargs.get("scheduler"))

    arrays = []
    for s, t, r in zip(sources, targets, regions_list):
        slices = ArraySliceDep(s.chunks)
        arrays.append(
            map_blocks(
                load_store_chunk,
                s,
                t,
                slices,
                region=r,
                lock=lock,
                return_stored=return_stored,
                load_stored=load_stored,
                name="store-map",
                meta=s._meta,
            )
        )

    if compute:
        if not return_stored:
            import dask

            dask.compute(arrays, **kwargs)
            return None
        else:
            stored_persisted = persist(*arrays, **kwargs)
            arrays = []
            for s, r in zip(stored_persisted, regions_list):
                slices = ArraySliceDep(s.chunks)
                arrays.append(
                    map_blocks(
                        load_chunk,
                        s,
                        slices,
                        lock=lock,
                        region=r,
                        name="load-stored",
                        meta=s._meta,
                    )
                )
    if len(arrays) == 1:
        return arrays[0]
    return tuple(arrays)


def to_npy_stack(dirname, x, axis=0):
    """Write dask array to a stack of .npy files

    This partitions the dask.array along one axis and stores each block along
    that axis as a single .npy file in the specified directory

    Examples
    --------
    >>> x = da.ones((5, 10, 10), chunks=(2, 4, 4))  # doctest: +SKIP
    >>> da.to_npy_stack('data/', x, axis=0)  # doctest: +SKIP

    The ``.npy`` files store numpy arrays for ``x[0:2], x[2:4], and x[4:5]``
    respectively, as is specified by the chunk size along the zeroth axis::

        $ tree data/
        data/
        |-- 0.npy
        |-- 1.npy
        |-- 2.npy
        |-- info

    The ``info`` file stores the dtype, chunks, and axis information of the array.
    You can load these stacks with the :func:`dask.array.from_npy_stack` function.

    >>> y = da.from_npy_stack('data/')  # doctest: +SKIP

    See Also
    --------
    from_npy_stack
    """
    from dask.array._array_expr._collection import Array, rechunk

    chunks = tuple((c if i == axis else (sum(c),)) for i, c in enumerate(x.chunks))
    xx = rechunk(x, chunks)

    if not os.path.exists(dirname):
        os.mkdir(dirname)

    meta = {"chunks": chunks, "dtype": x.dtype, "axis": axis}

    with open(os.path.join(dirname, "info"), "wb") as f:
        pickle.dump(meta, f)

    name = f"to-npy-stack-{uuid.uuid1()}"
    dsk = {
        (name, i): (np.save, os.path.join(dirname, f"{i}.npy"), key)
        for i, key in enumerate(core.flatten(xx.__dask_keys__()))
    }

    graph = HighLevelGraph.from_collections(name, dsk, dependencies=[xx])
    compute_as_if_collection(Array, graph, list(dsk))


def from_npy_stack(dirname, mmap_mode="r"):
    """Load dask array from stack of npy files

    Parameters
    ----------
    dirname: string
        Directory of .npy files
    mmap_mode: (None or 'r')
        Read data in memory map mode

    See Also
    --------
    to_npy_stack
    """
    from dask.array._array_expr._collection import new_collection

    return new_collection(FromNpyStack(dirname=dirname, mmap_mode=mmap_mode))


def from_delayed(value, shape, dtype=None, meta=None, name=None):
    """Create a dask array from a dask delayed value

    This routine is useful for constructing dask arrays in an ad-hoc fashion
    using dask delayed, particularly when combined with stack and concatenate.

    The dask array will consist of a single chunk.

    Examples
    --------
    >>> import dask
    >>> import dask.array as da
    >>> import numpy as np
    >>> value = dask.delayed(np.ones)(5)
    >>> array = da.from_delayed(value, (5,), dtype=float)
    >>> array
    dask.array<from-value, shape=(5,), dtype=float64, chunksize=(5,), chunktype=numpy.ndarray>
    >>> array.compute()
    array([1., 1., 1., 1., 1.])
    """
    from dask.array._array_expr._collection import new_collection
    from dask.delayed import Delayed, delayed

    # Convert to Delayed if it has a key but isn't a Delayed
    if not isinstance(value, Delayed) and hasattr(value, "key"):
        value = delayed(value)

    return new_collection(
        FromDelayed(
            value=value,
            shape=shape,
            dtype=dtype,
            _meta=meta,
            _name_prefix=name,
        )
    )
