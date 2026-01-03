from __future__ import annotations

from collections.abc import Collection
from threading import Lock
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from dask.delayed import Delayed

from dask.base import named_schedulers
from dask.utils import SerializableLock


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
    from dask.array.core import ArrayLike, load_chunk, load_store_chunk
    from dask.base import persist
    from dask.layers import ArraySliceDep

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
