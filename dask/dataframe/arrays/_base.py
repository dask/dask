"""
ExtensionArray interface for dask.dataframe.

This provides a base class for the collection backed by
pandas extension arrays.
"""
from math import ceil
from typing import Mapping

import pandas as pd
from pandas.api.extensions import ExtensionDtype

from ... import threaded
from ...base import DaskMethodsMixin, dont_optimize, optimize, tokenize
from ...context import globalmethod
from ...highlevelgraph import HighLevelGraph


def _concat(args):
    pass


def finalize(results):
    return _concat(results)


class ExtensionArray(DaskMethodsMixin):
    """
    A Dask collection backed by one or more pandas ExtensionArrays.

    Parameters
    ----------
    dsk : Mapping
    name : str
    dtype : pandas.api.extensions.ExtensionDtype
    npartitions : int
    """

    # divisions / partitions is a bit tricky. Ideally, we'd be able
    # to associate these arrays with a given index. It'd be nice to do
    #     >>> Series(series.array, index=series.index)
    # and just know that the divisions / partitions are OK. It'd be
    # nice to catch cases where the partitions are incorrect.
    # We can trivially check cases where the *number* of partitions
    # differ. We *cannot* check that the actual *lengths* match,
    # since this array won't have known lengths.
    # So for now we just look at the number.

    _dtype: ExtensionDtype
    dask: Mapping
    key: str

    def __init__(self, dsk, name, dtype, npartitions):
        if not isinstance(dsk, HighLevelGraph):
            dsk = HighLevelGraph.from_collections(name, dsk, dependencies=[])
        self.dask = dsk
        self._name = name
        self._dtype = dtype
        self.npartitions = npartitions

    @property
    def dtype(self) -> ExtensionDtype:
        return self._dtype

    @classmethod
    def from_pandas(
        cls,
        array: pd.api.extensions.ExtensionArray,
        npartitions: int,
        chunksize=None,
        name=None,
    ):
        if (npartitions is None) == (chunksize is None):
            raise ValueError(
                "Exactly one of npartitions and chunksize must be specified."
            )

        nrows = len(array)

        if chunksize is None:
            chunksize = int(ceil(nrows / npartitions))
        else:
            npartitions = int(ceil(nrows / chunksize))

        locations = list(range(0, nrows, chunksize)) + [len(array)]
        dtype = array.dtype

        name = name or ("from_pandas-" + tokenize(array, chunksize))
        dsk = {
            (name, i): array[start:stop]
            for i, (start, stop) in enumerate(zip(locations[:-1], locations[1:]))
        }
        return cls(dsk, name, dtype, npartitions)

    # -------------------------------------------------------------------------
    # Dask Interface
    def __dask_graph__(self) -> Mapping:
        return self.dask

    def __dask_keys__(self):
        return [self.key]

    def __dask_tokenize__(self):
        return self._name

    def __dask_layers__(self):
        return (self.key,)

    __dask_optimize__ = globalmethod(
        optimize, key="dataframe_optimize", falsey=dont_optimize
    )
    __dask_scheduler__ = staticmethod(threaded.get)

    def __dask_postcompute__(self):
        return finalize, ()

    def __dask_postpersist__(self):
        return type(self), (self._name, self._meta, self.divisions)
