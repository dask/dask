from __future__ import annotations

from collections.abc import Hashable, Mapping, Sequence
from typing import Any

import pytest

import dask.threaded
from dask.base import DaskMethodsMixin, dont_optimize, tokenize
from dask.context import globalmethod
from dask.delayed import Delayed, delayed
from dask.typing import (
    DaskCollection,
    HLGDaskCollection,
    PostComputeCallable,
    PostPersistCallable,
)

da = pytest.importorskip("dask.array")
db = pytest.importorskip("dask.bag")
dds = pytest.importorskip("dask.datasets")
dd = pytest.importorskip("dask.dataframe")
pandas = pytest.importorskip("pandas")


def finalize(x: Sequence[Any]) -> Any:
    return x[0]


class HLGCollection(DaskMethodsMixin):
    def __init__(self, based_on: HLGDaskCollection) -> None:
        self.based_on = based_on

    def __dask_graph__(self) -> Mapping:
        return self.based_on.__dask_graph__()

    def __dask_layers__(self) -> Sequence[str]:
        return self.based_on.__dask_layers__()

    def __dask_keys__(self) -> list[Hashable]:
        return self.based_on.__dask_keys__()

    def __dask_postcompute__(self) -> tuple[PostComputeCallable, tuple]:
        return finalize, ()

    def __dask_postpersist__(self) -> tuple[PostPersistCallable, tuple]:
        return self.based_on.__dask_postpersist__()

    def __dask_tokenize__(self) -> Hashable:
        return tokenize(self.based_on)

    __dask_scheduler__ = staticmethod(dask.threaded.get)

    __dask_optimize__ = globalmethod(
        dont_optimize,
        key="hlgcollection_optim",
        falsey=dont_optimize,
    )


class NotHLGCollection(DaskMethodsMixin):
    def __init__(self, based_on: DaskCollection) -> None:
        self.based_on = based_on

    def __dask_graph__(self) -> Mapping:
        return self.based_on.__dask_graph__()

    def __dask_keys__(self) -> list[Hashable]:
        return self.based_on.__dask_keys__()

    def __dask_postcompute__(self) -> tuple[PostComputeCallable, tuple]:
        return finalize, ()

    def __dask_postpersist__(self) -> tuple[PostPersistCallable, tuple]:
        return self.based_on.__dask_postpersist__()

    def __dask_tokenize__(self) -> Hashable:
        return tokenize(self.based_on)

    __dask_scheduler__ = staticmethod(dask.threaded.get)

    __dask_optimize__ = globalmethod(
        dont_optimize,
        key="collection_optim",
        falsey=dont_optimize,
    )


def increment_(x):
    return x + 1


increment: Delayed = delayed(increment_)


def assert_isinstance(coll: DaskCollection, protocol: Any):
    assert isinstance(coll, protocol)


@pytest.mark.parametrize("protocol", [DaskCollection, HLGDaskCollection])
def test_isinstance_core(protocol: Any) -> None:
    from dask.array import Array
    from dask.bag import Bag
    from dask.dataframe import DataFrame

    arr: Array = da.ones(10)
    bag: Bag = db.from_sequence([1, 2, 3, 4, 5], npartitions=2)
    df: DataFrame = dds.timeseries()
    dobj: Delayed = increment(2)

    assert_isinstance(arr, protocol)
    assert_isinstance(bag, protocol)
    assert_isinstance(df, protocol)
    assert_isinstance(dobj, protocol)


def test_isinstance_custom() -> None:
    a = da.ones(10)
    hlgc = HLGCollection(a)
    nhlgc = NotHLGCollection(a)

    assert isinstance(hlgc, DaskCollection)
    assert isinstance(nhlgc, DaskCollection)

    assert isinstance(nhlgc, DaskCollection)
    assert not isinstance(nhlgc, HLGDaskCollection)


def test_parameter_passing() -> None:
    from dask.array import Array

    def compute(coll: DaskCollection) -> Any:
        return coll.compute()

    a: Delayed = increment(2)
    hlgc = HLGCollection(a)
    assert compute(hlgc) == 3

    d: Delayed = increment(3)
    assert compute(d) == 4

    array: Array = da.ones(10)
    assert compute(array).shape == (10,)
