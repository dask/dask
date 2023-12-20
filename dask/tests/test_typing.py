from __future__ import annotations

from collections.abc import Hashable, Mapping, Sequence
from typing import Any

import pytest

import dask
import dask.threaded
from dask.base import DaskMethodsMixin, dont_optimize, tokenize
from dask.context import globalmethod
from dask.delayed import Delayed, delayed
from dask.typing import (
    Key,
    NestedKeys,
    _DaskCollection,
    _Graph,
    _HLGDaskCollection,
    _PostComputeCallable,
    _PostPersistCallable,
)

try:
    from IPython.display import DisplayObject
except ImportError:
    DisplayObject = Any


da = pytest.importorskip("dask.array")
db = pytest.importorskip("dask.bag")
dds = pytest.importorskip("dask.datasets")
dd = pytest.importorskip("dask.dataframe")


def finalize(x: Sequence[Any]) -> Any:
    return x[0]


def get1(dsk: Mapping, keys: Sequence[Key] | Key, **kwargs: Any) -> Any:
    return dask.threaded.get(dsk, keys, **kwargs)


def get2(dsk: Mapping, keys: Sequence[Key] | Key, **kwargs: Any) -> Any:
    return dask.get(dsk, keys, **kwargs)


class Inheriting(_DaskCollection):
    def __init__(self, based_on: _DaskCollection) -> None:
        self.based_on = based_on

    def __dask_graph__(self) -> _Graph:
        return self.based_on.__dask_graph__()

    def __dask_keys__(self) -> NestedKeys:
        return self.based_on.__dask_keys__()

    def __dask_postcompute__(self) -> tuple[_PostComputeCallable, tuple]:
        return finalize, ()

    def __dask_postpersist__(self) -> tuple[_PostPersistCallable, tuple]:
        return self.based_on.__dask_postpersist__()

    def __dask_tokenize__(self) -> Hashable:
        return tokenize(self.based_on)

    __dask_scheduler__ = staticmethod(dask.threaded.get)

    __dask_optimize__ = globalmethod(
        dont_optimize,
        key="hlgcollection_optim",
        falsey=dont_optimize,
    )

    def compute(self, **kwargs) -> Any:
        return dask.compute(self, **kwargs)

    def persist(self, **kwargs) -> Inheriting:
        return Inheriting(self.based_on.persist(**kwargs))

    def visualize(
        self,
        filename: str = "mydask",
        format: str | None = None,
        optimize_graph: bool = False,
        **kwargs: Any,
    ) -> DisplayObject | None:
        return dask.visualize(
            self,
            filename=filename,
            format=format,
            optimize_graph=optimize_graph,
            **kwargs,
        )


class HLGCollection(DaskMethodsMixin):
    def __init__(self, based_on: _HLGDaskCollection) -> None:
        self.based_on = based_on

    def __dask_graph__(self) -> _Graph:
        return self.based_on.__dask_graph__()

    def __dask_layers__(self) -> Sequence[str]:
        return self.based_on.__dask_layers__()

    def __dask_keys__(self) -> NestedKeys:
        return self.based_on.__dask_keys__()

    def __dask_postcompute__(self) -> tuple[_PostComputeCallable, tuple]:
        return finalize, ()

    def __dask_postpersist__(self) -> tuple[_PostPersistCallable, tuple]:
        return self.based_on.__dask_postpersist__()

    def __dask_tokenize__(self) -> Hashable:
        return tokenize(self.based_on)

    __dask_scheduler__ = staticmethod(get1)

    __dask_optimize__ = globalmethod(
        dont_optimize,
        key="hlgcollection_optim",
        falsey=dont_optimize,
    )


class NotHLGCollection(DaskMethodsMixin):
    def __init__(self, based_on: _DaskCollection) -> None:
        self.based_on = based_on

    def __dask_graph__(self) -> _Graph:
        return self.based_on.__dask_graph__()

    def __dask_keys__(self) -> NestedKeys:
        return self.based_on.__dask_keys__()

    def __dask_postcompute__(self) -> tuple[_PostComputeCallable, tuple]:
        return finalize, ()

    def __dask_postpersist__(self) -> tuple[_PostPersistCallable, tuple]:
        return self.based_on.__dask_postpersist__()

    def __dask_tokenize__(self) -> Hashable:
        return tokenize(self.based_on)

    __dask_scheduler__ = staticmethod(get2)

    __dask_optimize__ = globalmethod(
        dont_optimize,
        key="collection_optim",
        falsey=dont_optimize,
    )


def increment_(x: int) -> int:
    return x + 1


increment: Delayed = delayed(increment_)


def assert_isinstance(coll: _DaskCollection, protocol: Any) -> None:
    assert isinstance(coll, protocol)


@pytest.mark.parametrize("protocol", [_DaskCollection, _HLGDaskCollection])
def test_isinstance_core(protocol):
    arr = da.ones(10)
    bag = db.from_sequence([1, 2, 3, 4, 5], npartitions=2)
    df = dds.timeseries()
    dobj = increment(2)

    assert_isinstance(arr, protocol)
    assert_isinstance(bag, protocol)
    assert_isinstance(df, protocol)
    assert_isinstance(dobj, protocol)


def test_isinstance_custom() -> None:
    a = da.ones(10)
    hlgc = HLGCollection(a)
    nhlgc = NotHLGCollection(a)

    assert isinstance(hlgc, _DaskCollection)
    assert isinstance(nhlgc, _DaskCollection)

    assert isinstance(nhlgc, _DaskCollection)
    assert not isinstance(nhlgc, _HLGDaskCollection)


def compute(coll: _DaskCollection) -> Any:
    return coll.compute()


def compute2(coll: _DaskCollection) -> Any:
    return coll.compute()


def test_parameter_passing() -> None:
    from dask.array import Array

    a: Delayed = increment(2)
    hlgc = HLGCollection(a)
    assert compute(hlgc) == 3
    assert compute2(hlgc) == 3

    d: Delayed = increment(3)
    assert compute(d) == 4
    assert compute2(d) == 4

    array: Array = da.ones(10)
    assert compute(array).shape == (10,)
    assert compute2(array).shape == (10,)


def test_inheriting_class() -> None:
    inheriting: Inheriting = Inheriting(increment(2))
    assert isinstance(inheriting, Inheriting)
