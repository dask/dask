from __future__ import annotations

from collections.abc import Hashable, Mapping, MutableMapping, Sequence
from typing import Any

import pytest

import dask.array as da
import dask.bag as db
import dask.dataframe as dd
import dask.threaded
from dask.base import DaskMethodsMixin, dont_optimize, tokenize
from dask.delayed import delayed
from dask.typing import (
    DaskCollection,
    HLGDaskCollection,
    PostComputeCallable,
    PostPersistCallable,
)

pandas = pytest.importorskip("pandas")


class HLGCollection(DaskMethodsMixin):
    def __init__(self, based_on: HLGDaskCollection) -> None:
        self.based_on = based_on

    def __dask_graph__(self) -> Mapping | None:
        return self.based_on.__dask_graph__()

    def __dask_layers__(self) -> Sequence[str]:
        return self.based_on.__dask_layers__()

    def __dask_keys__(self) -> list[Hashable]:
        return self.based_on.__dask_keys__()

    def __dask_postcompute__(self) -> tuple[PostComputeCallable, tuple]:
        return self.based_on.__dask_postcompute__()

    def __dask_postpersist__(self) -> tuple[PostPersistCallable, tuple]:
        return self.based_on.__dask_postpersist__()

    def __dask_tokenize__(self) -> Hashable:
        return tokenize(self.based_on)

    __dask_scheduler__ = staticmethod(dask.threaded.get)

    @staticmethod
    def __dask_optimize__(
        dsk: MutableMapping, keys: Sequence[Hashable], **kwargs: Any
    ) -> MutableMapping:
        return dont_optimize(dsk, keys, **kwargs)


class NotHLGCollection(DaskMethodsMixin):
    def __init__(self, based_on: DaskCollection) -> None:
        self.based_on = based_on

    def __dask_graph__(self) -> Mapping | None:
        return self.based_on.__dask_graph__()

    def __dask_keys__(self) -> list[Hashable]:
        return self.based_on.__dask_keys__()

    def __dask_postcompute__(self) -> tuple[PostComputeCallable, tuple]:
        return self.based_on.__dask_postcompute__()

    def __dask_postpersist__(self) -> tuple[PostPersistCallable, tuple]:
        return self.based_on.__dask_postpersist__()

    def __dask_tokenize__(self) -> Hashable:
        return tokenize(self.based_on)

    __dask_scheduler__ = staticmethod(dask.threaded.get)

    @staticmethod
    def __dask_optimize__(
        dsk: MutableMapping, keys: Sequence[Hashable], **kwargs: Any
    ) -> MutableMapping:
        return dont_optimize(dsk, keys, **kwargs)


@delayed
def increment(x: int) -> int:
    return x + 1


@pytest.mark.parametrize("protocol", [DaskCollection, HLGDaskCollection])
def test_isinstance_core(protocol: Any) -> None:
    arr = da.ones(10)
    bag = db.from_sequence([1, 2, 3, 4, 5], npartitions=2)
    df = dd.from_pandas(
        pandas.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}),
        npartitions=2,
    )
    dobj = increment(2)

    assert isinstance(arr, protocol)
    assert isinstance(bag, protocol)
    assert isinstance(df, protocol)
    assert isinstance(dobj, protocol)


def test_isinstance_custom() -> None:
    a = da.ones(10)
    hlgc = HLGCollection(a)
    nhlgc = NotHLGCollection(a)

    assert isinstance(hlgc, DaskCollection)
    assert isinstance(nhlgc, DaskCollection)

    assert isinstance(nhlgc, DaskCollection)
    assert not isinstance(nhlgc, HLGDaskCollection)


def test_parameter_passing() -> None:
    def compute(coll: DaskCollection) -> Any:
        return coll.compute()

    a = increment(2)
    hlgc = HLGCollection(a)
    assert compute(hlgc) == 3

    d = increment(3)
    assert compute(d) == 4
