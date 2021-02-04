import pytest
import time

from dask import delayed
from dask.utils_test import import_or_none
from dask.graph_manipulation import checkpoint, clone, bind, block_until_done

da = import_or_none("dask.array")
db = import_or_none("dask.bag")
dd = import_or_none("dask.dataframe")
pd = import_or_none("pandas")


def test_checkpoint_basic():
    blocker_done = False

    @delayed
    def blocker():
        nonlocal blocker_done
        blocker_done = True
        return 1

    cp = checkpoint(blocker())
    assert cp.compute(scheduler="sync") is None
    assert blocker_done


@pytest.mark.skipif("not da or not db or not dd")
def test_checkpoint_full():
    count = 0

    def count_me(x):
        nonlocal count
        count += 1
        return x

    # Test two samples of all collections where applicable, one with multiple chunks
    # and one with a single chunk

    # dask.delayed
    c1 = delayed(count_me)("Hello 1")  # 1 chunk
    # dask.array
    c2 = da.ones((10, 10), chunks=5).map_blocks(count_me)  # 4 chunks
    c3 = da.ones((1, ), chunks=-1).map_blocks(count_me)  # 1 chunk
    # dask.bag
    c4 = db.from_sequence([1, 2], npartitions=2).map(count_me)  # 2 chunks
    c5 = db.from_sequence([1], npartitions=1).map(count_me)  # 1 chunk
    c6 = db.Item.from_delayed(delayed(count_me)("Hello 2"))  # 1 chunk
    # dask.dataframe
    df = pd.DataFrame({"x": list(range(10))})
    c7 = dd.from_pandas(df, npartitions=2).map_partitions(count_me)  # 2 chunks
    c8 = dd.from_pandas(df, npartitions=1).map_partitions(count_me)  # 1 chunk
    c9 = dd.from_pandas(df["x"], npartitions=2).map_partitions(count_me)  # 2 chunks
    c10 = dd.from_pandas(df["x"], npartitions=1).map_partitions(count_me)  # 1 chunk

    count = 0
    # Arguments can be nested into data structures
    cp = checkpoint(c1, [c2, {"x": [c3]}], c4, c5, c6, c7, c8, c9, c10)
    cp.compute(scheduler="sync")
    assert count == 16
