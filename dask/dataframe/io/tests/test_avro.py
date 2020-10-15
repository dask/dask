import os

import pandas as pd
import numpy as np
import pytest

import dask.dataframe as dd

from dask.dataframe.utils import assert_eq
from dask.dataframe.io.demo import names as name_list

try:
    import fastavro as fa
except ImportError:
    fa = None


@pytest.mark.parametrize("chunksize", [None, "1KB"])
@pytest.mark.parametrize("split_blocks", [True, False])
@pytest.mark.parametrize("size", [100, 1000])
@pytest.mark.parametrize("nfiles", [1, 2])
def test_read_avro_basic(tmpdir, chunksize, size, split_blocks, nfiles):
    # Require fastavro library
    pytest.importorskip("fastavro")

    # Define avro schema
    schema = fa.parse_schema(
        {
            "name": "avro.example.User",
            "type": "record",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "age", "type": "int"},
            ],
        }
    )

    # Write avro dataset with two files.
    # Collect block and record (row) count while writing.
    nblocks = 0
    nrecords = 0
    paths = [os.path.join(str(tmpdir), f"test.{i}.avro") for i in range(nfiles)]
    records = []
    for path in paths:
        names = np.random.choice(name_list, size)
        ages = np.random.randint(18, 100, size)
        data = [{"name": names[i], "age": ages[i]} for i in range(size)]
        with open(path, "wb") as f:
            fa.writer(f, schema, data)
        with open(path, "rb") as fo:
            avro_reader = fa.block_reader(fo)
            for block in avro_reader:
                nrecords += block.num_records
                nblocks += 1
                records += list(block)
    if nfiles == 1:
        paths = paths[0]

    # Read back with dask.dataframe
    df = dd.io.avro.read_avro(paths, chunksize=chunksize, split_blocks=split_blocks)

    # Check basic length and partition count
    if split_blocks is True and chunksize == "1KB":
        assert df.npartitions == nblocks
    assert len(df) == nrecords

    # Full comparison
    expect = pd.DataFrame.from_records(records)
    assert_eq(df.compute(scheduler="synchronous").reset_index(drop=True), expect)
