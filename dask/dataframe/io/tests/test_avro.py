import os

# import pandas as pd
import numpy as np
import pytest

import dask.dataframe as dd

# from dask.dataframe.utils import assert_eq

try:
    import fastavro as fa
except ImportError:
    fa = None

name_list = [
    "Alice",
    "Bob",
    "Charlie",
    "Dan",
    "Edith",
    "Frank",
    "George",
    "Hannah",
    "Ingrid",
    "Jerry",
    "Kevin",
    "Laura",
    "Michael",
    "Norbert",
    "Oliver",
    "Patricia",
    "Quinn",
    "Ray",
    "Sarah",
    "Tim",
    "Ursula",
    "Victor",
    "Wendy",
    "Xavier",
    "Yvonne",
    "Zelda",
]


@pytest.mark.parametrize("blocksize", [1, 100, 1000])
def test_read_avro_basic(tmpdir, blocksize):

    pytest.importorskip("fastavro")

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

    size = 1_000_000
    nblocks = 0
    nrecords = 0
    paths = [
        os.path.join(str(tmpdir), "test.0.avro"),
        os.path.join(str(tmpdir), "test.1.avro"),
    ]

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

    dd.io.avro.read_avro(paths)

    # print(nblocks, nrecords)
    # assert_eq(out, df)
