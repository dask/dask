import numpy as np
import pandas as pd
import pytest

import dask.dataframe as dd
from dask import config
from dask.dataframe.utils import assert_eq


@pytest.mark.parametrize("use_nullables", (True, False))
def test_roundtrip_parquet_dask_to_dask_pyarrow_stringtype(tmpdir, use_nullables):
    stringtype = config.get("dataframe.parquet.dtypes.string.storage")

    tmpdir = str(tmpdir)
    npartitions = 3

    size = 20
    pdf = pd.DataFrame(
        {
            "a": range(size),
            "b": np.random.random(size=size),
            "c": [True, False] * (size // 2),
            "d": ["alice", "bob"] * (size // 2),
            "e": [None, None] * (size // 2),
            "f": [np.nan, np.nan] * (size // 2),
        }
    )
    # Note: since we set use_nullable_dtypes=True below, we are expecting *all*
    # of the resulting series to use those dtypes. If there is a mix of nullable
    # and non-nullable dtypes here, then that will result in dtype mismatches
    # in the finale frame.
    if use_nullables is True:
        pdf = pdf.astype(
            {
                "a": "Int64",
                "b": "Float64",
                "c": "boolean",
                "d": f"string[{stringtype}]",
                "e": f"string[{stringtype}]",
                "f": "Float64",
            }
        )
        # # Ensure all columns are extension dtypes
        assert all(
            [pd.api.types.is_extension_array_dtype(dtype) for dtype in pdf.dtypes]
        )
    ddf = dd.from_pandas(pdf, npartitions=npartitions)
    ddf.to_parquet(tmpdir, overwrite=True, engine="pyarrow")
    ddf2 = dd.read_parquet(tmpdir, engine="pyarrow", use_nullable_dtypes=use_nullables)
    assert_eq(ddf2, pdf, check_index=False)
    if use_nullables is True:
        assert all(
            [pd.api.types.is_extension_array_dtype(dtype) for dtype in ddf2.dtypes]
        ), ddf2.dtypes
    else:
        assert not any(
            [pd.api.types.is_extension_array_dtype(dtype) for dtype in ddf2.dtypes]
        ), ddf2.dtypes
