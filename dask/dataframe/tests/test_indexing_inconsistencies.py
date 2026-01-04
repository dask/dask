import pandas as pd
import dask.dataframe as dd
from pandas.testing import assert_frame_equal


def test_boolean_series_indexing_matches_pandas():
    # Create a simple Pandas DataFrame
    pdf = pd.DataFrame({"c0": [0, 1]})

    # Convert to Dask DataFrame (single partition for determinism)
    ddf = dd.from_pandas(pdf, npartitions=1)

    # Boolean mask via astype(bool)
    pd_result = pdf[pdf["c0"].astype(bool)]
    dd_result = ddf[ddf["c0"].astype(bool)].compute()

    # Ensure Dask matches Pandas exactly
    assert_frame_equal(dd_result, pd_result)
