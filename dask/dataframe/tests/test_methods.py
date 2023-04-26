import numpy as np
import pandas as pd

import dask.dataframe.methods as methods


def test_assign_not_modifying_array_inplace():
    df = pd.DataFrame({"a": [1, 2, 3], "b": 1})
    result = methods.assign(df, "a", 5)
    assert not np.shares_memory(df["a"].values, result["a"].values)
