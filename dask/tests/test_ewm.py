import pytest
import numpy as np
import pandas as pd
import dask.dataframe as df


def test_mean_basic():
    data = np.vstack([np.arange(4) for _ in range(7)])
    pdf = pd.DataFrame.from_records(data)
    ddf = df.from_pandas(pdf, npartitions=3)
    alpha = 0.5

    p = df.from_pandas(pdf.ewm(alpha=alpha, adjust=False).mean())
    d = ddf.ewm(alpha=alpha, adjust=False).mean()

    assert p == d


def test_var_basic():
    data = np.vstack([np.arange(4) for _ in range(7)])
    pdf = pd.DataFrame.from_records(data)
    ddf = df.from_pandas(pdf, npartitions=3)
    alpha = 0.5

    p = df.from_pandas(pdf.ewm(alpha=alpha, adjust=False).var())
    d = ddf.ewm(alpha=alpha, adjust=False).var()

    assert p == d


def test_std_basic():
    data = np.vstack([np.arange(4) for _ in range(7)])
    pdf = pd.DataFrame.from_records(data)
    ddf = df.from_pandas(pdf, npartitions=3)
    alpha = 0.5

    p = df.from_pandas(pdf.ewm(alpha=alpha, adjust=False).std())
    d = ddf.ewm(alpha=alpha, adjust=False).std()

    assert p == d
