import dask.dataframe as dd

import pandas as pd
import numpy as np
import pytest

rs = np.random.RandomState(96)
@pytest.mark.parametrize("df", [
    pd.DataFrame({
        'a': [1, 2, 3]*3,
        'b': [1.2, 3.4, 5.6]*3,
        'c': -np.arange(9, dtype=np.int8)}),
    pd.DataFrame({
        'i': rs.randint(0, 1000000, (10000,)),
        'j': rs.randn(10000),
        'k': rs.uniform(0, 9999999, (10000,))}),
    pd.DataFrame({
        'i': np.repeat(rs.randint(0, 1000000, (1000,)), 3),
        'j': np.repeat(rs.randn(1000), 3),
        'k': np.repeat(rs.uniform(0, 9999999, (1000,)), 3)}),
    pd.DataFrame({
        'x': rs.randint(0, 1000000, (10000,))}),
    pd.DataFrame({
        'w': np.zeros((20000,)),
        'x': np.zeros((20000,)),
        'y': np.zeros((20000,)) + 4803592,
        'z': np.zeros((20000,))}),
])
def test_basic(df):
    ddf = dd.from_pandas(df, 2)

    approx = ddf.nunique_approx().compute()
    exact = len(df.drop_duplicates())
    assert abs(approx - exact) <= 2 or abs(approx - exact) / exact < 0.05

@pytest.mark.xfail
def test_with_complex():
    df = pd.DataFrame({
        'a': [1, 2, 3]*3,
        'b': [1.2, 3.4, 5.6]*3,
        'c': [1+2j, 3+4j, 5+6j]*3, # complex not currently supported
        'd': -np.arange(9, dtype=np.int8)})
    ddf = dd.from_pandas(df, 2)

    approx = ddf.nunique_approx().compute()
    assert abs(approx - len(df.drop_duplicates())) < 0.1
