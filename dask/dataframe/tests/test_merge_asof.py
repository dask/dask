import dask.dataframe as dd
import numpy as np
import pandas as pd

from dask.dataframe.multi import (merge_asof)
from dask.dataframe.utils import (assert_eq)

import pytest

def test_basic():
    A = pd.DataFrame({'left_val': ['a', 'b', 'c']}, index=[1, 5, 10])
    a = dd.from_pandas(A, 3)

    B = pd.DataFrame({'right_val': [1, 2, 3, 6, 7]}, index=[1, 2, 3, 6, 7])
    b = dd.from_pandas(B, 3)

    C = pd.merge_asof(A, B, left_index=True, right_index=True)
    c = merge_asof(a, b, left_index=True, right_index=True)

    assert_eq(c, C)

def test_bigger():
    A = pd.DataFrame({'left_val': list('abcd'*3)}, index=[1, 3, 7, 9, 10, 13, 14, 17, 20, 24, 25, 28])
    a = dd.from_pandas(A, 4)

    B = pd.DataFrame({'right_val': list('xyz'*4), 'right_index': [1, 2, 3, 6, 7, 10, 12, 14, 16, 19, 23, 26]}, index=[1, 2, 3, 6, 7, 10, 12, 14, 16, 19, 23, 26])
    b = dd.from_pandas(B, 3)

    C = pd.merge_asof(A, B, left_index=True, right_index=True)
    c = merge_asof(a, b, left_index=True, right_index=True)

    #print (C)
    print (c.compute())

    assert_eq(c, C)
