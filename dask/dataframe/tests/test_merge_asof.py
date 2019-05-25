import dask.dataframe as dd
import numpy as np
import pandas as pd
import time

from dask.dataframe.multi import (merge_asof)
from dask.dataframe.utils import (assert_eq)

import pytest

def test_basic():
    A = pd.DataFrame({'left_val': list('abcd'*3)}, index=[1,3,7,9,10,13,14,17,20,24,25,28])
    a = dd.from_pandas(A, npartitions=4)
    B = pd.DataFrame({'right_val': ['x','y','z','x','y','z','x','y','z','x','y','z']}, index=[1,2,3,6,7,10,12,14,16,19,23,26])
    b = dd.from_pandas(B, npartitions=3)

    C = pd.merge_asof(A, B, left_index=True, right_index=True)
    c = merge_asof(a, b, left_index=True, right_index=True)

    print (C.dtypes)
    print (c._meta.dtypes)

    print (C)
    print (c.compute())

    assert_eq(c, C)
