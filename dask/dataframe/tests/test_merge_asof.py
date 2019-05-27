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
    B = pd.DataFrame({'right_val': list('xyz'*4)}, index=[1,2,3,6,7,10,12,14,16,19,23,26])
    b = dd.from_pandas(B, npartitions=3)

    C = pd.merge_asof(A, B, left_index=True, right_index=True)
    c = merge_asof(a, b, left_index=True, right_index=True)

    assert_eq(c, C)

def test_random():
    N = 1000
    p = 0.2
    from random import random, randint
    for i in range(1000):
        index_left = [i for i in range(N) if random() < p]
        index_right = [i for i in range(N) if random() < p]
        value_left = [randint(0,9) for i in range(len(index_left))]
        value_right = [randint(0,9) for i in range(len(index_right))]

        A = pd.DataFrame({'left_val': value_left}, index=index_left)
        a = dd.from_pandas(A, npartitions=1)
        a.compute()
        B = pd.DataFrame({'right_val': value_right}, index=index_right)
        b = dd.from_pandas(B, npartitions=1)
        b.compute()

        start_time = time.time()
        C = pd.merge_asof(A, B, left_index=True, right_index=True)
        end_time = time.time()
        old_time = end_time - start_time

        c = merge_asof(a, b, left_index=True, right_index=True)

        start_time = time.time()
        c.compute()
        end_time = time.time()
        new_time = end_time - start_time

        print (old_time/new_time)

        assert_eq (c, C)
