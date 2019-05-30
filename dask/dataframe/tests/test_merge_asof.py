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

def test_duplicates_random():
    avg_slowdown = 0
    N = 0
    with open('tests/merge_asof_tests.txt', 'r') as f:
        N += 1
        index_left = [int(x) for x in f.readline().split(',')]
        index_right = [int(x) for x in f.readline().split(',')]
        value_left = [int(x) for x in f.readline().split(',')]
        value_right = [int(x) for x in f.readline().split(',')]

        A = pd.DataFrame({'left_val': value_left}, index=index_left)
        a = dd.from_pandas(A, npartitions=5)
        B = pd.DataFrame({'right_val': value_right}, index=index_right)
        b = dd.from_pandas(B, npartitions=5)

        #start_time = time.time()
        C = pd.merge_asof(A, B, left_index=True, right_index=True)
        #end_time = time.time()
        #initial_time = end_time - start_time

        c = merge_asof(a, b, left_index=True, right_index=True)
        #start_time = time.time()
        #c.compute()
        #end_time = time.time()
        #avg_slowdown += initial_time/(end_time-start_time)

        #assert_eq (c, C)
    #print (avg_slowdown / N)
    #raise NotImplementedError("fail")
