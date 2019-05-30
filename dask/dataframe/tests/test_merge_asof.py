import dask.dataframe as dd
import numpy as np
import pandas as pd
import time

from dask.dataframe.multi import (merge_asof)
from dask.dataframe.utils import (assert_eq)

from dateutil.parser import parse
from datetime import datetime

import pytest

def test_indexed_basic():
    A = pd.DataFrame({'left_val': list('abcd'*3)}, index=[1,3,7,9,10,13,14,17,20,24,25,28])
    a = dd.from_pandas(A, npartitions=4)
    B = pd.DataFrame({'right_val': list('xyz'*4)}, index=[1,2,3,6,7,10,12,14,16,19,23,26])
    b = dd.from_pandas(B, npartitions=3)

    C = pd.merge_asof(A, B, left_index=True, right_index=True)
    c = merge_asof(a, b, left_index=True, right_index=True)

    assert_eq(c, C)

def test_indexed_random():
    avg_slowdown = 0
    N = 0
    with open('tests/merge_asof_tests.txt', 'r') as f:
        N += 1
        index_left = [int(x) for x in f.readline().split(',')]
        index_right = [int(x) for x in f.readline().split(',')]
        value_left = [int(x) for x in f.readline().split(',')]
        value_right = [int(x) for x in f.readline().split(',')]
        ticker_left = [int(x) for x in f.readline().split(',')]
        ticker_right = [int(x) for x in f.readline().split(',')]

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

        assert_eq (c, C)
    #print (avg_slowdown / N)
    #raise NotImplementedError("fail")

def test_on_basic():
    A = pd.DataFrame({'time': [1,3,7,9,10,13,14,17,20,24,25,28], 'left_val': list('abcd'*3)})
    a = dd.from_pandas(A, npartitions=4)
    B = pd.DataFrame({'time': [1,2,3,6,7,10,12,14,16,19,23,26], 'right_val': list('xyz'*4)})
    b = dd.from_pandas(B, npartitions=3)

    C = pd.merge_asof(A, B, on='time').set_index('time')
    c = merge_asof(a, b, on='time')

    assert_eq(c, C)

def test_on_random():
    avg_slowdown = 0
    N = 0
    with open('tests/merge_asof_tests.txt', 'r') as f:
        N += 1
        index_left = [int(x) for x in f.readline().split(',')]
        index_right = [int(x) for x in f.readline().split(',')]
        value_left = [int(x) for x in f.readline().split(',')]
        value_right = [int(x) for x in f.readline().split(',')]
        ticker_left = [int(x) for x in f.readline().split(',')]
        ticker_right = [int(x) for x in f.readline().split(',')]

        A = pd.DataFrame({'time': index_left, 'left_val': value_left})
        a = dd.from_pandas(A, npartitions=5)
        B = pd.DataFrame({'time': index_right, 'right_val': value_right})
        b = dd.from_pandas(B, npartitions=5)

        #start_time = time.time()
        C = pd.merge_asof(A, B, on='time').set_index('time')
        #end_time = time.time()
        #initial_time = end_time - start_time

        c = merge_asof(a, b, on='time')
        #start_time = time.time()
        #c.compute()
        #end_time = time.time()
        #avg_slowdown += initial_time/(end_time-start_time)

        #print (C)
        #print (c.compute())

        assert_eq (c, C)
    #print (avg_slowdown / N)
    #raise NotImplementedError("fail")

def test_by_basic():
    A = pd.DataFrame({'time': [parse(d) for d in ['2016-05-25 13:30:00.023', '2016-05-25 13:30:00.023', '2016-05-25 13:30:00.030', '2016-05-25 13:30:00.041', '2016-05-25 13:30:00.048', '2016-05-25 13:30:00.049', '2016-05-25 13:30:00.072', '2016-05-25 13:30:00.075']], 'ticker': ['GOOG', 'MSFT', 'MSFT', 'MSFT', 'GOOG', 'AAPL', 'GOOG', 'MSFT'], 'bid': [720.50, 51.95, 51.97, 51.99, 720.50, 97.99, 720.50, 52.01], 'ask': [720.93, 51.96, 51.98, 52.00, 720.93, 98.01, 720.88, 52.03]})
    a = dd.from_pandas(A, npartitions=4)
    B = pd.DataFrame({'time': [parse(d) for d in ['2016-05-25 13:30:00.023', '2016-05-25 13:30:00.038', '2016-05-25 13:30:00.048', '2016-05-25 13:30:00.048', '2016-05-25 13:30:00.048']], 'ticker': ['MSFT', 'MSFT', 'GOOG', 'GOOG', 'AAPL'], 'price': [51.95, 51.95, 720.77, 720.92, 98.00], 'quantity': [75, 155, 100, 100, 100]})
    b = dd.from_pandas(B, npartitions=3)

    C = pd.merge_asof(B, A, on='time', by='ticker').set_index('time')
    c = merge_asof(b, a, on='time', by='ticker')

    print (C)
    print (c.compute())

    assert_eq(c, C)

def test_by_random():
    avg_slowdown = 0
    N = 0
    with open('tests/merge_asof_tests.txt', 'r') as f:
        N += 1
        index_left = [int(x) for x in f.readline().split(',')]
        index_right = [int(x) for x in f.readline().split(',')]
        value_left = [int(x) for x in f.readline().split(',')]
        value_right = [int(x) for x in f.readline().split(',')]
        ticker_left = [int(x) for x in f.readline().split(',')]
        ticker_right = [int(x) for x in f.readline().split(',')]

        A = pd.DataFrame({'time': index_left, 'left_val': value_left, 'ticker': ticker_left})
        a = dd.from_pandas(A, npartitions=5)
        B = pd.DataFrame({'time': index_right, 'right_val': value_right, 'ticker': ticker_right})
        b = dd.from_pandas(B, npartitions=5)

        #start_time = time.time()
        C = pd.merge_asof(A, B, on='time', by='ticker').set_index('time')
        #end_time = time.time()
        #initial_time = end_time - start_time

        c = merge_asof(a, b, on='time', by='ticker')
        #start_time = time.time()
        c.compute()
        #end_time = time.time()
        #avg_slowdown += initial_time/(end_time-start_time)

        #assert_eq (c, C)
    #print (avg_slowdown / N)
    #raise NotImplementedError("fail")
