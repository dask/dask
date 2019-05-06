import dask.dataframe as dd
import numpy as np
import pandas as pd
import time

from dask.dataframe.multi import (merge_asof)
from dask.dataframe.utils import (assert_eq)

import pytest

def test_time():
    print ("here we go...")

    for N in [100, 1000, 10000, 100000]:
        for npartitions_1 in [1, 5, 10, 25, 50, 100]:
            for npartitions_2 in [1, 5, 10, 25, 50, 100]:
                avg = 0

                for i in range(10):
                    quotes = []
                    trades = []

                    for j in range(N):
                        t = 0
                        ticker = 0
                        bid = 0
                        ask = 0
                        quotes.append((t, ticker, bid, ask))

                    for j in range(N):
                        t = 0
                        ticker = 0
                        price = 0
                        quantity = 0
                        trades.append((t, ticker, price, quantity))


                    quotes.sort()
                    trades.sort()
                    times_left = [x[0] for x in quotes]
                    ticker_left = [x[1] for x in quotes]
                    bid_left = [x[2] for x in quotes]
                    ask_left = [x[3] for x in quotes]
                    times_right = [x[0] for x in trades]
                    ticker_right = [x[1] for x in trades]
                    price_right = [x[2] for x in trades]
                    quantity_right = [x[3] for x in trades]

                    A = pd.DataFrame({'ticker': ticker_left, 'bid': bid_left, 'ask': ask_left}, index=times_left)
                    a = dd.from_pandas(A, npartitions_1)
                    a.compute()

                    B = pd.DataFrame({'ticker': ticker_right, 'price': price_right, 'quantity': quantity_right}, index=times_right)
                    b = dd.from_pandas(B, npartitions_2)
                    b.compute()

                    start_time = time.time()
                    C = pd.merge_asof(A, B, left_index=True, right_index=True)
                    end_time = time.time()
                    T1 = end_time - start_time
                    c = merge_asof(a, b, left_index=True, right_index=True)
                    start_time = time.time()
                    c.compute()
                    end_time = time.time()
                    T2 = end_time - start_time

                    avg += (T1 / T2) / 10

                print ("average speedup for (N, np1, np2) =", (N, npartitions_1, npartitions_2), "is", avg)

"""
def test_basic():
    print ("test basic")
    A = pd.DataFrame({'left_val': ['a', 'b', 'c']}, index=[1, 5, 10])
    a = dd.from_pandas(A, 3)

    B = pd.DataFrame({'right_val': [1, 2, 3, 6, 7]}, index=[1, 2, 3, 6, 7])
    b = dd.from_pandas(B, 3)

    start_time = time.time()
    C = pd.merge_asof(A, B, left_index=True, right_index=True)
    end_time = time.time()
    print (end_time - start_time)
    c = merge_asof(a, b, left_index=True, right_index=True)
    start_time = time.time()
    c.compute()
    end_time = time.time()
    print (end_time - start_time)

    #assert_eq(c, C)

    A = pd.DataFrame({'left_val': ['a','b','c','d','a','b','c','d','a','b','c','d']}, index=[1,3,7,9,10,13,14,17,20,24,25,28])
    a = dd.from_pandas(A , 12)
    B = pd.DataFrame({'right_val': ['x','y','z','x','y','z','x','y','z','x','y','z']}, index=[1,2,3,6,7,10,12,14,16,19,23,26])
    b = dd.from_pandas(B, 12)
    C = pd.merge_asof(A, B, left_index=True, right_index=True)
    c = merge_asof(a, b, left_index=True, right_index=True)
    #assert_eq(c, C)

def test_bigger():
    A = pd.DataFrame({'left_val': list('abcd'*3)}, index=[1, 3, 7, 9, 10, 13, 14, 17, 20, 24, 25, 28])
    a = dd.from_pandas(A, 4)

    B = pd.DataFrame({'right_val': list('xyz'*4), 'right_index': [1, 2, 3, 6, 7, 10, 12, 14, 16, 19, 23, 26]}, index=[1, 2, 3, 6, 7, 10, 12, 14, 16, 19, 23, 26])
    b = dd.from_pandas(B, 3)

    C = pd.merge_asof(A, B, left_index=True, right_index=True)
    c = merge_asof(a, b, left_index=True, right_index=True)

    #assert_eq(c, C)

def test_real():
    A = pd.DataFrame({'left_val': [23,38,48,48,48]}, index=[0,1,2,3,4])
    a = dd.from_pandas(A,5)
    B = pd.DataFrame({'right_val': [23,23,30,41,48,49,72,75]}, index=[0,1,2,3,4,5,6,7])
    b = dd.from_pandas(B,8)
    C = pd.merge_asof(A,B,left_index=True,right_index=True)
    c = merge_asof(a,b,left_index=True,right_index=True)

    #assert_eq(c, C)
"""
