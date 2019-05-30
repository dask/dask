from random import random, randint

N = 100
num_trials = 10000

with open('merge_asof_tests.txt', 'w') as f:
    for trial in range(num_trials):
        index_left, index_right, value_left, value_right, ticker_left, ticker_right = [], [], [], [], [], []
        for i in range(N):
            L = [i] * randint(0,3)
            index_left.extend(L)
            value_left.extend([randint(0,9) for i in range(len(L))])
            ticker_left.extend([randint(0,9) for i in range(len(L))])
        for i in range(N):
            L = [i] * randint(0,3)
            index_right.extend(L)
            value_right.extend([randint(0,9) for i in range(len(L))])
            ticker_right.extend([randint(0,9) for i in range(len(L))])
        for A in [index_left, index_right, value_left, value_right, ticker_left, ticker_right]:
            f.write(str(A)[1:-1] + "\n")
