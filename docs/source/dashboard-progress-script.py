"""
This script was run to produce some of the screenshots on https://docs.dask.org/en/stable/dashboard.html
"""

from __future__ import annotations

import time

from dask import delayed
from dask.distributed import Client, wait


@delayed
def inc(x):
    time.sleep(0.1)
    return x + 1


@delayed
def double(x):
    time.sleep(0.1)
    return 2 * x


@delayed
def add(x, y):
    time.sleep(0.1)
    return x + y


if __name__ == "__main__":
    with Client(n_workers=4, threads_per_worker=2, memory_limit="4 GiB") as client:
        while True:
            data = list(range(1000))
            output = []
            for x in data:
                a = inc(x)
                b = double(x)
                c = add(a, b)
                output.append(c)

            total = delayed(sum)(output)
            total = total.persist()
            wait(total)
            time.sleep(5)
            del total
            time.sleep(2)
