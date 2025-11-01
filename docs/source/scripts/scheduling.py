from __future__ import annotations

from random import randint
from time import time

import matplotlib.pyplot as plt

import dask
from dask import local, multiprocessing, threaded


def noop(x):
    pass


nrepetitions = 1


def trivial(width, height):
    """Embarrassingly parallel dask"""
    d = {("x", 0, i): i for i in range(width)}
    for j in range(1, height):
        d.update({("x", j, i): (noop, ("x", j - 1, i)) for i in range(width)})
    return d, [("x", height - 1, i) for i in range(width)]


def crosstalk(width, height, connections):
    """Natural looking dask with some inter-connections"""
    d = {("x", 0, i): i for i in range(width)}
    for j in range(1, height):
        d.update(
            {
                ("x", j, i): (
                    noop,
                    [("x", j - 1, randint(0, width)) for _ in range(connections)],
                )
                for i in range(width)
            }
        )
    return d, [("x", height - 1, i) for i in range(width)]


def dense(width, height):
    """Full barriers between each step"""
    d = {("x", 0, i): i for i in range(width)}
    for j in range(1, height):
        d.update(
            {
                ("x", j, i): (noop, [("x", j - 1, k) for k in range(width)])
                for i in range(width)
            }
        )
    return d, [("x", height - 1, i) for i in range(width)]


import numpy as np

x = np.logspace(0, 4, 10)
trivial_results = dict()
for get in (dask.get, threaded.get, local.get_sync, multiprocessing.get):
    y = list()
    for n in x:
        dsk, keys = trivial(int(n), 5)
        start = time()
        get(dsk, keys)
        end = time()
        y.append(end - start)
    trivial_results[get] = np.array(y)


########
# Plot #
########

f, (left, right) = plt.subplots(
    nrows=1, ncols=2, sharex=True, figsize=(12, 5), squeeze=True
)

for get, result in trivial_results.items():
    left.loglog(x * 5, result, label=get.__module__)
    right.loglog(x * 5, result / x, label=get.__module__)

left.set_title("Cost for Entire graph")
right.set_title("Cost per task")
left.set_ylabel("Duration (s)")
right.set_ylabel("Duration (s)")
left.set_xlabel("Number of tasks")
right.set_xlabel("Number of tasks")

plt.legend()
plt.savefig("images/scaling-nodes.png")

#####################
# Crosstalk example #
#####################

x = np.linspace(1, 100, 10)
crosstalk_results = dict()
for get in [threaded.get, local.get_sync]:  # type: ignore[assignment]
    y = list()
    for n in x:
        dsk, keys = crosstalk(1000, 5, int(n))
        start = time()
        get(dsk, keys)
        end = time()
        y.append(end - start)
    crosstalk_results[get] = np.array(y)

########
# Plot #
########

f, (left, right) = plt.subplots(
    nrows=1, ncols=2, sharex=True, figsize=(12, 5), squeeze=True
)

for get, result in crosstalk_results.items():
    left.plot(x, result, label=get.__module__)
    right.semilogy(x, result / 5000.0 / x, label=get.__module__)

left.set_title("Cost for Entire graph")
right.set_title("Cost per edge")
left.set_ylabel("Duration (s)")
right.set_ylabel("Duration (s)")
left.set_xlabel("Number of edges per task")
right.set_xlabel("Number of edges per task")
plt.legend()
plt.savefig("images/scaling-edges.png")
