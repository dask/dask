from time import time
import dask
from dask import threaded, multiprocessing, local
from random import randint
import matplotlib.pyplot as plt


def noop(x):
    pass


nrepetitions = 1


def trivial(width, height):
    """ Embarrassingly parallel dask """
    d = {("x", 0, i): i for i in range(width)}
    for j in range(1, height):
        d.update({("x", j, i): (noop, ("x", j - 1, i)) for i in range(width)})
    return d, [("x", height - 1, i) for i in range(width)]


def crosstalk(width, height, connections):
    """ Natural looking dask with some inter-connections """
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
    """ Full barriers between each step """
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
for get in [dask.get, threaded.get, local.get_sync, multiprocessing.get]:
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

for get in trivial_results:
    left.loglog(x * 5, trivial_results[get], label=get.__module__)
    right.loglog(x * 5, trivial_results[get] / x, label=get.__module__)

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
for get in [threaded.get, local.get_sync]:
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

for get in crosstalk_results:
    left.plot(x, crosstalk_results[get], label=get.__module__)
    right.semilogy(x, crosstalk_results[get] / 5000.0 / x, label=get.__module__)

left.set_title("Cost for Entire graph")
right.set_title("Cost per edge")
left.set_ylabel("Duration (s)")
right.set_ylabel("Duration (s)")
left.set_xlabel("Number of edges per task")
right.set_xlabel("Number of edges per task")
plt.legend()
plt.savefig("images/scaling-edges.png")
