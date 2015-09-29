Distibuted 3
============

This project experiments with distributed computation.  It is the third in a
sequence of experiments


Basic Model
-----------

There are several *worker* processes on different nodes, they host a server over
tcp and generally respond to the following requests

1.  Serve data from a local dictionary of data, e.g.

        {'x': np.array(...),
         'y': np.array(...)}

    Operations include normal dictionary operations, like get, set, and delete
    key-value pairs.

2.  Perform arbitrary computations on that data, e.g.

        z <- add(x, y)  # can be done with only local data

    Also support getting data from other sources

        c <- add(x, a)  # need to find out where we can get 'a'

A special *center* process that keeps track of what data-keys are on
which workers. E.g.

    {'alice':   {'x', 'y'}
     'bob':     {'w', 'x'}
     'charlie': {'a', 'b', 'c'}}

All worker nodes in the same network have the same center node.  They update
and query this center node to share and learn what nodes have what data.  The
center node could conceptually be replaced by a Redis server.  While metadata
storage is centralized all data transfer is peer-to-peer.

    Alice:  Hey Center!  Who has a?
    Center: Hey Alice!   Charlie has a.
    Alice:  Hey Bob!     Send me a!
    Bob:    Hey Alice!   Here's a!


Client Model
------------

In principle one can connect to the worker and center servers with sockets to
manipulate them.  Convenience functions exist to scatter data, and run remote
procedures.

In practice though we probably want to wrap this system with various user
abstractions.  As a proof of concept we've implemented a *pool* abstraction
that respects data locality, i.e. computations prefer to take place on nodes
where the requisite data already resides.

Our goal isn't to produce a distributed pool though, nor to produce any
particular distributed client library, but rather to create a substrate upon
which several such projects could be built with minimal incidental pain.


Pool Example
------------

### Setup

    $ bin/dcenter                           # Center
    Started center at 127.0.0.1:8787

    $ bin/dworker 127.0.0.1:8787            # Worker 1
    Start worker at 127.0.0.1:8788
    Register with center at 127.0.0.1:8787

    $ bin/dworker 127.0.0.1:8787            # Worker 2
    Start worker at 127.0.0.1:8789
    Register with center at 127.0.0.1:8787

### Client Pool

```python
In [1]: from distributed3 import Pool

In [2]: pool = Pool('127.0.0.1:8787')
In [3]: pool.sync_center()

In [4]: pool.available_cores
Out[4]: {('127.0.0.1', 8788): 4, ('127.0.0.1', 8789): 4}

In [5]: A = pool.map(lambda x: x**2, range(10))

In [6]: B = pool.map(lambda x: -x, A)

In [7]: total = pool.apply(sum, [B])

In [8]: total.get()
Out[8]: -285
```

The results `A`, `B`, and `total` are kept remotely on the workers until
recalled explicitly with the `.get()` method.

```python
In [9]: A
Out[9]:
[RemoteData<center=127.0.0.1:8787, key=ad85f2de-6...>,
 RemoteData<center=127.0.0.1:8787, key=ad8686cc-6...>,
 RemoteData<center=127.0.0.1:8787, key=ad870de0-6...>,
 RemoteData<center=127.0.0.1:8787, key=ad878e14-6...>,
 RemoteData<center=127.0.0.1:8787, key=ad880b50-6...>,
 RemoteData<center=127.0.0.1:8787, key=ad881c30-6...>,
 RemoteData<center=127.0.0.1:8787, key=ad88219e-6...>,
 RemoteData<center=127.0.0.1:8787, key=ad88266c-6...>,
 RemoteData<center=127.0.0.1:8787, key=ad882ae0-6...>,
 RemoteData<center=127.0.0.1:8787, key=ad882f4a-6...>]

In [10]: pool.gather(A)
Out[10]: [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]
```

Computations on remote data, e.g. `B = pool.map(lambda x: -x, A)`, keep data on
the cluster.

```python
In [6]: B = pool.map(lambda x: -x, A)
```

Moreover they try to avoid worker-to-worker communication by
performing computations where the inputs are stored.

Data transfer does happen though when necessary, as in when we compute the sum
of all of `B`.  In this case we'll probably choose to perform the computation
on the node that has the greater number of elements of B on it.  All of the
other elements will be pulled by peer-to-peer transfer.

```python
In [7]: total = pool.apply(sum, [B])
```


Implementation
--------------

The conceptual model of workers and center has persisted since the original
implementation in `dask.distributed`.  The implementation has evolved
significantly over the various iterations of this project.

1.  `dask.distributed`:  threads and callbacks
2.  `dist`:  Actor model with threads and queues
3.  `distributed`:  Coroutine model with asyncio
4.  `distributed3`:  Coroutine model with tornado

We need a pleasant way to write somewhat complex interactions between nodes.
These interactions can not block because each node is expected to handle a
variety of operations concurrently:

1.  Compute several functions at once
2.  Serve data to other workers
3.  Manage heartbeats, etc..

One process should not block the others.  Furthermore we've found that relying
on system threads to be painful, particularly when debugging.  We've settled on
coroutines and an event loop.

Currently we use
[tornado coroutines](http://tornado.readthedocs.org/en/latest/coroutine.html)
on top of
[tornado networking](http://tornado.readthedocs.org/en/latest/networking.html).
We have found coroutines to be a pleasant way of writing complex
interactions between multiple systems that still operate concurrently and,
importantly, all within a single system thread, which significantly aids
debugging.  Additionally tornado is well used, has excellent documentation and
excellent StackOverflow and community coverage.


Future Work
-----------

Current work focuses around improving the user experience with the `Pool`
interface shown above, and the refactoring of the core infrastructure that
results.  Pool serves as a minimum viable client interface.  Problems and
solutions on pool should inform the design of more sophisticated systems in the
future.  We've found that moderately complex problems can be solved with only
moderately complex code, (a great improvement over previous systems), which is
strongly encouraging that we're on the right track.

I (matt) will probably then look towards a somewhat-simple data-local task
scheduler to replace dask.distributed.  However this is not the only path
forward.  My hope is that the code in `core/center/client/worker.py` is
sufficiently general to serve as a base for a variety of projects.
