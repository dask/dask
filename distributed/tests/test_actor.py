import operator
from time import sleep
from tornado import gen

import pytest

import dask
from distributed import Actor, ActorFuture, Client, Future, wait, Nanny
from distributed.utils_test import gen_cluster
from distributed.utils_test import client, cluster_fixture, loop  # noqa: F401
from distributed.metrics import time


class Counter(object):
    n = 0

    def __init__(self):
        self.n = 0

    def increment(self):
        self.n += 1
        return self.n

    def add(self, x):
        self.n += x
        return self.n


class List(object):
    L = []

    def __init__(self, dummy=None):
        self.L = []

    def append(self, x):
        self.L.append(x)


class ParameterServer(object):
    def __init__(self):
        self.data = {}

    def put(self, key, value):
        self.data[key] = value

    def get(self, key):
        return self.data[key]


@pytest.mark.parametrize("direct_to_workers", [True, False])
def test_client_actions(direct_to_workers):
    @gen_cluster(client=True)
    def test(c, s, a, b):
        c = yield Client(
            s.address, asynchronous=True, direct_to_workers=direct_to_workers
        )

        counter = c.submit(Counter, workers=[a.address], actor=True)
        assert isinstance(counter, Future)
        counter = yield counter
        assert counter._address
        assert hasattr(counter, "increment")
        assert hasattr(counter, "add")
        assert hasattr(counter, "n")

        n = yield counter.n
        assert n == 0

        assert counter._address == a.address

        assert isinstance(a.actors[counter.key], Counter)
        assert s.tasks[counter.key].actor

        yield [counter.increment(), counter.increment()]

        n = yield counter.n
        assert n == 2

        counter.add(10)
        while (yield counter.n) != 10 + 2:
            n = yield counter.n
            yield gen.sleep(0.01)

        yield c.close()

    test()


@pytest.mark.parametrize("separate_thread", [False, True])
def test_worker_actions(separate_thread):
    @gen_cluster(client=True)
    def test(c, s, a, b):
        counter = c.submit(Counter, workers=[a.address], actor=True)
        a_address = a.address

        def f(counter):
            start = counter.n

            assert type(counter) is Actor
            assert counter._address == a_address

            future = counter.increment(separate_thread=separate_thread)
            assert isinstance(future, ActorFuture)
            assert "Future" in type(future).__name__
            end = future.result(timeout=1)
            assert end > start

        futures = [c.submit(f, counter, pure=False) for _ in range(10)]
        yield futures

        counter = yield counter
        assert (yield counter.n) == 10

    test()


@gen_cluster(client=True)
def test_Actor(c, s, a, b):
    counter = yield c.submit(Counter, actor=True)

    assert counter._cls == Counter

    assert hasattr(counter, "n")
    assert hasattr(counter, "increment")
    assert hasattr(counter, "add")

    assert not hasattr(counter, "abc")


@pytest.mark.xfail(
    reason="Tornado can pass things out of order"
    + "Should rely on sending small messages rather than rpc"
)
@gen_cluster(client=True)
def test_linear_access(c, s, a, b):
    start = time()
    future = c.submit(sleep, 0.2)
    actor = c.submit(List, actor=True, dummy=future)
    actor = yield actor

    for i in range(100):
        actor.append(i)

    while True:
        yield gen.sleep(0.1)
        L = yield actor.L
        if len(L) == 100:
            break

    L = yield actor.L
    stop = time()
    assert L == tuple(range(100))

    assert stop - start > 0.2


@gen_cluster(client=True)
def test_exceptions_create(c, s, a, b):
    class Foo(object):
        x = 0

        def __init__(self):
            raise ValueError("bar")

    with pytest.raises(ValueError) as info:
        future = yield c.submit(Foo, actor=True)

    assert "bar" in str(info.value)


@gen_cluster(client=True)
def test_exceptions_method(c, s, a, b):
    class Foo(object):
        def throw(self):
            1 / 0

    foo = yield c.submit(Foo, actor=True)
    with pytest.raises(ZeroDivisionError):
        yield foo.throw()


@gen_cluster(client=True)
def test_gc(c, s, a, b):
    actor = c.submit(Counter, actor=True)
    yield wait(actor)
    del actor

    while a.actors or b.actors:
        yield gen.sleep(0.01)


@gen_cluster(client=True)
def test_track_dependencies(c, s, a, b):
    actor = c.submit(Counter, actor=True)
    yield wait(actor)
    x = c.submit(sleep, 0.5)
    y = c.submit(lambda x, y: x, x, actor)
    del actor

    yield gen.sleep(0.3)

    assert a.actors or b.actors


@gen_cluster(client=True)
def test_future(c, s, a, b):
    counter = c.submit(Counter, actor=True, workers=[a.address])
    assert isinstance(counter, Future)
    yield wait(counter)
    assert isinstance(a.actors[counter.key], Counter)

    counter = yield counter
    assert isinstance(counter, Actor)
    assert counter._address

    yield gen.sleep(0.1)
    assert counter.key in c.futures  # don't lose future


@gen_cluster(client=True)
def test_future_dependencies(c, s, a, b):
    counter = c.submit(Counter, actor=True, workers=[a.address])

    def f(a):
        assert isinstance(a, Actor)
        assert a._cls == Counter

    x = c.submit(f, counter, workers=[b.address])
    yield x

    assert {ts.key for ts in s.tasks[x.key].dependencies} == {counter.key}
    assert {ts.key for ts in s.tasks[counter.key].dependents} == {x.key}

    y = c.submit(f, counter, workers=[a.address], pure=False)
    yield y

    assert {ts.key for ts in s.tasks[y.key].dependencies} == {counter.key}
    assert {ts.key for ts in s.tasks[counter.key].dependents} == {x.key, y.key}


def test_sync(client):
    counter = client.submit(Counter, actor=True)
    counter = counter.result()

    assert counter.n == 0

    future = counter.increment()
    n = future.result()
    assert n == 1
    assert counter.n == 1

    assert future.result() == future.result()

    assert "ActorFuture" in repr(future)
    assert "distributed.actor" not in repr(future)


@gen_cluster(client=True, config={"distributed.comm.timeouts.connect": "1s"})
def test_failed_worker(c, s, a, b):
    future = c.submit(Counter, actor=True, workers=[a.address])
    yield wait(future)
    counter = yield future

    yield a.close()

    with pytest.raises(Exception) as info:
        yield counter.increment()

    assert "actor" in str(info.value).lower()
    assert "worker" in str(info.value).lower()
    assert "lost" in str(info.value).lower()


@gen_cluster(client=True)
def bench(c, s, a, b):
    counter = yield c.submit(Counter, actor=True)

    for i in range(1000):
        yield counter.increment()


@gen_cluster(client=True)
def test_numpy_roundtrip(c, s, a, b):
    np = pytest.importorskip("numpy")

    server = yield c.submit(ParameterServer, actor=True)

    x = np.random.random(1000)
    yield server.put("x", x)

    y = yield server.get("x")

    assert (x == y).all()


@gen_cluster(client=True)
def test_numpy_roundtrip_getattr(c, s, a, b):
    np = pytest.importorskip("numpy")

    counter = yield c.submit(Counter, actor=True)

    x = np.random.random(1000)

    yield counter.add(x)

    y = yield counter.n

    assert (x == y).all()


@gen_cluster(client=True)
def test_repr(c, s, a, b):
    counter = yield c.submit(Counter, actor=True)

    assert "Counter" in repr(counter)
    assert "Actor" in repr(counter)
    assert counter.key in repr(counter)
    assert "distributed.actor" not in repr(counter)


@gen_cluster(client=True)
def test_dir(c, s, a, b):
    counter = yield c.submit(Counter, actor=True)

    d = set(dir(counter))

    for attr in dir(Counter):
        if not attr.startswith("_"):
            assert attr in d


@gen_cluster(client=True)
def test_many_computations(c, s, a, b):
    counter = yield c.submit(Counter, actor=True)

    def add(n, counter):
        for i in range(n):
            counter.increment().result()

    futures = c.map(add, range(10), counter=counter)
    done = c.submit(lambda x: None, futures)

    while not done.done():
        assert len(s.processing) <= a.ncores + b.ncores
        yield gen.sleep(0.01)

    yield done


@gen_cluster(client=True, ncores=[("127.0.0.1", 5)] * 2)
def test_thread_safety(c, s, a, b):
    class Unsafe(object):
        def __init__(self):
            self.n = 0

        def f(self):
            assert self.n == 0
            self.n += 1

            for i in range(20):
                sleep(0.002)
                assert self.n == 1
            self.n = 0

    unsafe = yield c.submit(Unsafe, actor=True)

    futures = [unsafe.f() for i in range(10)]
    yield futures


@gen_cluster(client=True)
def test_Actors_create_dependencies(c, s, a, b):
    counter = yield c.submit(Counter, actor=True)
    future = c.submit(lambda x: None, counter)
    yield wait(future)
    assert s.tasks[future.key].dependencies == {s.tasks[counter.key]}


@gen_cluster(client=True)
def test_load_balance(c, s, a, b):
    class Foo(object):
        def __init__(self, x):
            pass

    b = c.submit(operator.mul, "b", 1000000)
    yield wait(b)
    [ws] = s.tasks[b.key].who_has

    x = yield c.submit(Foo, b, actor=True)
    y = yield c.submit(Foo, b, actor=True)
    assert x.key != y.key  # actors assumed not pure

    assert s.tasks[x.key].who_has == {ws}  # first went to best match
    assert s.tasks[x.key].who_has != s.tasks[y.key].who_has  # second load balanced


@gen_cluster(client=True, ncores=[("127.0.0.1", 1)] * 5)
def test_load_balance_map(c, s, *workers):
    class Foo(object):
        def __init__(self, x, y=None):
            pass

    b = c.submit(operator.mul, "b", 1000000)
    yield wait(b)

    actors = c.map(Foo, range(10), y=b, actor=True)
    yield wait(actors)

    assert all(len(w.actors) == 2 for w in workers)


@gen_cluster(client=True, ncores=[("127.0.0.1", 1)] * 4, Worker=Nanny)
def bench_param_server(c, s, *workers):
    import dask.array as da
    import numpy as np

    x = da.random.random((500000, 1000), chunks=(1000, 1000))
    x = x.persist()
    yield wait(x)

    class ParameterServer:
        data = None

        def __init__(self, n):
            self.data = np.random.random(n)

        def update(self, x):
            self.data += x
            self.data /= 2

        def get_data(self):
            return self.data

    def f(block, ps=None):
        start = time()
        params = ps.get_data(separate_thread=False).result()
        stop = time()
        update = (block - params).mean(axis=0)
        ps.update(update, separate_thread=False)
        print(format_time(stop - start))
        return np.array([[stop - start]])

    from distributed.utils import format_time

    start = time()
    ps = yield c.submit(ParameterServer, x.shape[1], actor=True)
    y = x.map_blocks(f, ps=ps, dtype=x.dtype)
    # result = yield c.compute(y.mean())
    yield wait(y.persist())
    end = time()
    print(format_time(end - start))


@pytest.mark.xfail(reason="unknown")
@gen_cluster(client=True)
def test_compute(c, s, a, b):
    @dask.delayed
    def f(n, counter):
        assert isinstance(counter, Actor)
        for i in range(n):
            counter.increment().result()

    @dask.delayed
    def check(counter, blanks):
        return counter.n

    counter = dask.delayed(Counter)()
    values = [f(i, counter) for i in range(5)]
    final = check(counter, values)

    result = yield c.compute(final, actors=counter)
    assert result == 0 + 1 + 2 + 3 + 4

    start = time()
    while a.data or b.data:
        yield gen.sleep(0.01)
        assert time() < start + 5


def test_compute_sync(client):
    @dask.delayed
    def f(n, counter):
        assert isinstance(counter, Actor), type(counter)
        for i in range(n):
            counter.increment().result()

    @dask.delayed
    def check(counter, blanks):
        return counter.n

    counter = dask.delayed(Counter)()
    values = [f(i, counter) for i in range(5)]
    final = check(counter, values)

    result = final.compute(actors=counter)
    assert result == 0 + 1 + 2 + 3 + 4

    def check(dask_worker):
        return len(dask_worker.data) + len(dask_worker.actors)

    start = time()
    while any(client.run(check).values()):
        sleep(0.01)
        assert time() < start + 2


@gen_cluster(
    client=True,
    ncores=[("127.0.0.1", 1)],
    config={"distributed.worker.profile.interval": "1ms"},
)
def test_actors_in_profile(c, s, a):
    class Sleeper(object):
        def sleep(self, time):
            sleep(time)

    sleeper = yield c.submit(Sleeper, actor=True)

    for i in range(5):
        yield sleeper.sleep(0.200)
        if (
            list(a.profile_recent["children"])[0].startswith("sleep")
            or "Sleeper.sleep" in a.profile_keys
        ):
            return
    assert False, list(a.profile_keys)


@gen_cluster(client=True)
def test_waiter(c, s, a, b):
    from tornado.locks import Event

    class Waiter(object):
        def __init__(self):
            self.event = Event()

        @gen.coroutine
        def set(self):
            self.event.set()

        @gen.coroutine
        def wait(self):
            yield self.event.wait()

    waiter = yield c.submit(Waiter, actor=True)

    futures = [waiter.wait() for i in range(5)]  # way more than we have actor threads

    yield gen.sleep(0.1)
    assert not any(future.done() for future in futures)

    yield waiter.set()

    yield futures
