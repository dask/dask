from __future__ import print_function, division, absolute_import

import cloudpickle
import pickle
from collections import defaultdict
from datetime import timedelta
import json
from operator import add, mul
import sys
from time import sleep

import dask
from dask import delayed
from toolz import merge, concat, valmap, first, frequencies
from tornado import gen

import pytest

from distributed import Nanny, Worker, Client, wait, fire_and_forget
from distributed.core import connect, rpc
from distributed.scheduler import Scheduler, BANDWIDTH
from distributed.client import wait
from distributed.metrics import time
from distributed.protocol.pickle import dumps
from distributed.worker import dumps_function, dumps_task
from distributed.utils import tmpfile
from distributed.utils_test import (
    inc,
    dec,
    gen_cluster,
    gen_test,
    slowinc,
    slowadd,
    slowdec,
    cluster,
    div,
    varying,
    slow,
)
from distributed.utils_test import loop, nodebug  # noqa: F401
from dask.compatibility import apply


alice = "alice:1234"
bob = "bob:1234"

occupancy = defaultdict(lambda: 0)


@gen_cluster()
def test_administration(s, a, b):
    assert isinstance(s.address, str)
    assert s.address in str(s)
    assert str(sum(s.ncores.values())) in repr(s)
    assert str(len(s.ncores)) in repr(s)


@gen_cluster(client=True, ncores=[("127.0.0.1", 1)])
def test_respect_data_in_memory(c, s, a):
    x = delayed(inc)(1)
    y = delayed(inc)(x)
    f = c.persist(y)
    yield wait([f])

    assert s.tasks[y.key].who_has == {s.workers[a.address]}

    z = delayed(add)(x, y)
    f2 = c.persist(z)
    while f2.key not in s.tasks or not s.tasks[f2.key]:
        assert s.tasks[y.key].who_has
        yield gen.sleep(0.0001)


@gen_cluster(client=True)
def test_recompute_released_results(c, s, a, b):
    x = delayed(inc)(1)
    y = delayed(inc)(x)

    yy = c.persist(y)
    yield wait(yy)

    while s.tasks[x.key].who_has or x.key in a.data or x.key in b.data:  # let x go away
        yield gen.sleep(0.01)

    z = delayed(dec)(x)
    zz = c.compute(z)
    result = yield zz
    assert result == 1


@gen_cluster(client=True)
def test_decide_worker_with_many_independent_leaves(c, s, a, b):
    xs = yield [
        c.scatter(list(range(0, 100, 2)), workers=a.address),
        c.scatter(list(range(1, 100, 2)), workers=b.address),
    ]
    xs = list(concat(zip(*xs)))
    ys = [delayed(inc)(x) for x in xs]

    y2s = c.persist(ys)
    yield wait(y2s)

    nhits = sum(y.key in a.data for y in y2s[::2]) + sum(
        y.key in b.data for y in y2s[1::2]
    )

    assert nhits > 80


@gen_cluster(client=True, ncores=[("127.0.0.1", 1)] * 3)
def test_decide_worker_with_restrictions(client, s, a, b, c):
    x = client.submit(inc, 1, workers=[a.address, b.address])
    yield wait(x)
    assert x.key in a.data or x.key in b.data


@gen_cluster(client=True, ncores=[("127.0.0.1", 1)] * 3)
def test_move_data_over_break_restrictions(client, s, a, b, c):
    [x] = yield client.scatter([1], workers=b.address)
    y = client.submit(inc, x, workers=[a.address, b.address])
    yield wait(y)
    assert y.key in a.data or y.key in b.data


@gen_cluster(client=True, ncores=[("127.0.0.1", 1)] * 3)
def test_balance_with_restrictions(client, s, a, b, c):
    [x], [y] = yield [
        client.scatter([[1, 2, 3]], workers=a.address),
        client.scatter([1], workers=c.address),
    ]
    z = client.submit(inc, 1, workers=[a.address, c.address])
    yield wait(z)

    assert s.tasks[z.key].who_has == {s.workers[c.address]}


@gen_cluster(client=True, ncores=[("127.0.0.1", 1)] * 3)
def test_no_valid_workers(client, s, a, b, c):
    x = client.submit(inc, 1, workers="127.0.0.5:9999")
    while not s.tasks:
        yield gen.sleep(0.01)

    assert s.tasks[x.key] in s.unrunnable

    with pytest.raises(gen.TimeoutError):
        yield gen.with_timeout(timedelta(milliseconds=50), x)


@gen_cluster(client=True, ncores=[("127.0.0.1", 1)] * 3)
def test_no_valid_workers_loose_restrictions(client, s, a, b, c):
    x = client.submit(inc, 1, workers="127.0.0.5:9999", allow_other_workers=True)

    result = yield x
    assert result == 2


@gen_cluster(client=True, ncores=[])
def test_no_workers(client, s):
    x = client.submit(inc, 1)
    while not s.tasks:
        yield gen.sleep(0.01)

    assert s.tasks[x.key] in s.unrunnable

    with pytest.raises(gen.TimeoutError):
        yield gen.with_timeout(timedelta(milliseconds=50), x)


@gen_cluster(ncores=[])
def test_retire_workers_empty(s):
    yield s.retire_workers(workers=[])


@gen_cluster()
def test_remove_client(s, a, b):
    s.update_graph(
        tasks={"x": dumps_task((inc, 1)), "y": dumps_task((inc, "x"))},
        dependencies={"x": [], "y": ["x"]},
        keys=["y"],
        client="ident",
    )

    assert s.tasks
    assert s.dependencies

    s.remove_client(client="ident")

    assert not s.tasks
    assert not s.dependencies


@gen_cluster()
def test_server_listens_to_other_ops(s, a, b):
    with rpc(s.address) as r:
        ident = yield r.identity()
        assert ident["type"] == "Scheduler"
        assert ident["id"].lower().startswith("scheduler")


@gen_cluster()
def test_remove_worker_from_scheduler(s, a, b):
    dsk = {("x-%d" % i): (inc, i) for i in range(20)}
    s.update_graph(
        tasks=valmap(dumps_task, dsk),
        keys=list(dsk),
        dependencies={k: set() for k in dsk},
    )

    assert a.address in s.stream_comms
    s.remove_worker(address=a.address)
    assert a.address not in s.ncores
    assert len(s.workers[b.address].processing) == len(dsk)  # b owns everything
    s.validate_state()


@gen_cluster(config={"distributed.scheduler.events-cleanup-delay": "10 ms"})
def test_clear_events_worker_removal(s, a, b):
    assert a.address in s.events
    assert a.address in s.ncores
    assert b.address in s.events
    assert b.address in s.ncores

    s.remove_worker(address=a.address)
    # Shortly after removal, the events should still be there
    assert a.address in s.events
    assert a.address not in s.ncores
    s.validate_state()

    start = time()
    while a.address in s.events:
        yield gen.sleep(0.01)
        assert time() < start + 2
    assert b.address in s.events


@gen_cluster(
    config={"distributed.scheduler.events-cleanup-delay": "10 ms"}, client=True
)
def test_clear_events_client_removal(c, s, a, b):
    assert c.id in s.events
    s.remove_client(c.id)

    assert c.id in s.events
    assert c.id not in s.clients
    assert c not in s.clients

    s.remove_client(c.id)
    # If it doesn't reconnect after a given time, the events log should be cleared
    start = time()
    while c.id in s.events:
        yield gen.sleep(0.01)
        assert time() < start + 2


@gen_cluster()
def test_add_worker(s, a, b):
    w = Worker(s.ip, s.port, ncores=3)
    w.data["x-5"] = 6
    w.data["y"] = 1
    yield w

    dsk = {("x-%d" % i): (inc, i) for i in range(10)}
    s.update_graph(
        tasks=valmap(dumps_task, dsk),
        keys=list(dsk),
        client="client",
        dependencies={k: set() for k in dsk},
    )

    s.add_worker(
        address=w.address, keys=list(w.data), ncores=w.ncores, services=s.services
    )

    s.validate_state()

    assert w.ip in s.host_info
    assert s.host_info[w.ip]["addresses"] == {a.address, b.address, w.address}
    yield w.close()


@gen_cluster(scheduler_kwargs={"blocked_handlers": ["feed"]})
def test_blocked_handlers_are_respected(s, a, b):
    def func(scheduler):
        return dumps(dict(scheduler.worker_info))

    comm = yield connect(s.address)
    yield comm.write({"op": "feed", "function": dumps(func), "interval": 0.01})

    response = yield comm.read()

    assert "exception" in response
    assert isinstance(response["exception"], ValueError)
    assert "'feed' handler has been explicitly disallowed" in repr(
        response["exception"]
    )

    yield comm.close()


def test_scheduler_init_pulls_blocked_handlers_from_config():
    with dask.config.set({"distributed.scheduler.blocked-handlers": ["test-handler"]}):
        s = Scheduler()
    assert s.blocked_handlers == ["test-handler"]


@gen_cluster()
def test_feed(s, a, b):
    def func(scheduler):
        return dumps(dict(scheduler.worker_info))

    comm = yield connect(s.address)
    yield comm.write({"op": "feed", "function": dumps(func), "interval": 0.01})

    for i in range(5):
        response = yield comm.read()
        expected = dict(s.worker_info)
        assert cloudpickle.loads(response) == expected

    yield comm.close()


@gen_cluster()
def test_feed_setup_teardown(s, a, b):
    def setup(scheduler):
        return 1

    def func(scheduler, state):
        assert state == 1
        return "OK"

    def teardown(scheduler, state):
        scheduler.flag = "done"

    comm = yield connect(s.address)
    yield comm.write(
        {
            "op": "feed",
            "function": dumps(func),
            "setup": dumps(setup),
            "teardown": dumps(teardown),
            "interval": 0.01,
        }
    )

    for i in range(5):
        response = yield comm.read()
        assert response == "OK"

    yield comm.close()
    start = time()
    while not hasattr(s, "flag"):
        yield gen.sleep(0.01)
        assert time() - start < 5


@gen_cluster()
def test_feed_large_bytestring(s, a, b):
    np = pytest.importorskip("numpy")

    x = np.ones(10000000)

    def func(scheduler):
        y = x
        return True

    comm = yield connect(s.address)
    yield comm.write({"op": "feed", "function": dumps(func), "interval": 0.05})

    for i in range(5):
        response = yield comm.read()
        assert response is True

    yield comm.close()


@gen_cluster(client=True)
def test_delete_data(c, s, a, b):
    d = yield c.scatter({"x": 1, "y": 2, "z": 3})

    assert {ts.key for ts in s.tasks.values() if ts.who_has} == {"x", "y", "z"}
    assert set(a.data) | set(b.data) == {"x", "y", "z"}
    assert merge(a.data, b.data) == {"x": 1, "y": 2, "z": 3}

    del d["x"]
    del d["y"]

    start = time()
    while set(a.data) | set(b.data) != {"z"}:
        yield gen.sleep(0.01)
        assert time() < start + 5


@gen_cluster(client=True, ncores=[("127.0.0.1", 1)])
def test_delete(c, s, a):
    x = c.submit(inc, 1)
    yield x
    assert x.key in a.data

    yield c._cancel(x)

    start = time()
    while x.key in a.data:
        yield gen.sleep(0.01)
        assert time() < start + 5


@gen_cluster()
def test_filtered_communication(s, a, b):
    c = yield connect(s.address)
    f = yield connect(s.address)
    yield c.write({"op": "register-client", "client": "c"})
    yield f.write({"op": "register-client", "client": "f"})
    yield c.read()
    yield f.read()

    assert set(s.client_comms) == {"c", "f"}

    yield c.write(
        {
            "op": "update-graph",
            "tasks": {"x": dumps_task((inc, 1)), "y": dumps_task((inc, "x"))},
            "dependencies": {"x": [], "y": ["x"]},
            "client": "c",
            "keys": ["y"],
        }
    )

    yield f.write(
        {
            "op": "update-graph",
            "tasks": {"x": dumps_task((inc, 1)), "z": dumps_task((add, "x", 10))},
            "dependencies": {"x": [], "z": ["x"]},
            "client": "f",
            "keys": ["z"],
        }
    )

    msg, = yield c.read()
    assert msg["op"] == "key-in-memory"
    assert msg["key"] == "y"
    msg, = yield f.read()
    assert msg["op"] == "key-in-memory"
    assert msg["key"] == "z"


def test_dumps_function():
    a = dumps_function(inc)
    assert cloudpickle.loads(a)(10) == 11

    b = dumps_function(inc)
    assert a is b

    c = dumps_function(dec)
    assert a != c


def test_dumps_task():
    d = dumps_task((inc, 1))
    assert set(d) == {"function", "args"}

    f = lambda x, y=2: x + y
    d = dumps_task((apply, f, (1,), {"y": 10}))
    assert cloudpickle.loads(d["function"])(1, 2) == 3
    assert cloudpickle.loads(d["args"]) == (1,)
    assert cloudpickle.loads(d["kwargs"]) == {"y": 10}

    d = dumps_task((apply, f, (1,)))
    assert cloudpickle.loads(d["function"])(1, 2) == 3
    assert cloudpickle.loads(d["args"]) == (1,)
    assert set(d) == {"function", "args"}


@gen_cluster()
def test_ready_remove_worker(s, a, b):
    s.update_graph(
        tasks={"x-%d" % i: dumps_task((inc, i)) for i in range(20)},
        keys=["x-%d" % i for i in range(20)],
        client="client",
        dependencies={"x-%d" % i: [] for i in range(20)},
    )

    assert all(len(w.processing) > w.ncores for w in s.workers.values())

    s.remove_worker(address=a.address)

    assert set(s.workers) == {b.address}
    assert all(len(w.processing) > w.ncores for w in s.workers.values())


@gen_cluster(client=True, Worker=Nanny)
def test_restart(c, s, a, b):
    futures = c.map(inc, range(20))
    yield wait(futures)

    yield s.restart()

    assert len(s.workers) == 2

    for ws in s.workers.values():
        assert not ws.occupancy
        assert not ws.processing

    assert not s.tasks
    assert not s.dependencies


@gen_cluster()
def test_broadcast(s, a, b):
    result = yield s.broadcast(msg={"op": "ping"})
    assert result == {a.address: b"pong", b.address: b"pong"}

    result = yield s.broadcast(msg={"op": "ping"}, workers=[a.address])
    assert result == {a.address: b"pong"}

    result = yield s.broadcast(msg={"op": "ping"}, hosts=[a.ip])
    assert result == {a.address: b"pong", b.address: b"pong"}


@gen_cluster(Worker=Nanny)
def test_broadcast_nanny(s, a, b):
    result1 = yield s.broadcast(msg={"op": "identity"}, nanny=True)
    assert all(d["type"] == "Nanny" for d in result1.values())

    result2 = yield s.broadcast(
        msg={"op": "identity"}, workers=[a.worker_address], nanny=True
    )
    assert len(result2) == 1
    assert first(result2.values())["id"] == a.id

    result3 = yield s.broadcast(msg={"op": "identity"}, hosts=[a.ip], nanny=True)
    assert result1 == result3


@gen_test()
def test_worker_name():
    s = Scheduler(validate=True)
    s.start(0)
    w = yield Worker(s.ip, s.port, name="alice")
    assert s.workers[w.address].name == "alice"
    assert s.aliases["alice"] == w.address

    with pytest.raises(ValueError):
        w2 = yield Worker(s.ip, s.port, name="alice")
        yield w2.close()

    yield w.close()
    yield s.close()


@gen_test()
def test_coerce_address():
    with dask.config.set({"distributed.comm.timeouts.connect": "100ms"}):
        s = Scheduler(validate=True)
        s.start(0)
        print("scheduler:", s.address, s.listen_address)
        a = Worker(s.ip, s.port, name="alice")
        b = Worker(s.ip, s.port, name=123)
        c = Worker("127.0.0.1", s.port, name="charlie")
        yield [a, b, c]

        assert s.coerce_address("127.0.0.1:8000") == "tcp://127.0.0.1:8000"
        assert s.coerce_address("[::1]:8000") == "tcp://[::1]:8000"
        assert s.coerce_address("tcp://127.0.0.1:8000") == "tcp://127.0.0.1:8000"
        assert s.coerce_address("tcp://[::1]:8000") == "tcp://[::1]:8000"
        assert s.coerce_address("localhost:8000") in (
            "tcp://127.0.0.1:8000",
            "tcp://[::1]:8000",
        )
        assert s.coerce_address(u"localhost:8000") in (
            "tcp://127.0.0.1:8000",
            "tcp://[::1]:8000",
        )
        assert s.coerce_address(a.address) == a.address
        # Aliases
        assert s.coerce_address("alice") == a.address
        assert s.coerce_address(123) == b.address
        assert s.coerce_address("charlie") == c.address

        assert s.coerce_hostname("127.0.0.1") == "127.0.0.1"
        assert s.coerce_hostname("alice") == a.ip
        assert s.coerce_hostname(123) == b.ip
        assert s.coerce_hostname("charlie") == c.ip
        assert s.coerce_hostname("jimmy") == "jimmy"

        assert s.coerce_address("zzzt:8000", resolve=False) == "tcp://zzzt:8000"

        yield s.close()
        yield [w.close() for w in [a, b, c]]


@pytest.mark.skipif(
    sys.platform.startswith("win"), reason="file descriptors not really a thing"
)
@gen_cluster(ncores=[])
def test_file_descriptors_dont_leak(s):
    psutil = pytest.importorskip("psutil")
    proc = psutil.Process()
    before = proc.num_fds()

    w = yield Worker(s.ip, s.port)
    yield w.close()

    during = proc.num_fds()

    start = time()
    while proc.num_fds() > before:
        yield gen.sleep(0.01)
        assert time() < start + 5


@gen_cluster()
def test_update_graph_culls(s, a, b):
    s.update_graph(
        tasks={
            "x": dumps_task((inc, 1)),
            "y": dumps_task((inc, "x")),
            "z": dumps_task((inc, 2)),
        },
        keys=["y"],
        dependencies={"y": "x", "x": [], "z": []},
        client="client",
    )
    assert "z" not in s.tasks
    assert "z" not in s.dependencies


@gen_cluster(ncores=[])
def test_add_worker_is_idempotent(s):
    s.add_worker(address=alice, ncores=1, resolve_address=False)
    ncores = dict(s.ncores)
    s.add_worker(address=alice, resolve_address=False)
    assert s.ncores == s.ncores


def test_io_loop(loop):
    s = Scheduler(loop=loop, validate=True)
    assert s.io_loop is loop


@gen_cluster(client=True)
def test_story(c, s, a, b):
    x = delayed(inc)(1)
    y = delayed(inc)(x)
    f = c.persist(y)
    yield wait([f])

    assert s.transition_log

    story = s.story(x.key)
    assert all(line in s.transition_log for line in story)
    assert len(story) < len(s.transition_log)
    assert all(x.key == line[0] or x.key in line[-2] for line in story)

    assert len(s.story(x.key, y.key)) > len(story)


@gen_cluster(ncores=[], client=True)
def test_scatter_no_workers(c, s):
    with pytest.raises(gen.TimeoutError):
        yield s.scatter(data={"x": 1}, client="alice", timeout=0.1)

    start = time()
    with pytest.raises(gen.TimeoutError):
        yield c.scatter(123, timeout=0.1)
    assert time() < start + 1.5

    w = Worker(s.ip, s.port, ncores=3)
    yield [c.scatter(data={"y": 2}, timeout=5), w._start()]

    assert w.data["y"] == 2
    yield w.close()


@gen_cluster(ncores=[])
def test_scheduler_sees_memory_limits(s):
    w = yield Worker(s.ip, s.port, ncores=3, memory_limit=12345)

    assert s.workers[w.address].memory_limit == 12345
    yield w.close()


@gen_cluster(client=True, timeout=1000)
def test_retire_workers(c, s, a, b):
    [x] = yield c.scatter([1], workers=a.address)
    [y] = yield c.scatter([list(range(1000))], workers=b.address)

    assert s.workers_to_close() == [a.address]

    workers = yield s.retire_workers()
    assert list(workers) == [a.address]
    assert workers[a.address]["ncores"] == a.ncores
    assert list(s.ncores) == [b.address]

    assert s.workers_to_close() == []

    assert s.workers[b.address].has_what == {s.tasks[x.key], s.tasks[y.key]}

    workers = yield s.retire_workers()
    assert not workers


@gen_cluster(client=True)
def test_retire_workers_n(c, s, a, b):
    yield s.retire_workers(n=1, close_workers=True)
    assert len(s.workers) == 1

    yield s.retire_workers(n=0, close_workers=True)
    assert len(s.workers) == 1

    yield s.retire_workers(n=1, close_workers=True)
    assert len(s.workers) == 0

    yield s.retire_workers(n=0, close_workers=True)
    assert len(s.workers) == 0

    while not (a.status.startswith("clos") and b.status.startswith("clos")):
        yield gen.sleep(0.01)


@gen_cluster(client=True, ncores=[("127.0.0.1", 1)] * 4)
def test_workers_to_close(cl, s, *workers):
    s.task_duration["a"] = 4
    s.task_duration["b"] = 4
    s.task_duration["c"] = 1

    futures = cl.map(slowinc, [1, 1, 1], key=["a-4", "b-4", "c-1"])
    while sum(len(w.processing) for w in s.workers.values()) < 3:
        yield gen.sleep(0.001)

    wtc = s.workers_to_close()
    assert all(not s.workers[w].processing for w in wtc)
    assert len(wtc) == 1


@gen_cluster(client=True, ncores=[("127.0.0.1", 1)] * 4)
def test_workers_to_close_grouped(c, s, *workers):
    groups = {
        workers[0].address: "a",
        workers[1].address: "a",
        workers[2].address: "b",
        workers[3].address: "b",
    }

    def key(ws):
        return groups[ws.address]

    assert set(s.workers_to_close(key=key)) == set(w.address for w in workers)

    # Assert that job in one worker blocks closure of group
    future = c.submit(slowinc, 1, delay=0.2, workers=workers[0].address)
    while len(s.rprocessing) < 1:
        yield gen.sleep(0.001)

    assert set(s.workers_to_close(key=key)) == {workers[2].address, workers[3].address}

    del future

    while len(s.rprocessing) > 0:
        yield gen.sleep(0.001)

    # Assert that *total* byte count in group determines group priority
    av = yield c.scatter("a" * 100, workers=workers[0].address)
    bv = yield c.scatter("b" * 75, workers=workers[2].address)
    bv2 = yield c.scatter("b" * 75, workers=workers[3].address)

    assert set(s.workers_to_close(key=key)) == {workers[0].address, workers[1].address}


@gen_cluster(client=True)
def test_retire_workers_no_suspicious_tasks(c, s, a, b):
    future = c.submit(
        slowinc, 100, delay=0.5, workers=a.address, allow_other_workers=True
    )
    yield gen.sleep(0.2)
    yield s.retire_workers(workers=[a.address])

    assert all(ts.suspicious == 0 for ts in s.tasks.values())


@slow
@pytest.mark.skipif(
    sys.platform.startswith("win"), reason="file descriptors not really a thing"
)
@pytest.mark.skipif(sys.version_info < (3, 6), reason="intermittent failure")
@gen_cluster(client=True, ncores=[], timeout=240)
def test_file_descriptors(c, s):
    yield gen.sleep(0.1)
    psutil = pytest.importorskip("psutil")
    da = pytest.importorskip("dask.array")
    proc = psutil.Process()
    num_fds_1 = proc.num_fds()

    N = 20
    nannies = yield [Nanny(s.ip, s.port, loop=s.loop) for i in range(N)]

    while len(s.ncores) < N:
        yield gen.sleep(0.1)

    num_fds_2 = proc.num_fds()

    yield gen.sleep(0.2)

    num_fds_3 = proc.num_fds()
    assert num_fds_3 <= num_fds_2 + N  # add some heartbeats

    x = da.random.random(size=(1000, 1000), chunks=(25, 25))
    x = c.persist(x)
    yield wait(x)

    num_fds_4 = proc.num_fds()
    assert num_fds_4 <= num_fds_2 + 2 * N

    y = c.persist(x + x.T)
    yield wait(y)

    num_fds_5 = proc.num_fds()
    assert num_fds_5 < num_fds_4 + N

    yield gen.sleep(1)

    num_fds_6 = proc.num_fds()
    assert num_fds_6 < num_fds_5 + N

    yield [n.close() for n in nannies]

    assert not s.rpc.open
    assert not c.rpc.active
    assert not s.stream_comms

    start = time()
    while proc.num_fds() > num_fds_1 + N:
        yield gen.sleep(0.01)
        assert time() < start + 3


@slow
@nodebug
@gen_cluster(client=True)
def test_learn_occupancy(c, s, a, b):
    futures = c.map(slowinc, range(1000), delay=0.2)
    while sum(len(ts.who_has) for ts in s.tasks.values()) < 10:
        yield gen.sleep(0.01)

    assert 100 < s.total_occupancy < 1000
    for w in [a, b]:
        assert 50 < s.workers[w.address].occupancy < 700


@slow
@nodebug
@gen_cluster(client=True)
def test_learn_occupancy_2(c, s, a, b):
    future = c.map(slowinc, range(1000), delay=0.2)
    while not any(ts.who_has for ts in s.tasks.values()):
        yield gen.sleep(0.01)

    assert 100 < s.total_occupancy < 1000


@gen_cluster(client=True)
def test_occupancy_cleardown(c, s, a, b):
    s.validate = False

    # Inject excess values in s.occupancy
    s.workers[a.address].occupancy = 2
    s.total_occupancy += 2
    futures = c.map(slowinc, range(100), delay=0.01)
    yield wait(futures)

    # Verify that occupancy values have been zeroed out
    assert abs(s.total_occupancy) < 0.01
    assert all(ws.occupancy == 0 for ws in s.workers.values())


@nodebug
@gen_cluster(client=True, ncores=[("127.0.0.1", 1)] * 30)
def test_balance_many_workers(c, s, *workers):
    futures = c.map(slowinc, range(20), delay=0.2)
    yield wait(futures)
    assert {len(w.has_what) for w in s.workers.values()} == {0, 1}


@nodebug
@gen_cluster(client=True, ncores=[("127.0.0.1", 1)] * 30)
def test_balance_many_workers_2(c, s, *workers):
    s.extensions["stealing"]._pc.callback_time = 100000000
    futures = c.map(slowinc, range(90), delay=0.2)
    yield wait(futures)
    assert {len(w.has_what) for w in s.workers.values()} == {3}


@gen_cluster(client=True)
def test_learn_occupancy_multiple_workers(c, s, a, b):
    x = c.submit(slowinc, 1, delay=0.2, workers=a.address)
    yield gen.sleep(0.05)
    futures = c.map(slowinc, range(100), delay=0.2)

    yield wait(x)

    assert not any(v == 0.5 for w in s.workers.values() for v in w.processing.values())
    s.validate_state()


@gen_cluster(client=True)
def test_include_communication_in_occupancy(c, s, a, b):
    s.task_duration["slowadd"] = 0.001
    x = c.submit(mul, b"0", int(BANDWIDTH), workers=a.address)
    y = c.submit(mul, b"1", int(BANDWIDTH * 1.5), workers=b.address)

    z = c.submit(slowadd, x, y, delay=1)
    while z.key not in s.tasks or not s.tasks[z.key].processing_on:
        yield gen.sleep(0.01)

    ts = s.tasks[z.key]
    assert ts.processing_on == s.workers[b.address]
    assert s.workers[b.address].processing[ts] > 1
    yield wait(z)
    del z


@gen_cluster(client=True)
def test_worker_arrives_with_processing_data(c, s, a, b):
    x = delayed(slowinc)(1, delay=0.4)
    y = delayed(slowinc)(x, delay=0.4)
    z = delayed(slowinc)(y, delay=0.4)

    yy, zz = c.persist([y, z])

    while not any(w.processing for w in s.workers.values()):
        yield gen.sleep(0.01)

    w = Worker(s.ip, s.port, ncores=1)
    w.put_key_in_memory(y.key, 3)

    yield w

    start = time()

    while len(s.workers) < 3:
        yield gen.sleep(0.01)

    assert s.get_task_status(keys={x.key, y.key, z.key}) == {
        x.key: "released",
        y.key: "memory",
        z.key: "processing",
    }

    yield w.close()


@gen_cluster(client=True, ncores=[("127.0.0.1", 1)])
def test_worker_breaks_and_returns(c, s, a):
    future = c.submit(slowinc, 1, delay=0.1)
    for i in range(10):
        future = c.submit(slowinc, future, delay=0.1)

    yield wait(future)

    a.batched_stream.comm.close()

    yield gen.sleep(0.1)
    start = time()
    yield wait(future, timeout=10)
    end = time()

    assert end - start < 1

    states = frequencies(ts.state for ts in s.tasks.values())
    assert states == {"memory": 1, "released": 10}


@gen_cluster(client=True, ncores=[])
def test_no_workers_to_memory(c, s):
    x = delayed(slowinc)(1, delay=0.4)
    y = delayed(slowinc)(x, delay=0.4)
    z = delayed(slowinc)(y, delay=0.4)

    yy, zz = c.persist([y, z])

    while not s.tasks:
        yield gen.sleep(0.01)

    w = Worker(s.ip, s.port, ncores=1)
    w.put_key_in_memory(y.key, 3)

    yield w

    start = time()

    while not s.workers:
        yield gen.sleep(0.01)

    assert s.get_task_status(keys={x.key, y.key, z.key}) == {
        x.key: "released",
        y.key: "memory",
        z.key: "processing",
    }

    yield w.close()


@gen_cluster(client=True)
def test_no_worker_to_memory_restrictions(c, s, a, b):
    x = delayed(slowinc)(1, delay=0.4)
    y = delayed(slowinc)(x, delay=0.4)
    z = delayed(slowinc)(y, delay=0.4)

    yy, zz = c.persist([y, z], workers={(x, y, z): "alice"})

    while not s.tasks:
        yield gen.sleep(0.01)

    w = Worker(s.ip, s.port, ncores=1, name="alice")
    w.put_key_in_memory(y.key, 3)

    yield w

    while len(s.workers) < 3:
        yield gen.sleep(0.01)
    yield gen.sleep(0.3)

    assert s.get_task_status(keys={x.key, y.key, z.key}) == {
        x.key: "released",
        y.key: "memory",
        z.key: "processing",
    }

    yield w.close()


def test_run_on_scheduler_sync(loop):
    def f(dask_scheduler=None):
        return dask_scheduler.address

    with cluster() as (s, [a, b]):
        with Client(s["address"], loop=loop) as c:
            address = c.run_on_scheduler(f)
            assert address == s["address"]

            with pytest.raises(ZeroDivisionError):
                c.run_on_scheduler(div, 1, 0)


@gen_cluster(client=True)
def test_run_on_scheduler(c, s, a, b):
    def f(dask_scheduler=None):
        return dask_scheduler.address

    response = yield c._run_on_scheduler(f)
    assert response == s.address


@gen_cluster(client=True)
def test_close_worker(c, s, a, b):
    assert len(s.workers) == 2

    yield s.close_worker(worker=a.address)

    assert len(s.workers) == 1
    assert a.address not in s.workers

    yield gen.sleep(0.5)

    assert len(s.workers) == 1


@slow
@gen_cluster(client=True, Worker=Nanny, timeout=20)
def test_close_nanny(c, s, a, b):
    assert len(s.workers) == 2

    assert a.process.is_alive()
    a_worker_address = a.worker_address
    start = time()
    yield s.close_worker(worker=a_worker_address)

    assert len(s.workers) == 1
    assert a_worker_address not in s.workers

    start = time()
    while a.is_alive():
        yield gen.sleep(0.1)
        assert time() < start + 5

    assert a.pid is None

    for i in range(10):
        yield gen.sleep(0.1)
        assert len(s.workers) == 1
        assert not a.is_alive()
        assert a.pid is None

    while a.status != "closed":
        yield gen.sleep(0.05)
        assert time() < start + 10


@gen_cluster(client=True, timeout=20)
def test_retire_workers_close(c, s, a, b):
    yield s.retire_workers(close_workers=True)
    assert not s.workers
    while a.status != "closed" and b.status != "closed":
        yield gen.sleep(0.01)


@gen_cluster(client=True, timeout=20, Worker=Nanny)
def test_retire_nannies_close(c, s, a, b):
    nannies = [a, b]
    yield s.retire_workers(close_workers=True, remove=True)
    assert not s.workers

    start = time()

    while any(n.status != "closed" for n in nannies):
        yield gen.sleep(0.05)
        assert time() < start + 10

    assert not any(n.is_alive() for n in nannies)
    assert not s.workers


@gen_cluster(client=True, ncores=[("127.0.0.1", 2)])
def test_fifo_submission(c, s, w):
    futures = []
    for i in range(20):
        future = c.submit(slowinc, i, delay=0.1, key="inc-%02d" % i, fifo_timeout=0.01)
        futures.append(future)
        yield gen.sleep(0.02)
    yield wait(futures[-1])
    assert futures[10].status == "finished"


@gen_test()
def test_scheduler_file():
    with tmpfile() as fn:
        s = Scheduler(scheduler_file=fn)
        s.start(0)
        with open(fn) as f:
            data = json.load(f)
        assert data["address"] == s.address

        c = yield Client(scheduler_file=fn, loop=s.loop, asynchronous=True)
    yield s.close()


@pytest.mark.xfail(reason="")
@gen_cluster(client=True, ncores=[])
def test_non_existent_worker(c, s):
    with dask.config.set({"distributed.comm.timeouts.connect": "100ms"}):
        s.add_worker(address="127.0.0.1:5738", ncores=2, nbytes={}, host_info={})
        futures = c.map(inc, range(10))
        yield gen.sleep(0.300)
        assert not s.workers
        assert all(ts.state == "no-worker" for ts in s.tasks.values())


@gen_cluster(client=True, ncores=[("127.0.0.1", 1)] * 3)
def test_correct_bad_time_estimate(c, s, *workers):
    future = c.submit(slowinc, 1, delay=0)
    yield wait(future)

    futures = [c.submit(slowinc, future, delay=0.1, pure=False) for i in range(20)]

    yield gen.sleep(0.5)

    yield wait(futures)

    assert all(w.data for w in workers), [sorted(w.data) for w in workers]


@gen_test()
def test_service_hosts():
    pytest.importorskip("bokeh")
    from distributed.bokeh.scheduler import BokehScheduler

    port = 0
    for url, expected in [
        ("tcp://0.0.0.0", ("::", "0.0.0.0")),
        ("tcp://127.0.0.1", "127.0.0.1"),
        ("tcp://127.0.0.1:38275", "127.0.0.1"),
    ]:
        services = {("bokeh", port): BokehScheduler}

        s = Scheduler(services=services)
        yield s.start(url)

        sock = first(s.services["bokeh"].server._http._sockets.values())
        if isinstance(expected, tuple):
            assert sock.getsockname()[0] in expected
        else:
            assert sock.getsockname()[0] == expected
        yield s.close()

    port = ("127.0.0.1", 0)
    for url in ["tcp://0.0.0.0", "tcp://127.0.0.1", "tcp://127.0.0.1:38275"]:
        services = {("bokeh", port): BokehScheduler}

        s = Scheduler(services=services)
        yield s.start(url)

        sock = first(s.services["bokeh"].server._http._sockets.values())
        assert sock.getsockname()[0] == "127.0.0.1"
        yield s.close()


@gen_cluster(client=True, worker_kwargs={"profile_cycle_interval": 100})
def test_profile_metadata(c, s, a, b):
    start = time() - 1
    futures = c.map(slowinc, range(10), delay=0.05, workers=a.address)
    yield wait(futures)
    yield gen.sleep(0.200)

    meta = yield s.get_profile_metadata(profile_cycle_interval=0.100)
    now = time() + 1
    assert meta
    assert all(start < t < now for t, count in meta["counts"])
    assert all(0 <= count < 30 for t, count in meta["counts"][:4])
    assert not meta["counts"][-1][1]


@gen_cluster(client=True, worker_kwargs={"profile_cycle_interval": 100})
def test_profile_metadata_keys(c, s, a, b):
    start = time() - 1
    x = c.map(slowinc, range(10), delay=0.05)
    y = c.map(slowdec, range(10), delay=0.05)
    yield wait(x + y)

    meta = yield s.get_profile_metadata(profile_cycle_interval=0.100)
    assert set(meta["keys"]) == {"slowinc", "slowdec"}
    assert len(meta["counts"]) == len(meta["keys"]["slowinc"])


@gen_cluster(client=True)
def test_cancel_fire_and_forget(c, s, a, b):
    x = delayed(slowinc)(1, delay=0.05)
    y = delayed(slowinc)(x, delay=0.05)
    z = delayed(slowinc)(y, delay=0.05)
    w = delayed(slowinc)(z, delay=0.05)
    future = c.compute(w)
    fire_and_forget(future)

    yield gen.sleep(0.05)
    yield future.cancel(force=True)
    assert future.status == "cancelled"
    assert not s.tasks


@gen_cluster(client=True, Worker=Nanny)
def test_log_tasks_during_restart(c, s, a, b):
    future = c.submit(sys.exit, 0)
    yield wait(future)
    assert "exit" in str(s.events)


@gen_cluster(client=True, ncores=[("127.0.0.1", 1)] * 2)
def test_reschedule(c, s, a, b):
    yield c.submit(slowinc, -1, delay=0.1)  # learn cost
    x = c.map(slowinc, range(4), delay=0.1)

    # add much more work onto worker a
    futures = c.map(slowinc, range(10, 20), delay=0.1, workers=a.address)

    while len(s.tasks) < len(x) + len(futures):
        yield gen.sleep(0.001)

    for future in x:
        s.reschedule(key=future.key)

    # Worker b gets more of the original tasks
    yield wait(x)
    assert sum(future.key in b.data for future in x) >= 3
    assert sum(future.key in a.data for future in x) <= 1


@gen_cluster(client=True)
def test_get_task_status(c, s, a, b):
    future = c.submit(inc, 1)
    yield wait(future)

    result = yield a.scheduler.get_task_status(keys=[future.key])
    assert result == {future.key: "memory"}


def test_deque_handler():
    from distributed.scheduler import logger

    s = Scheduler()
    deque_handler = s._deque_handler
    logger.info("foo123")
    assert len(deque_handler.deque) >= 1
    msg = deque_handler.deque[-1]
    assert "distributed.scheduler" in deque_handler.format(msg)
    assert any(msg.msg == "foo123" for msg in deque_handler.deque)


@gen_cluster(client=True)
def test_retries(c, s, a, b):
    args = [ZeroDivisionError("one"), ZeroDivisionError("two"), 42]

    future = c.submit(varying(args), retries=3)
    result = yield future
    assert result == 42
    assert s.tasks[future.key].retries == 1
    assert future.key not in s.exceptions

    future = c.submit(varying(args), retries=2, pure=False)
    result = yield future
    assert result == 42
    assert s.tasks[future.key].retries == 0
    assert future.key not in s.exceptions

    future = c.submit(varying(args), retries=1, pure=False)
    with pytest.raises(ZeroDivisionError) as exc_info:
        res = yield future
    exc_info.match("two")

    future = c.submit(varying(args), retries=0, pure=False)
    with pytest.raises(ZeroDivisionError) as exc_info:
        res = yield future
    exc_info.match("one")


@pytest.mark.xfail(reason="second worker also errant for some reason")
@gen_cluster(client=True, ncores=[("127.0.0.1", 1)] * 3, timeout=5)
def test_mising_data_errant_worker(c, s, w1, w2, w3):
    with dask.config.set({"distributed.comm.timeouts.connect": "1s"}):
        np = pytest.importorskip("numpy")

        x = c.submit(np.random.random, 10000000, workers=w1.address)
        yield wait(x)
        yield c.replicate(x, workers=[w1.address, w2.address])

        y = c.submit(len, x, workers=w3.address)
        while not w3.tasks:
            yield gen.sleep(0.001)
        w1.close()
        yield wait(y)


@gen_cluster(client=True)
def test_dont_recompute_if_persisted(c, s, a, b):
    x = delayed(inc)(1, dask_key_name="x")
    y = delayed(inc)(x, dask_key_name="y")

    yy = y.persist()
    yield wait(yy)

    old = list(s.transition_log)

    yyy = y.persist()
    yield wait(yyy)

    yield gen.sleep(0.100)
    assert list(s.transition_log) == old


@gen_cluster(client=True)
def test_dont_recompute_if_persisted_2(c, s, a, b):
    x = delayed(inc)(1, dask_key_name="x")
    y = delayed(inc)(x, dask_key_name="y")
    z = delayed(inc)(y, dask_key_name="z")

    yy = y.persist()
    yield wait(yy)

    old = s.story("x", "y")

    zz = z.persist()
    yield wait(zz)

    yield gen.sleep(0.100)
    assert s.story("x", "y") == old


@gen_cluster(client=True)
def test_dont_recompute_if_persisted_3(c, s, a, b):
    x = delayed(inc)(1, dask_key_name="x")
    y = delayed(inc)(2, dask_key_name="y")
    z = delayed(inc)(y, dask_key_name="z")
    w = delayed(add)(x, z, dask_key_name="w")

    ww = w.persist()
    yield wait(ww)

    old = list(s.transition_log)

    www = w.persist()
    yield wait(www)
    yield gen.sleep(0.100)
    assert list(s.transition_log) == old


@gen_cluster(client=True)
def test_dont_recompute_if_persisted_4(c, s, a, b):
    x = delayed(inc)(1, dask_key_name="x")
    y = delayed(inc)(x, dask_key_name="y")
    z = delayed(inc)(x, dask_key_name="z")

    yy = y.persist()
    yield wait(yy)

    old = s.story("x")

    while s.tasks["x"].state == "memory":
        yield gen.sleep(0.01)

    yyy, zzz = dask.persist(y, z)
    yield wait([yyy, zzz])

    new = s.story("x")
    assert len(new) > len(old)


@gen_cluster(client=True)
def test_dont_forget_released_keys(c, s, a, b):
    x = c.submit(inc, 1, key="x")
    y = c.submit(inc, x, key="y")
    z = c.submit(dec, x, key="z")
    del x
    yield wait([y, z])
    del z

    while "z" in s.tasks:
        yield gen.sleep(0.01)

    assert "x" in s.tasks


@gen_cluster(client=True)
def test_dont_recompute_if_erred(c, s, a, b):
    x = delayed(inc)(1, dask_key_name="x")
    y = delayed(div)(x, 0, dask_key_name="y")

    yy = y.persist()
    yield wait(yy)

    old = list(s.transition_log)

    yyy = y.persist()
    yield wait(yyy)

    yield gen.sleep(0.100)
    assert list(s.transition_log) == old


@gen_cluster()
def test_closing_scheduler_closes_workers(s, a, b):
    yield s.close()

    start = time()
    while a.status != "closed" or b.status != "closed":
        yield gen.sleep(0.01)
        assert time() < start + 2


@gen_cluster(
    client=True, ncores=[("127.0.0.1", 1)], worker_kwargs={"resources": {"A": 1}}
)
def test_resources_reset_after_cancelled_task(c, s, w):
    future = c.submit(sleep, 0.2, resources={"A": 1})

    while not w.executing:
        yield gen.sleep(0.01)

    yield future.cancel()

    while w.executing:
        yield gen.sleep(0.01)

    assert not s.workers[w.address].used_resources["A"]
    assert w.available_resources == {"A": 1}

    yield c.submit(inc, 1, resources={"A": 1})


@gen_cluster(client=True)
def test_gh2187(c, s, a, b):
    def foo():
        return "foo"

    def bar(x):
        return x + "bar"

    def baz(x):
        return x + "baz"

    def qux(x):
        sleep(0.1)
        return x + "qux"

    w = c.submit(foo, key="w")
    x = c.submit(bar, w, key="x")
    y = c.submit(baz, x, key="y")
    yield y
    z = c.submit(qux, y, key="z")
    del y
    yield gen.sleep(0.1)
    f = c.submit(bar, x, key="y")
    yield f


@gen_cluster(client=True, config={"distributed.scheduler.idle-timeout": "200ms"})
def test_idle_timeout(c, s, a, b):
    future = c.submit(slowinc, 1)
    yield future

    assert s.status != "closed"

    start = time()
    while s.status != "closed":
        yield gen.sleep(0.01)
    assert time() < start + 3

    assert a.status == "closed"
    assert b.status == "closed"


@gen_cluster()
def test_workerstate_clean(s, a, b):
    ws = s.workers[a.address].clean()
    assert ws.address == a.address
    b = pickle.dumps(ws)
    assert len(b) < 1000


@gen_cluster(client=True)
def test_result_type(c, s, a, b):
    x = c.submit(lambda: 1)
    yield x

    assert "int" in s.tasks[x.key].type


@gen_cluster()
def test_close_workers(s, a, b):
    yield s.close(close_workers=True)
    assert a.status == "closed"
    assert b.status == "closed"
