import asyncio
import json
import logging
import operator
import re
import sys
from collections import defaultdict
from time import sleep

import cloudpickle
import dask
from dask import delayed
from tlz import merge, concat, valmap, first, frequencies

import pytest

from distributed import Nanny, Worker, Client, wait, fire_and_forget
from distributed.comm import Comm
from distributed.core import connect, rpc, ConnectionPool, Status
from distributed.scheduler import Scheduler
from distributed.client import wait
from distributed.metrics import time
from distributed.protocol.pickle import dumps
from distributed.worker import dumps_function, dumps_task
from distributed.utils import tmpfile, typename, TimeoutError
from distributed.utils_test import (  # noqa: F401
    captured_logger,
    cleanup,
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
    tls_only_security,
)
from distributed.utils_test import loop, nodebug  # noqa: F401
from dask.compatibility import apply

if sys.version_info < (3, 8):
    try:
        import pickle5 as pickle
    except ImportError:
        import pickle
else:
    import pickle


alice = "alice:1234"
bob = "bob:1234"

occupancy = defaultdict(lambda: 0)


@gen_cluster()
async def test_administration(s, a, b):
    assert isinstance(s.address, str)
    assert s.address in str(s)
    assert str(sum(s.nthreads.values())) in repr(s)
    assert str(len(s.nthreads)) in repr(s)


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)])
async def test_respect_data_in_memory(c, s, a):
    x = delayed(inc)(1)
    y = delayed(inc)(x)
    f = c.persist(y)
    await wait([f])

    assert s.tasks[y.key].who_has == {s.workers[a.address]}

    z = delayed(operator.add)(x, y)
    f2 = c.persist(z)
    while f2.key not in s.tasks or not s.tasks[f2.key]:
        assert s.tasks[y.key].who_has
        await asyncio.sleep(0.0001)


@gen_cluster(client=True)
async def test_recompute_released_results(c, s, a, b):
    x = delayed(inc)(1)
    y = delayed(inc)(x)

    yy = c.persist(y)
    await wait(yy)

    while s.tasks[x.key].who_has or x.key in a.data or x.key in b.data:  # let x go away
        await asyncio.sleep(0.01)

    z = delayed(dec)(x)
    zz = c.compute(z)
    result = await zz
    assert result == 1


@gen_cluster(client=True)
async def test_decide_worker_with_many_independent_leaves(c, s, a, b):
    xs = await asyncio.gather(
        c.scatter(list(range(0, 100, 2)), workers=a.address),
        c.scatter(list(range(1, 100, 2)), workers=b.address),
    )
    xs = list(concat(zip(*xs)))
    ys = [delayed(inc)(x) for x in xs]

    y2s = c.persist(ys)
    await wait(y2s)

    nhits = sum(y.key in a.data for y in y2s[::2]) + sum(
        y.key in b.data for y in y2s[1::2]
    )

    assert nhits > 80


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 3)
async def test_decide_worker_with_restrictions(client, s, a, b, c):
    x = client.submit(inc, 1, workers=[a.address, b.address])
    await x
    assert x.key in a.data or x.key in b.data


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 3)
async def test_move_data_over_break_restrictions(client, s, a, b, c):
    [x] = await client.scatter([1], workers=b.address)
    y = client.submit(inc, x, workers=[a.address, b.address])
    await wait(y)
    assert y.key in a.data or y.key in b.data


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 3)
async def test_balance_with_restrictions(client, s, a, b, c):
    [x], [y] = await asyncio.gather(
        client.scatter([[1, 2, 3]], workers=a.address),
        client.scatter([1], workers=c.address),
    )
    z = client.submit(inc, 1, workers=[a.address, c.address])
    await wait(z)

    assert s.tasks[z.key].who_has == {s.workers[c.address]}


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 3)
async def test_no_valid_workers(client, s, a, b, c):
    x = client.submit(inc, 1, workers="127.0.0.5:9999")
    while not s.tasks:
        await asyncio.sleep(0.01)

    assert s.tasks[x.key] in s.unrunnable

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(x, 0.05)


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 3)
async def test_no_valid_workers_loose_restrictions(client, s, a, b, c):
    x = client.submit(inc, 1, workers="127.0.0.5:9999", allow_other_workers=True)
    result = await x
    assert result == 2


@gen_cluster(client=True, nthreads=[])
async def test_no_workers(client, s):
    x = client.submit(inc, 1)
    while not s.tasks:
        await asyncio.sleep(0.01)

    assert s.tasks[x.key] in s.unrunnable

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(x, 0.05)


@gen_cluster(nthreads=[])
async def test_retire_workers_empty(s):
    await s.retire_workers(workers=[])


@gen_cluster()
async def test_remove_client(s, a, b):
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
async def test_server_listens_to_other_ops(s, a, b):
    with rpc(s.address) as r:
        ident = await r.identity()
        assert ident["type"] == "Scheduler"
        assert ident["id"].lower().startswith("scheduler")


@gen_cluster()
async def test_remove_worker_from_scheduler(s, a, b):
    dsk = {("x-%d" % i): (inc, i) for i in range(20)}
    s.update_graph(
        tasks=valmap(dumps_task, dsk),
        keys=list(dsk),
        dependencies={k: set() for k in dsk},
    )

    assert a.address in s.stream_comms
    await s.remove_worker(address=a.address)
    assert a.address not in s.nthreads
    assert len(s.workers[b.address].processing) == len(dsk)  # b owns everything
    s.validate_state()


@gen_cluster()
async def test_remove_worker_by_name_from_scheduler(s, a, b):
    assert a.address in s.stream_comms
    assert await s.remove_worker(address=a.name) == "OK"
    assert a.address not in s.nthreads
    assert await s.remove_worker(address=a.address) == "already-removed"
    s.validate_state()


@gen_cluster(config={"distributed.scheduler.events-cleanup-delay": "10 ms"})
async def test_clear_events_worker_removal(s, a, b):
    assert a.address in s.events
    assert a.address in s.nthreads
    assert b.address in s.events
    assert b.address in s.nthreads

    await s.remove_worker(address=a.address)
    # Shortly after removal, the events should still be there
    assert a.address in s.events
    assert a.address not in s.nthreads
    s.validate_state()

    start = time()
    while a.address in s.events:
        await asyncio.sleep(0.01)
        assert time() < start + 2
    assert b.address in s.events


@gen_cluster(
    config={"distributed.scheduler.events-cleanup-delay": "10 ms"}, client=True
)
async def test_clear_events_client_removal(c, s, a, b):
    assert c.id in s.events
    s.remove_client(c.id)

    assert c.id in s.events
    assert c.id not in s.clients
    assert c not in s.clients

    s.remove_client(c.id)
    # If it doesn't reconnect after a given time, the events log should be cleared
    start = time()
    while c.id in s.events:
        await asyncio.sleep(0.01)
        assert time() < start + 2


@gen_cluster()
async def test_add_worker(s, a, b):
    w = Worker(s.address, nthreads=3)
    w.data["x-5"] = 6
    w.data["y"] = 1

    dsk = {("x-%d" % i): (inc, i) for i in range(10)}
    s.update_graph(
        tasks=valmap(dumps_task, dsk),
        keys=list(dsk),
        client="client",
        dependencies={k: set() for k in dsk},
    )
    s.validate_state()
    await w
    s.validate_state()

    assert w.ip in s.host_info
    assert s.host_info[w.ip]["addresses"] == {a.address, b.address, w.address}
    await w.close()


@gen_cluster(scheduler_kwargs={"blocked_handlers": ["feed"]})
async def test_blocked_handlers_are_respected(s, a, b):
    def func(scheduler):
        return dumps(dict(scheduler.worker_info))

    comm = await connect(s.address)
    await comm.write({"op": "feed", "function": dumps(func), "interval": 0.01})

    response = await comm.read()

    assert "exception" in response
    assert isinstance(response["exception"], ValueError)
    assert "'feed' handler has been explicitly disallowed" in repr(
        response["exception"]
    )

    await comm.close()


def test_scheduler_init_pulls_blocked_handlers_from_config():
    with dask.config.set({"distributed.scheduler.blocked-handlers": ["test-handler"]}):
        s = Scheduler()
    assert s.blocked_handlers == ["test-handler"]


@gen_cluster()
async def test_feed(s, a, b):
    def func(scheduler):
        return dumps(dict(scheduler.worker_info))

    comm = await connect(s.address)
    await comm.write({"op": "feed", "function": dumps(func), "interval": 0.01})

    for i in range(5):
        response = await comm.read()
        expected = dict(s.worker_info)
        assert cloudpickle.loads(response) == expected

    await comm.close()


@gen_cluster()
async def test_feed_setup_teardown(s, a, b):
    def setup(scheduler):
        return 1

    def func(scheduler, state):
        assert state == 1
        return "OK"

    def teardown(scheduler, state):
        scheduler.flag = "done"

    comm = await connect(s.address)
    await comm.write(
        {
            "op": "feed",
            "function": dumps(func),
            "setup": dumps(setup),
            "teardown": dumps(teardown),
            "interval": 0.01,
        }
    )

    for i in range(5):
        response = await comm.read()
        assert response == "OK"

    await comm.close()
    start = time()
    while not hasattr(s, "flag"):
        await asyncio.sleep(0.01)
        assert time() - start < 5


@gen_cluster()
async def test_feed_large_bytestring(s, a, b):
    np = pytest.importorskip("numpy")

    x = np.ones(10000000)

    def func(scheduler):
        y = x
        return True

    comm = await connect(s.address)
    await comm.write({"op": "feed", "function": dumps(func), "interval": 0.05})

    for i in range(5):
        response = await comm.read()
        assert response is True

    await comm.close()


@gen_cluster(client=True)
async def test_delete_data(c, s, a, b):
    d = await c.scatter({"x": 1, "y": 2, "z": 3})

    assert {ts.key for ts in s.tasks.values() if ts.who_has} == {"x", "y", "z"}
    assert set(a.data) | set(b.data) == {"x", "y", "z"}
    assert merge(a.data, b.data) == {"x": 1, "y": 2, "z": 3}

    del d["x"]
    del d["y"]

    start = time()
    while set(a.data) | set(b.data) != {"z"}:
        await asyncio.sleep(0.01)
        assert time() < start + 5


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)])
async def test_delete(c, s, a):
    x = c.submit(inc, 1)
    await x
    assert x.key in a.data

    await c._cancel(x)

    start = time()
    while x.key in a.data:
        await asyncio.sleep(0.01)
        assert time() < start + 5


@gen_cluster()
async def test_filtered_communication(s, a, b):
    c = await connect(s.address)
    f = await connect(s.address)
    await c.write({"op": "register-client", "client": "c", "versions": {}})
    await f.write({"op": "register-client", "client": "f", "versions": {}})
    await c.read()
    await f.read()

    assert set(s.client_comms) == {"c", "f"}

    await c.write(
        {
            "op": "update-graph",
            "tasks": {"x": dumps_task((inc, 1)), "y": dumps_task((inc, "x"))},
            "dependencies": {"x": [], "y": ["x"]},
            "client": "c",
            "keys": ["y"],
        }
    )

    await f.write(
        {
            "op": "update-graph",
            "tasks": {
                "x": dumps_task((inc, 1)),
                "z": dumps_task((operator.add, "x", 10)),
            },
            "dependencies": {"x": [], "z": ["x"]},
            "client": "f",
            "keys": ["z"],
        }
    )
    (msg,) = await c.read()
    assert msg["op"] == "key-in-memory"
    assert msg["key"] == "y"
    (msg,) = await f.read()
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
async def test_ready_remove_worker(s, a, b):
    s.update_graph(
        tasks={"x-%d" % i: dumps_task((inc, i)) for i in range(20)},
        keys=["x-%d" % i for i in range(20)],
        client="client",
        dependencies={"x-%d" % i: [] for i in range(20)},
    )

    assert all(len(w.processing) > w.nthreads for w in s.workers.values())

    await s.remove_worker(address=a.address)

    assert set(s.workers) == {b.address}
    assert all(len(w.processing) > w.nthreads for w in s.workers.values())


@gen_cluster(client=True, Worker=Nanny)
async def test_restart(c, s, a, b):
    futures = c.map(inc, range(20))
    await wait(futures)

    await s.restart()

    assert len(s.workers) == 2

    for ws in s.workers.values():
        assert not ws.occupancy
        assert not ws.processing

    assert not s.tasks
    assert not s.dependencies


@gen_cluster()
async def test_broadcast(s, a, b):
    result = await s.broadcast(msg={"op": "ping"})
    assert result == {a.address: b"pong", b.address: b"pong"}

    result = await s.broadcast(msg={"op": "ping"}, workers=[a.address])
    assert result == {a.address: b"pong"}

    result = await s.broadcast(msg={"op": "ping"}, hosts=[a.ip])
    assert result == {a.address: b"pong", b.address: b"pong"}


@gen_cluster(security=tls_only_security(),)
async def test_broadcast_tls(s, a, b):
    result = await s.broadcast(msg={"op": "ping"})
    assert result == {a.address: b"pong", b.address: b"pong"}

    result = await s.broadcast(msg={"op": "ping"}, workers=[a.address])
    assert result == {a.address: b"pong"}

    result = await s.broadcast(msg={"op": "ping"}, hosts=[a.ip])
    assert result == {a.address: b"pong", b.address: b"pong"}


@gen_cluster(Worker=Nanny)
async def test_broadcast_nanny(s, a, b):
    result1 = await s.broadcast(msg={"op": "identity"}, nanny=True)
    assert all(d["type"] == "Nanny" for d in result1.values())

    result2 = await s.broadcast(
        msg={"op": "identity"}, workers=[a.worker_address], nanny=True
    )
    assert len(result2) == 1
    assert first(result2.values())["id"] == a.id

    result3 = await s.broadcast(msg={"op": "identity"}, hosts=[a.ip], nanny=True)
    assert result1 == result3


@gen_test()
async def test_worker_name():
    s = await Scheduler(validate=True, port=0)
    w = await Worker(s.address, name="alice")
    assert s.workers[w.address].name == "alice"
    assert s.aliases["alice"] == w.address

    with pytest.raises(ValueError):
        w2 = await Worker(s.address, name="alice")
        await w2.close()

    await w.close()
    await s.close()


@gen_test()
async def test_coerce_address():
    with dask.config.set({"distributed.comm.timeouts.connect": "100ms"}):
        s = await Scheduler(validate=True, port=0)
        print("scheduler:", s.address, s.listen_address)
        a = Worker(s.address, name="alice")
        b = Worker(s.address, name=123)
        c = Worker("127.0.0.1", s.port, name="charlie")
        await asyncio.gather(a, b, c)

        assert s.coerce_address("127.0.0.1:8000") == "tcp://127.0.0.1:8000"
        assert s.coerce_address("[::1]:8000") == "tcp://[::1]:8000"
        assert s.coerce_address("tcp://127.0.0.1:8000") == "tcp://127.0.0.1:8000"
        assert s.coerce_address("tcp://[::1]:8000") == "tcp://[::1]:8000"
        assert s.coerce_address("localhost:8000") in (
            "tcp://127.0.0.1:8000",
            "tcp://[::1]:8000",
        )
        assert s.coerce_address("localhost:8000") in (
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

        await s.close()
        await asyncio.gather(a.close(), b.close(), c.close())


@pytest.mark.asyncio
async def test_config_stealing(cleanup):
    # Regression test for https://github.com/dask/distributed/issues/3409

    with dask.config.set({"distributed.scheduler.work-stealing": True}):
        async with Scheduler(port=0) as s:
            assert "stealing" in s.extensions

    with dask.config.set({"distributed.scheduler.work-stealing": False}):
        async with Scheduler(port=0) as s:
            assert "stealing" not in s.extensions


@pytest.mark.skipif(
    sys.platform.startswith("win"), reason="file descriptors not really a thing"
)
@gen_cluster(nthreads=[])
async def test_file_descriptors_dont_leak(s):
    psutil = pytest.importorskip("psutil")
    proc = psutil.Process()
    before = proc.num_fds()

    w = await Worker(s.address)
    await w.close()

    during = proc.num_fds()

    start = time()
    while proc.num_fds() > before:
        await asyncio.sleep(0.01)
        assert time() < start + 5


@gen_cluster()
async def test_update_graph_culls(s, a, b):
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


def test_io_loop(loop):
    s = Scheduler(loop=loop, validate=True)
    assert s.io_loop is loop


@gen_cluster(client=True)
async def test_story(c, s, a, b):
    x = delayed(inc)(1)
    y = delayed(inc)(x)
    f = c.persist(y)
    await wait([f])

    assert s.transition_log

    story = s.story(x.key)
    assert all(line in s.transition_log for line in story)
    assert len(story) < len(s.transition_log)
    assert all(x.key == line[0] or x.key in line[-2] for line in story)

    assert len(s.story(x.key, y.key)) > len(story)


@gen_cluster(nthreads=[], client=True)
async def test_scatter_no_workers(c, s):
    with pytest.raises(TimeoutError):
        await s.scatter(data={"x": 1}, client="alice", timeout=0.1)

    start = time()
    with pytest.raises(TimeoutError):
        await c.scatter(123, timeout=0.1)
    assert time() < start + 1.5

    w = Worker(s.address, nthreads=3)
    await asyncio.gather(c.scatter(data={"y": 2}, timeout=5), w)

    assert w.data["y"] == 2
    await w.close()


@gen_cluster(nthreads=[])
async def test_scheduler_sees_memory_limits(s):
    w = await Worker(s.address, nthreads=3, memory_limit=12345)

    assert s.workers[w.address].memory_limit == 12345
    await w.close()


@gen_cluster(client=True, timeout=1000)
async def test_retire_workers(c, s, a, b):
    [x] = await c.scatter([1], workers=a.address)
    [y] = await c.scatter([list(range(1000))], workers=b.address)

    assert s.workers_to_close() == [a.address]

    workers = await s.retire_workers()
    assert list(workers) == [a.address]
    assert workers[a.address]["nthreads"] == a.nthreads
    assert list(s.nthreads) == [b.address]

    assert s.workers_to_close() == []

    assert s.workers[b.address].has_what == {s.tasks[x.key], s.tasks[y.key]}

    workers = await s.retire_workers()
    assert not workers


@gen_cluster(client=True)
async def test_retire_workers_n(c, s, a, b):
    await s.retire_workers(n=1, close_workers=True)
    assert len(s.workers) == 1

    await s.retire_workers(n=0, close_workers=True)
    assert len(s.workers) == 1

    await s.retire_workers(n=1, close_workers=True)
    assert len(s.workers) == 0

    await s.retire_workers(n=0, close_workers=True)
    assert len(s.workers) == 0

    while not (
        a.status in (Status.closed, Status.closing, Status.closing_gracefully)
        and b.status in (Status.closed, Status.closing, Status.closing_gracefully)
    ):
        await asyncio.sleep(0.01)


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 4)
async def test_workers_to_close(cl, s, *workers):
    with dask.config.set(
        {"distributed.scheduler.default-task-durations": {"a": 4, "b": 4, "c": 1}}
    ):
        futures = cl.map(slowinc, [1, 1, 1], key=["a-4", "b-4", "c-1"])
        while sum(len(w.processing) for w in s.workers.values()) < 3:
            await asyncio.sleep(0.001)

        wtc = s.workers_to_close()
        assert all(not s.workers[w].processing for w in wtc)
        assert len(wtc) == 1


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 4)
async def test_workers_to_close_grouped(c, s, *workers):
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
        await asyncio.sleep(0.001)

    assert set(s.workers_to_close(key=key)) == {workers[2].address, workers[3].address}

    del future

    while len(s.rprocessing) > 0:
        await asyncio.sleep(0.001)

    # Assert that *total* byte count in group determines group priority
    av = await c.scatter("a" * 100, workers=workers[0].address)
    bv = await c.scatter("b" * 75, workers=workers[2].address)
    bv2 = await c.scatter("b" * 75, workers=workers[3].address)

    assert set(s.workers_to_close(key=key)) == {workers[0].address, workers[1].address}


@gen_cluster(client=True)
async def test_retire_workers_no_suspicious_tasks(c, s, a, b):
    future = c.submit(
        slowinc, 100, delay=0.5, workers=a.address, allow_other_workers=True
    )
    await asyncio.sleep(0.2)
    await s.retire_workers(workers=[a.address])

    assert all(ts.suspicious == 0 for ts in s.tasks.values())
    assert all(tp.suspicious == 0 for tp in s.task_prefixes.values())


@pytest.mark.slow
@pytest.mark.skipif(
    sys.platform.startswith("win"), reason="file descriptors not really a thing"
)
@gen_cluster(client=True, nthreads=[], timeout=240)
async def test_file_descriptors(c, s):
    await asyncio.sleep(0.1)
    psutil = pytest.importorskip("psutil")
    da = pytest.importorskip("dask.array")
    proc = psutil.Process()
    num_fds_1 = proc.num_fds()

    N = 20
    nannies = await asyncio.gather(*[Nanny(s.address, loop=s.loop) for _ in range(N)])

    while len(s.nthreads) < N:
        await asyncio.sleep(0.1)

    num_fds_2 = proc.num_fds()

    await asyncio.sleep(0.2)

    num_fds_3 = proc.num_fds()
    assert num_fds_3 <= num_fds_2 + N  # add some heartbeats

    x = da.random.random(size=(1000, 1000), chunks=(25, 25))
    x = c.persist(x)
    await wait(x)

    num_fds_4 = proc.num_fds()
    assert num_fds_4 <= num_fds_2 + 2 * N

    y = c.persist(x + x.T)
    await wait(y)

    num_fds_5 = proc.num_fds()
    assert num_fds_5 < num_fds_4 + N

    await asyncio.sleep(1)

    num_fds_6 = proc.num_fds()
    assert num_fds_6 < num_fds_5 + N

    await asyncio.gather(*[n.close() for n in nannies])
    await c.close()

    assert not s.rpc.open
    for addr, occ in c.rpc.occupied.items():
        for comm in occ:
            assert comm.closed() or comm.peer_address != s.address, comm
    assert not s.stream_comms

    start = time()
    while proc.num_fds() > num_fds_1 + N:
        await asyncio.sleep(0.01)
        assert time() < start + 3


@pytest.mark.slow
@nodebug
@gen_cluster(client=True)
async def test_learn_occupancy(c, s, a, b):
    futures = c.map(slowinc, range(1000), delay=0.2)
    while sum(len(ts.who_has) for ts in s.tasks.values()) < 10:
        await asyncio.sleep(0.01)

    assert 100 < s.total_occupancy < 1000
    for w in [a, b]:
        assert 50 < s.workers[w.address].occupancy < 700


@pytest.mark.slow
@nodebug
@gen_cluster(client=True)
async def test_learn_occupancy_2(c, s, a, b):
    future = c.map(slowinc, range(1000), delay=0.2)
    while not any(ts.who_has for ts in s.tasks.values()):
        await asyncio.sleep(0.01)

    assert 100 < s.total_occupancy < 1000


@gen_cluster(client=True)
async def test_occupancy_cleardown(c, s, a, b):
    s.validate = False

    # Inject excess values in s.occupancy
    s.workers[a.address].occupancy = 2
    s.total_occupancy += 2
    futures = c.map(slowinc, range(100), delay=0.01)
    await wait(futures)

    # Verify that occupancy values have been zeroed out
    assert abs(s.total_occupancy) < 0.01
    assert all(ws.occupancy == 0 for ws in s.workers.values())


@nodebug
@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 30)
async def test_balance_many_workers(c, s, *workers):
    futures = c.map(slowinc, range(20), delay=0.2)
    await wait(futures)
    assert {len(w.has_what) for w in s.workers.values()} == {0, 1}


@nodebug
@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 30)
async def test_balance_many_workers_2(c, s, *workers):
    s.extensions["stealing"]._pc.callback_time = 100000000
    futures = c.map(slowinc, range(90), delay=0.2)
    await wait(futures)
    assert {len(w.has_what) for w in s.workers.values()} == {3}


@gen_cluster(client=True)
async def test_learn_occupancy_multiple_workers(c, s, a, b):
    x = c.submit(slowinc, 1, delay=0.2, workers=a.address)
    await asyncio.sleep(0.05)
    futures = c.map(slowinc, range(100), delay=0.2)

    await wait(x)

    assert not any(v == 0.5 for w in s.workers.values() for v in w.processing.values())
    s.validate_state()


@gen_cluster(client=True)
async def test_include_communication_in_occupancy(c, s, a, b):
    await c.submit(slowadd, 1, 2, delay=0)
    x = c.submit(operator.mul, b"0", int(s.bandwidth), workers=a.address)
    y = c.submit(operator.mul, b"1", int(s.bandwidth * 1.5), workers=b.address)

    z = c.submit(slowadd, x, y, delay=1)
    while z.key not in s.tasks or not s.tasks[z.key].processing_on:
        await asyncio.sleep(0.01)

    ts = s.tasks[z.key]
    assert ts.processing_on == s.workers[b.address]
    assert s.workers[b.address].processing[ts] > 1
    await wait(z)
    del z


@gen_cluster(client=True)
async def test_worker_arrives_with_processing_data(c, s, a, b):
    x = delayed(slowinc)(1, delay=0.4)
    y = delayed(slowinc)(x, delay=0.4)
    z = delayed(slowinc)(y, delay=0.4)

    yy, zz = c.persist([y, z])

    while not any(w.processing for w in s.workers.values()):
        await asyncio.sleep(0.01)

    w = Worker(s.address, nthreads=1)
    w.put_key_in_memory(y.key, 3)

    await w

    start = time()

    while len(s.workers) < 3:
        await asyncio.sleep(0.01)

    assert s.get_task_status(keys={x.key, y.key, z.key}) == {
        x.key: "released",
        y.key: "memory",
        z.key: "processing",
    }

    await w.close()


@pytest.mark.slow
@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)])
async def test_worker_breaks_and_returns(c, s, a):
    future = c.submit(slowinc, 1, delay=0.1)
    for i in range(20):
        future = c.submit(slowinc, future, delay=0.1)

    await wait(future)

    await a.batched_stream.comm.close()

    await asyncio.sleep(0.1)
    start = time()
    await wait(future, timeout=10)
    end = time()

    assert end - start < 2

    states = frequencies(ts.state for ts in s.tasks.values())
    assert states == {"memory": 1, "released": 20}


@gen_cluster(client=True, nthreads=[])
async def test_no_workers_to_memory(c, s):
    x = delayed(slowinc)(1, delay=0.4)
    y = delayed(slowinc)(x, delay=0.4)
    z = delayed(slowinc)(y, delay=0.4)

    yy, zz = c.persist([y, z])

    while not s.tasks:
        await asyncio.sleep(0.01)

    w = Worker(s.address, nthreads=1)
    w.put_key_in_memory(y.key, 3)

    await w

    start = time()

    while not s.workers:
        await asyncio.sleep(0.01)

    assert s.get_task_status(keys={x.key, y.key, z.key}) == {
        x.key: "released",
        y.key: "memory",
        z.key: "processing",
    }

    await w.close()


@gen_cluster(client=True)
async def test_no_worker_to_memory_restrictions(c, s, a, b):
    x = delayed(slowinc)(1, delay=0.4)
    y = delayed(slowinc)(x, delay=0.4)
    z = delayed(slowinc)(y, delay=0.4)

    yy, zz = c.persist([y, z], workers={(x, y, z): "alice"})

    while not s.tasks:
        await asyncio.sleep(0.01)

    w = Worker(s.address, nthreads=1, name="alice")
    w.put_key_in_memory(y.key, 3)

    await w

    while len(s.workers) < 3:
        await asyncio.sleep(0.01)
    await asyncio.sleep(0.3)

    assert s.get_task_status(keys={x.key, y.key, z.key}) == {
        x.key: "released",
        y.key: "memory",
        z.key: "processing",
    }

    await w.close()


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
async def test_run_on_scheduler(c, s, a, b):
    def f(dask_scheduler=None):
        return dask_scheduler.address

    response = await c._run_on_scheduler(f)
    assert response == s.address


@gen_cluster(client=True)
async def test_close_worker(c, s, a, b):
    assert len(s.workers) == 2

    await s.close_worker(worker=a.address)

    assert len(s.workers) == 1
    assert a.address not in s.workers

    await asyncio.sleep(0.5)

    assert len(s.workers) == 1


@pytest.mark.slow
@gen_cluster(client=True, Worker=Nanny, timeout=20)
async def test_close_nanny(c, s, a, b):
    assert len(s.workers) == 2

    assert a.process.is_alive()
    a_worker_address = a.worker_address
    start = time()
    await s.close_worker(worker=a_worker_address)

    assert len(s.workers) == 1
    assert a_worker_address not in s.workers

    start = time()
    while a.is_alive():
        await asyncio.sleep(0.1)
        assert time() < start + 5

    assert not a.is_alive()
    assert a.pid is None

    for i in range(10):
        await asyncio.sleep(0.1)
        assert len(s.workers) == 1
        assert not a.is_alive()
        assert a.pid is None

    while a.status != "closed":
        await asyncio.sleep(0.05)
        assert time() < start + 10


@gen_cluster(client=True, timeout=20)
async def test_retire_workers_close(c, s, a, b):
    await s.retire_workers(close_workers=True)
    assert not s.workers
    while a.status != "closed" and b.status != "closed":
        await asyncio.sleep(0.01)


@gen_cluster(client=True, timeout=20, Worker=Nanny)
async def test_retire_nannies_close(c, s, a, b):
    nannies = [a, b]
    await s.retire_workers(close_workers=True, remove=True)
    assert not s.workers

    start = time()

    while any(n.status != "closed" for n in nannies):
        await asyncio.sleep(0.05)
        assert time() < start + 10

    assert not any(n.is_alive() for n in nannies)
    assert not s.workers


@gen_cluster(client=True, nthreads=[("127.0.0.1", 2)])
async def test_fifo_submission(c, s, w):
    futures = []
    for i in range(20):
        future = c.submit(slowinc, i, delay=0.1, key="inc-%02d" % i, fifo_timeout=0.01)
        futures.append(future)
        await asyncio.sleep(0.02)
    await wait(futures[-1])
    assert futures[10].status == "finished"


@gen_test()
async def test_scheduler_file():
    with tmpfile() as fn:
        s = await Scheduler(scheduler_file=fn, port=0)
        with open(fn) as f:
            data = json.load(f)
        assert data["address"] == s.address

        c = await Client(scheduler_file=fn, loop=s.loop, asynchronous=True)
        await c.close()
        await s.close()


@pytest.mark.xfail(reason="")
@gen_cluster(client=True, nthreads=[])
async def test_non_existent_worker(c, s):
    with dask.config.set({"distributed.comm.timeouts.connect": "100ms"}):
        await s.add_worker(
            address="127.0.0.1:5738", nthreads=2, nbytes={}, host_info={}
        )
        futures = c.map(inc, range(10))
        await asyncio.sleep(0.300)
        assert not s.workers
        assert all(ts.state == "no-worker" for ts in s.tasks.values())


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 3)
async def test_correct_bad_time_estimate(c, s, *workers):
    future = c.submit(slowinc, 1, delay=0)
    await wait(future)

    futures = [c.submit(slowinc, future, delay=0.1, pure=False) for i in range(20)]

    await asyncio.sleep(0.5)

    await wait(futures)

    assert all(w.data for w in workers), [sorted(w.data) for w in workers]


@gen_test()
async def test_service_hosts():
    port = 0
    for url, expected in [
        ("tcp://0.0.0.0", ("::", "0.0.0.0")),
        ("tcp://127.0.0.1", ("::", "0.0.0.0")),
        ("tcp://127.0.0.1:38275", ("::", "0.0.0.0")),
    ]:
        async with Scheduler(host=url) as s:
            sock = first(s.http_server._sockets.values())
            if isinstance(expected, tuple):
                assert sock.getsockname()[0] in expected
            else:
                assert sock.getsockname()[0] == expected

    port = ("127.0.0.1", 0)
    for url in ["tcp://0.0.0.0", "tcp://127.0.0.1", "tcp://127.0.0.1:38275"]:
        async with Scheduler(dashboard_address="127.0.0.1:0", host=url) as s:
            sock = first(s.http_server._sockets.values())
            assert sock.getsockname()[0] == "127.0.0.1"


@gen_cluster(client=True, worker_kwargs={"profile_cycle_interval": "100ms"})
async def test_profile_metadata(c, s, a, b):
    start = time() - 1
    futures = c.map(slowinc, range(10), delay=0.05, workers=a.address)
    await wait(futures)
    await asyncio.sleep(0.200)

    meta = await s.get_profile_metadata(profile_cycle_interval=0.100)
    now = time() + 1
    assert meta
    assert all(start < t < now for t, count in meta["counts"])
    assert all(0 <= count < 30 for t, count in meta["counts"][:4])
    assert not meta["counts"][-1][1]


@gen_cluster(client=True, worker_kwargs={"profile_cycle_interval": "100ms"})
async def test_profile_metadata_timeout(c, s, a, b):
    start = time() - 1

    def raise_timeout(*args, **kwargs):
        raise TimeoutError

    b.handlers["profile_metadata"] = raise_timeout

    futures = c.map(slowinc, range(10), delay=0.05, workers=a.address)
    await wait(futures)
    await asyncio.sleep(0.200)

    meta = await s.get_profile_metadata(profile_cycle_interval=0.100)
    now = time() + 1
    assert meta
    assert all(start < t < now for t, count in meta["counts"])
    assert all(0 <= count < 30 for t, count in meta["counts"][:4])
    assert not meta["counts"][-1][1]


@gen_cluster(client=True, worker_kwargs={"profile_cycle_interval": "100ms"})
async def test_profile_metadata_keys(c, s, a, b):
    x = c.map(slowinc, range(10), delay=0.05)
    y = c.map(slowdec, range(10), delay=0.05)
    await wait(x + y)

    meta = await s.get_profile_metadata(profile_cycle_interval=0.100)
    assert set(meta["keys"]) == {"slowinc", "slowdec"}
    assert (
        len(meta["counts"]) - 3 <= len(meta["keys"]["slowinc"]) <= len(meta["counts"])
    )


@gen_cluster(
    client=True,
    config={
        "distributed.worker.profile.interval": "1ms",
        "distributed.worker.profile.cycle": "100ms",
    },
)
async def test_statistical_profiling(c, s, a, b):
    futures = c.map(slowinc, range(10), delay=0.1)

    await wait(futures)

    profile = await s.get_profile()
    assert profile["count"]


@gen_cluster(
    client=True,
    config={
        "distributed.worker.profile.interval": "1ms",
        "distributed.worker.profile.cycle": "100ms",
    },
)
async def test_statistical_profiling_failure(c, s, a, b):
    futures = c.map(slowinc, range(10), delay=0.1)

    def raise_timeout(*args, **kwargs):
        raise TimeoutError

    b.handlers["profile"] = raise_timeout
    await wait(futures)

    profile = await s.get_profile()
    assert profile["count"]


@gen_cluster(client=True)
async def test_cancel_fire_and_forget(c, s, a, b):
    x = delayed(slowinc)(1, delay=0.05)
    y = delayed(slowinc)(x, delay=0.05)
    z = delayed(slowinc)(y, delay=0.05)
    w = delayed(slowinc)(z, delay=0.05)
    future = c.compute(w)
    fire_and_forget(future)

    await asyncio.sleep(0.05)
    await future.cancel(force=True)
    assert future.status == "cancelled"
    assert not s.tasks


@gen_cluster(
    client=True, Worker=Nanny, clean_kwargs={"processes": False, "threads": False}
)
async def test_log_tasks_during_restart(c, s, a, b):
    future = c.submit(sys.exit, 0)
    await wait(future)
    assert "exit" in str(s.events)


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 2)
async def test_reschedule(c, s, a, b):
    await c.submit(slowinc, -1, delay=0.1)  # learn cost
    x = c.map(slowinc, range(4), delay=0.1)

    # add much more work onto worker a
    futures = c.map(slowinc, range(10, 20), delay=0.1, workers=a.address)

    while len(s.tasks) < len(x) + len(futures):
        await asyncio.sleep(0.001)

    for future in x:
        s.reschedule(key=future.key)

    # Worker b gets more of the original tasks
    await wait(x)
    assert sum(future.key in b.data for future in x) >= 3
    assert sum(future.key in a.data for future in x) <= 1


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 2)
async def test_reschedule_warns(c, s, a, b):
    with captured_logger(logging.getLogger("distributed.scheduler")) as sched:
        s.reschedule(key="__this-key-does-not-exist__")

    assert "not found on the scheduler" in sched.getvalue()
    assert "Aborting reschedule" in sched.getvalue()


@gen_cluster(client=True)
async def test_get_task_status(c, s, a, b):
    future = c.submit(inc, 1)
    await wait(future)

    result = await a.scheduler.get_task_status(keys=[future.key])
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
async def test_retries(c, s, a, b):
    args = [ZeroDivisionError("one"), ZeroDivisionError("two"), 42]

    future = c.submit(varying(args), retries=3)
    result = await future
    assert result == 42
    assert s.tasks[future.key].retries == 1
    assert future.key not in s.exceptions

    future = c.submit(varying(args), retries=2, pure=False)
    result = await future
    assert result == 42
    assert s.tasks[future.key].retries == 0
    assert future.key not in s.exceptions

    future = c.submit(varying(args), retries=1, pure=False)
    with pytest.raises(ZeroDivisionError) as exc_info:
        await future
    exc_info.match("two")

    future = c.submit(varying(args), retries=0, pure=False)
    with pytest.raises(ZeroDivisionError) as exc_info:
        await future
    exc_info.match("one")


@pytest.mark.xfail(reason="second worker also errant for some reason")
@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 3, timeout=5)
async def test_mising_data_errant_worker(c, s, w1, w2, w3):
    with dask.config.set({"distributed.comm.timeouts.connect": "1s"}):
        np = pytest.importorskip("numpy")

        x = c.submit(np.random.random, 10000000, workers=w1.address)
        await wait(x)
        await c.replicate(x, workers=[w1.address, w2.address])

        y = c.submit(len, x, workers=w3.address)
        while not w3.tasks:
            await asyncio.sleep(0.001)
        await w1.close()
        await wait(y)


@gen_cluster(client=True)
async def test_dont_recompute_if_persisted(c, s, a, b):
    x = delayed(inc)(1, dask_key_name="x")
    y = delayed(inc)(x, dask_key_name="y")

    yy = y.persist()
    await wait(yy)

    old = list(s.transition_log)

    yyy = y.persist()
    await wait(yyy)

    await asyncio.sleep(0.100)
    assert list(s.transition_log) == old


@gen_cluster(client=True)
async def test_dont_recompute_if_persisted_2(c, s, a, b):
    x = delayed(inc)(1, dask_key_name="x")
    y = delayed(inc)(x, dask_key_name="y")
    z = delayed(inc)(y, dask_key_name="z")

    yy = y.persist()
    await wait(yy)

    old = s.story("x", "y")

    zz = z.persist()
    await wait(zz)

    await asyncio.sleep(0.100)
    assert s.story("x", "y") == old


@gen_cluster(client=True)
async def test_dont_recompute_if_persisted_3(c, s, a, b):
    x = delayed(inc)(1, dask_key_name="x")
    y = delayed(inc)(2, dask_key_name="y")
    z = delayed(inc)(y, dask_key_name="z")
    w = delayed(operator.add)(x, z, dask_key_name="w")

    ww = w.persist()
    await wait(ww)

    old = list(s.transition_log)

    www = w.persist()
    await wait(www)
    await asyncio.sleep(0.100)
    assert list(s.transition_log) == old


@gen_cluster(client=True)
async def test_dont_recompute_if_persisted_4(c, s, a, b):
    x = delayed(inc)(1, dask_key_name="x")
    y = delayed(inc)(x, dask_key_name="y")
    z = delayed(inc)(x, dask_key_name="z")

    yy = y.persist()
    await wait(yy)

    old = s.story("x")

    while s.tasks["x"].state == "memory":
        await asyncio.sleep(0.01)

    yyy, zzz = dask.persist(y, z)
    await wait([yyy, zzz])

    new = s.story("x")
    assert len(new) > len(old)


@gen_cluster(client=True)
async def test_dont_forget_released_keys(c, s, a, b):
    x = c.submit(inc, 1, key="x")
    y = c.submit(inc, x, key="y")
    z = c.submit(dec, x, key="z")
    del x
    await wait([y, z])
    del z

    while "z" in s.tasks:
        await asyncio.sleep(0.01)

    assert "x" in s.tasks


@gen_cluster(client=True)
async def test_dont_recompute_if_erred(c, s, a, b):
    x = delayed(inc)(1, dask_key_name="x")
    y = delayed(div)(x, 0, dask_key_name="y")

    yy = y.persist()
    await wait(yy)

    old = list(s.transition_log)

    yyy = y.persist()
    await wait(yyy)

    await asyncio.sleep(0.100)
    assert list(s.transition_log) == old


@gen_cluster()
async def test_closing_scheduler_closes_workers(s, a, b):
    await s.close()

    start = time()
    while a.status != "closed" or b.status != "closed":
        await asyncio.sleep(0.01)
        assert time() < start + 2


@gen_cluster(
    client=True, nthreads=[("127.0.0.1", 1)], worker_kwargs={"resources": {"A": 1}}
)
async def test_resources_reset_after_cancelled_task(c, s, w):
    future = c.submit(sleep, 0.2, resources={"A": 1})

    while not w.executing:
        await asyncio.sleep(0.01)

    await future.cancel()

    while w.executing:
        await asyncio.sleep(0.01)

    assert not s.workers[w.address].used_resources["A"]
    assert w.available_resources == {"A": 1}

    await c.submit(inc, 1, resources={"A": 1})


@gen_cluster(client=True)
async def test_gh2187(c, s, a, b):
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
    await y
    z = c.submit(qux, y, key="z")
    del y
    await asyncio.sleep(0.1)
    f = c.submit(bar, x, key="y")
    await f


@gen_cluster(client=True)
async def test_collect_versions(c, s, a, b):
    cs = s.clients[c.id]
    (w1, w2) = s.workers.values()
    assert cs.versions
    assert w1.versions
    assert w2.versions
    assert "dask" in str(cs.versions)
    assert cs.versions == w1.versions == w2.versions


@gen_cluster(client=True, config={"distributed.scheduler.idle-timeout": "500ms"})
async def test_idle_timeout(c, s, a, b):
    beginning = time()
    assert s.idle_since <= beginning
    future = c.submit(slowinc, 1)
    await future
    assert s.idle_since is None or s.idle_since > beginning

    assert s.status != "closed"

    with captured_logger("distributed.scheduler") as logs:
        start = time()
        while s.status != "closed":
            await asyncio.sleep(0.01)
            assert time() < start + 3

        start = time()
        while not (a.status == "closed" and b.status == "closed"):
            await asyncio.sleep(0.01)
            assert time() < start + 1

    assert "idle" in logs.getvalue()
    assert "500" in logs.getvalue()
    assert "ms" in logs.getvalue()
    assert s.idle_since > beginning


@gen_cluster(client=True, config={"distributed.scheduler.bandwidth": "100 GB"})
async def test_bandwidth(c, s, a, b):
    start = s.bandwidth
    x = c.submit(operator.mul, b"0", 1000000, workers=a.address)
    y = c.submit(lambda x: x, x, workers=b.address)
    await y
    await b.heartbeat()
    assert s.bandwidth < start  # we've learned that we're slower
    assert b.latency
    assert typename(bytes) in s.bandwidth_types
    assert (b.address, a.address) in s.bandwidth_workers

    await a.close()
    assert not s.bandwidth_workers


@gen_cluster(client=True, Worker=Nanny)
async def test_bandwidth_clear(c, s, a, b):
    np = pytest.importorskip("numpy")
    x = c.submit(np.arange, 1000000, workers=[a.worker_address], pure=False)
    y = c.submit(np.arange, 1000000, workers=[b.worker_address], pure=False)
    z = c.submit(operator.add, x, y)  # force communication
    await z

    async def f(dask_worker):
        await dask_worker.heartbeat()

    await c.run(f)

    assert s.bandwidth_workers

    await s.restart()
    assert not s.bandwidth_workers


@gen_cluster()
async def test_workerstate_clean(s, a, b):
    ws = s.workers[a.address].clean()
    assert ws.address == a.address
    b = pickle.dumps(ws)
    assert len(b) < 1000


@gen_cluster(client=True)
async def test_result_type(c, s, a, b):
    x = c.submit(lambda: 1)
    await x

    assert "int" in s.tasks[x.key].type


@gen_cluster()
async def test_close_workers(s, a, b):
    await s.close(close_workers=True)
    assert a.status == "closed"
    assert b.status == "closed"


@pytest.mark.skipif(
    not sys.platform.startswith("linux"), reason="Need 127.0.0.2 to mean localhost"
)
@gen_test()
async def test_host_address():
    s = await Scheduler(host="127.0.0.2", port=0)
    assert "127.0.0.2" in s.address
    await s.close()


@gen_test()
async def test_dashboard_address():
    pytest.importorskip("bokeh")
    s = await Scheduler(dashboard_address="127.0.0.1:8901", port=0)
    assert s.services["dashboard"].port == 8901
    await s.close()

    s = await Scheduler(dashboard_address="127.0.0.1", port=0)
    assert s.services["dashboard"].port
    await s.close()


@gen_cluster(client=True)
async def test_adaptive_target(c, s, a, b):
    with dask.config.set(
        {"distributed.scheduler.default-task-durations": {"slowinc": 10}}
    ):
        assert s.adaptive_target() == 0
        x = c.submit(inc, 1)
        await x
        assert s.adaptive_target() == 1

        # Long task
        x = c.submit(slowinc, 1, delay=0.5)
        while x.key not in s.tasks:
            await asyncio.sleep(0.01)
        assert s.adaptive_target(target_duration=".1s") == 1  # still one

        L = c.map(slowinc, range(100), delay=0.5)
        while len(s.tasks) < 100:
            await asyncio.sleep(0.01)
        assert 10 < s.adaptive_target(target_duration=".1s") <= 100
        del x, L
        while s.tasks:
            await asyncio.sleep(0.01)
        assert s.adaptive_target(target_duration=".1s") == 0


@pytest.mark.asyncio
async def test_async_context_manager(cleanup):
    async with Scheduler(port=0) as s:
        assert s.status == "running"
        async with Worker(s.address) as w:
            assert w.status == "running"
            assert s.workers
        assert not s.workers


@pytest.mark.asyncio
async def test_allowed_failures_config(cleanup):
    async with Scheduler(port=0, allowed_failures=10) as s:
        assert s.allowed_failures == 10

    with dask.config.set({"distributed.scheduler.allowed_failures": 100}):
        async with Scheduler(port=0) as s:
            assert s.allowed_failures == 100

    with dask.config.set({"distributed.scheduler.allowed_failures": 0}):
        async with Scheduler(port=0) as s:
            assert s.allowed_failures == 0


@pytest.mark.asyncio
async def test_finished():
    async with Scheduler(port=0) as s:
        async with Worker(s.address) as w:
            pass

    await s.finished()
    await w.finished()


@pytest.mark.asyncio
async def test_retire_names_str(cleanup):
    async with Scheduler(port=0) as s:
        async with Worker(s.address, name="0") as a:
            async with Worker(s.address, name="1") as b:
                async with Client(s.address, asynchronous=True) as c:
                    futures = c.map(inc, range(10))
                    await wait(futures)
                    assert a.data and b.data
                    await s.retire_workers(names=[0])
                    assert all(f.done() for f in futures)
                    assert len(b.data) == 10


@gen_cluster(client=True)
async def test_get_task_duration(c, s, a, b):
    with dask.config.set(
        {"distributed.scheduler.default-task-durations": {"inc": 100}}
    ):
        future = c.submit(inc, 1)
        await future
        assert 10 < s.task_prefixes["inc"].duration_average < 100

        ts_pref1 = s.new_task("inc-abcdefab", None, "released")
        assert 10 < s.get_task_duration(ts_pref1) < 100

        # make sure get_task_duration adds TaskStates to unknown dict
        assert len(s.unknown_durations) == 0
        x = c.submit(slowinc, 1, delay=0.5)
        while len(s.tasks) < 3:
            await asyncio.sleep(0.01)

        ts = s.tasks[x.key]
        assert s.get_task_duration(ts) == 0.5  # default
        assert len(s.unknown_durations) == 1
        assert len(s.unknown_durations["slowinc"]) == 1


@pytest.mark.asyncio
@pytest.mark.skipif(
    sys.version_info < (3, 7), reason="asyncio.all_tasks not implemented"
)
async def test_no_danglng_asyncio_tasks(cleanup):
    start = asyncio.all_tasks()
    async with Scheduler(port=0) as s:
        async with Worker(s.address, name="0") as a:
            async with Client(s.address, asynchronous=True) as c:
                await asyncio.sleep(0.01)

    tasks = asyncio.all_tasks()
    assert tasks == start


@gen_cluster(client=True)
async def test_task_groups(c, s, a, b):
    da = pytest.importorskip("dask.array")
    x = da.arange(100, chunks=(20,))
    y = (x + 1).persist(optimize_graph=False)
    y = await y

    tg = s.task_groups[x.name]
    tp = s.task_prefixes["arange"]
    repr(tg)
    repr(tp)
    assert tg.states["memory"] == 0
    assert tg.states["released"] == 5
    assert tp.states["memory"] == 0
    assert tp.states["released"] == 5
    assert tg.prefix is tp
    assert tg in tp.groups
    assert tg.duration == tp.duration
    assert tg.nbytes_in_memory == tp.nbytes_in_memory
    assert tg.nbytes_total == tp.nbytes_total

    tg = s.task_groups[y.name]
    assert tg.states["memory"] == 5

    assert s.task_groups[y.name].dependencies == {s.task_groups[x.name]}

    await c.replicate(y)
    assert tg.nbytes_in_memory == y.nbytes
    assert "array" in str(tg.types)
    assert "array" in str(tp.types)

    del y

    while s.tasks:
        await asyncio.sleep(0.01)

    assert tg.nbytes_in_memory == 0
    assert tg.states["forgotten"] == 5
    # Ensure TaskGroup is removed once all tasks are in forgotten state
    assert tg.name not in s.task_groups
    assert sys.getrefcount(tg) == 2


@gen_cluster(client=True)
async def test_task_prefix(c, s, a, b):
    da = pytest.importorskip("dask.array")
    x = da.arange(100, chunks=(20,))
    y = (x + 1).sum().persist()
    y = await y

    assert s.task_prefixes["sum-aggregate"].states["memory"] == 1

    a = da.arange(101, chunks=(20,))
    b = (a + 1).sum().persist()
    b = await b

    assert s.task_prefixes["sum-aggregate"].states["memory"] == 2


@gen_cluster(
    client=True, Worker=Nanny, config={"distributed.scheduler.allowed-failures": 0}
)
async def test_failing_task_increments_suspicious(client, s, a, b):
    future = client.submit(sys.exit, 0)
    await wait(future)

    assert s.task_prefixes["exit"].suspicious == 1
    assert sum(tp.suspicious for tp in s.task_prefixes.values()) == sum(
        ts.suspicious for ts in s.tasks.values()
    )


@gen_cluster(client=True)
async def test_task_group_non_tuple_key(c, s, a, b):
    da = pytest.importorskip("dask.array")
    np = pytest.importorskip("numpy")
    x = da.arange(100, chunks=(20,))
    y = (x + 1).sum().persist()
    y = await y

    assert s.task_prefixes["sum"].states["released"] == 4
    assert "sum" not in s.task_groups

    f = c.submit(np.sum, [1, 2, 3])
    await f

    assert s.task_prefixes["sum"].states["released"] == 4
    assert s.task_prefixes["sum"].states["memory"] == 1
    assert "sum" in s.task_groups


@gen_cluster(client=True)
async def test_task_unique_groups(c, s, a, b):
    """ This test ensure that task groups remain unique when using submit
    """
    x = c.submit(sum, [1, 2])
    y = c.submit(len, [1, 2])
    z = c.submit(sum, [3, 4])
    await asyncio.wait([x, y, z])

    assert s.task_prefixes["len"].states["memory"] == 1
    assert s.task_prefixes["sum"].states["memory"] == 2


@gen_cluster(client=True)
async def test_task_group_on_fire_and_forget(c, s, a, b):
    # Regression test for https://github.com/dask/distributed/issues/3465
    with captured_logger("distributed.scheduler") as logs:
        x = await c.scatter(list(range(10)))
        fire_and_forget([c.submit(slowadd, i, x[i]) for i in range(len(x))])
        await asyncio.sleep(1)

    assert "Error transitioning" not in logs.getvalue()


class BrokenComm(Comm):
    peer_address = None
    local_address = None

    def close(self):
        pass

    def closed(self):
        pass

    def abort(self):
        pass

    def read(self, deserializers=None):
        raise EnvironmentError

    def write(self, msg, serializers=None, on_error=None):
        raise EnvironmentError


class FlakyConnectionPool(ConnectionPool):
    def __init__(self, *args, failing_connections=0, **kwargs):
        self.cnn_count = 0
        self.failing_connections = failing_connections
        super(FlakyConnectionPool, self).__init__(*args, **kwargs)

    async def connect(self, *args, **kwargs):
        self.cnn_count += 1
        if self.cnn_count > self.failing_connections:
            return await super(FlakyConnectionPool, self).connect(*args, **kwargs)
        else:
            return BrokenComm()


@gen_cluster(client=True)
async def test_gather_failing_cnn_recover(c, s, a, b):
    orig_rpc = s.rpc
    x = await c.scatter({"x": 1}, workers=a.address)

    s.rpc = await FlakyConnectionPool(failing_connections=1)
    with dask.config.set({"distributed.comm.retry.count": 1}):
        res = await s.gather(keys=["x"])
    assert res["status"] == "OK"


@gen_cluster(client=True)
async def test_gather_failing_cnn_error(c, s, a, b):
    orig_rpc = s.rpc
    x = await c.scatter({"x": 1}, workers=a.address)

    s.rpc = await FlakyConnectionPool(failing_connections=10)
    res = await s.gather(keys=["x"])
    assert res["status"] == "error"
    assert list(res["keys"]) == ["x"]


@gen_cluster(client=True)
async def test_gather_no_workers(c, s, a, b):
    await asyncio.sleep(1)
    x = await c.scatter({"x": 1}, workers=a.address)

    await a.close()
    await b.close()

    res = await s.gather(keys=["x"])
    assert res["status"] == "error"
    assert list(res["keys"]) == ["x"]


@gen_cluster(client=True, client_kwargs={"direct_to_workers": False})
async def test_gather_allow_worker_reconnect(c, s, a, b):
    """
    Test that client resubmissions allow failed workers to reconnect and re-use
    their results. Failure scenario would be a connection issue during result
    gathering.
    Upon connection failure, the worker is flagged as suspicious and removed
    from the scheduler. If the worker is healthy and reconnencts we want to use
    its results instead of recomputing them.
    """
    # GH3246
    ALREADY_CALCULATED = []

    import time

    def inc_slow(x):
        # Once the graph below is rescheduled this computation runs again. We
        # need to sleep for at least 0.5 seconds to give the worker a chance to
        # reconnect (Heartbeat timing)
        if x in ALREADY_CALCULATED:
            time.sleep(1)
        ALREADY_CALCULATED.append(x)
        return x + 1

    x = c.submit(inc_slow, 1)
    y = c.submit(inc_slow, 2)

    def reducer(x, y):
        return x + y

    z = c.submit(reducer, x, y)

    s.rpc = await FlakyConnectionPool(failing_connections=4)

    with dask.config.set(
        {"distributed.comm.retry.delay_min": 0.5, "distributed.comm.retry.count": 3,}
    ):
        with captured_logger(
            logging.getLogger("distributed.scheduler")
        ) as sched_logger, captured_logger(
            logging.getLogger("distributed.client")
        ) as client_logger, captured_logger(
            logging.getLogger("distributed.utils_comm")
        ) as utils_comm_logger:
            # Gather using the client (as an ordinary user would)
            # Upon a missing key, the client will reschedule the computations
            res = await c.gather(z)

    assert res == 5

    sched_logger = sched_logger.getvalue()
    client_logger = client_logger.getvalue()
    utils_comm_logger = utils_comm_logger.getvalue()

    # Ensure that the communication was done via the scheduler, i.e. we actually hit a bad connection
    assert s.rpc.cnn_count > 0

    assert "Retrying get_data_from_worker after exception" in utils_comm_logger

    # The reducer task was actually not found upon first collection. The client will reschedule the graph
    assert "Couldn't gather 1 keys, rescheduling" in client_logger
    # There will also be a `Unexpected worker completed task` message but this
    # is rather an artifact and not the intention
    assert "Workers don't have promised key" in sched_logger

    # Once the worker reconnects, it will also submit the keys it holds such
    # that the scheduler again knows about the result.
    # The final reduce step should then be used from the re-connected worker
    # instead of recomputing it.
    transitions_to_processing = [
        (key, start, timestamp)
        for key, start, finish, recommendations, timestamp in s.transition_log
        if finish == "processing" and "reducer" in key
    ]
    assert len(transitions_to_processing) == 1

    starts = []
    finish_processing_transitions = 0
    for transition in s.transition_log:
        key, start, finish, recommendations, timestamp = transition
        if "reducer" in key and finish == "processing":
            finish_processing_transitions += 1
    assert finish_processing_transitions == 1


@gen_cluster(client=True)
async def test_too_many_groups(c, s, a, b):
    x = dask.delayed(inc)(1)
    y = dask.delayed(dec)(2)
    z = dask.delayed(operator.add)(x, y)

    await c.compute(z)

    while s.tasks:
        await asyncio.sleep(0.01)

    assert len(s.task_groups) < 3


@pytest.mark.asyncio
async def test_multiple_listeners(cleanup):
    with captured_logger(logging.getLogger("distributed.scheduler")) as log:
        async with Scheduler(port=0, protocol=["inproc", "tcp"]) as s:
            async with Worker(s.listeners[0].contact_address) as a:
                async with Worker(s.listeners[1].contact_address) as b:
                    assert a.address.startswith("inproc")
                    assert a.scheduler.address.startswith("inproc")
                    assert b.address.startswith("tcp")
                    assert b.scheduler.address.startswith("tcp")

                    async with Client(s.address, asynchronous=True) as c:
                        futures = c.map(inc, range(20))
                        await wait(futures)

                        # Force inter-worker communication both ways
                        await c.submit(sum, futures, workers=[a.address])
                        await c.submit(len, futures, workers=[b.address])

    log = log.getvalue()
    assert re.search(r"Scheduler at:\s*tcp://", log)
    assert re.search(r"Scheduler at:\s*inproc://", log)


@gen_cluster(nthreads=[("127.0.0.1", 1)])
async def test_worker_name_collision(s, a):
    # test that a name collision for workers produces the expected respsone
    # and leaves the data structures of Scheduler in a good state
    # is not updated by the second worker
    with pytest.raises(ValueError, match=f"name taken, {a.name!r}"):
        await Worker(s.address, name=a.name, loop=s.loop, host="127.0.0.1")

    s.validate_state()
    assert set(s.workers) == {a.address}
    assert s.aliases == {a.name: a.address}


@gen_cluster(client=True, config={"distributed.scheduler.unknown-task-duration": "1h"})
async def test_unknown_task_duration_config(client, s, a, b):
    future = client.submit(slowinc, 1)
    while not s.tasks:
        await asyncio.sleep(0.001)
    assert sum(s.get_task_duration(ts) for ts in s.tasks.values()) == 3600
    assert len(s.unknown_durations) == 1
    await wait(future)
    assert len(s.unknown_durations) == 0


@gen_cluster()
async def test_unknown_task_duration_config(s, a, b):
    assert s.idle_since == s.time_started


@gen_cluster(client=True, timeout=1000)
async def test_retire_state_change(c, s, a, b):
    np = pytest.importorskip("numpy")
    y = c.map(lambda x: x ** 2, range(10))
    await c.scatter(y)
    for x in range(2):
        v = c.map(lambda i: i * np.random.randint(1000), y)
        k = c.map(lambda i: i * np.random.randint(1000), v)
        foo = c.map(lambda j: j * 6, k)
        step = c.compute(foo)
        c.gather(step)
    await c.retire_workers(workers=[a.address])
