from concurrent.futures import ThreadPoolExecutor
import importlib
import logging
from numbers import Number
from operator import add
import os
import psutil
import sys
from time import sleep
import traceback
import asyncio

import dask
from dask import delayed
from dask.utils import format_bytes
from dask.system import CPU_COUNT
import pytest
from tlz import pluck, sliding_window, first

from distributed import (
    Client,
    Nanny,
    get_client,
    default_client,
    get_worker,
    Reschedule,
    wait,
)
from distributed.compatibility import WINDOWS
from distributed.core import rpc, CommClosedError
from distributed.scheduler import Scheduler
from distributed.metrics import time
from distributed.worker import (
    Worker,
    error_message,
    logger,
    parse_memory_limit,
)
from distributed.utils import tmpfile, TimeoutError
from distributed.utils_test import (  # noqa: F401
    cleanup,
    inc,
    mul,
    gen_cluster,
    div,
    dec,
    slowinc,
    gen_test,
    captured_logger,
)
from distributed.utils_test import (  # noqa: F401
    client,
    loop,
    nodebug,
    cluster_fixture,
    s,
    a,
    b,
)


@pytest.mark.asyncio
async def test_worker_nthreads(cleanup):
    async with Scheduler() as s:
        async with Worker(s.address) as w:
            assert w.executor._max_workers == CPU_COUNT


@gen_cluster()
async def test_str(s, a, b):
    assert a.address in str(a)
    assert a.address in repr(a)
    assert str(a.nthreads) in str(a)
    assert str(a.nthreads) in repr(a)
    assert str(len(a.executing)) in repr(a)


@pytest.mark.asyncio
async def test_identity(cleanup):
    async with Scheduler() as s:
        async with Worker(s.address) as w:
            ident = w.identity(None)
            assert "Worker" in ident["type"]
            assert ident["scheduler"] == s.address
            assert isinstance(ident["nthreads"], int)
            assert isinstance(ident["memory_limit"], Number)


@gen_cluster(client=True)
async def test_worker_bad_args(c, s, a, b):
    class NoReprObj:
        """ This object cannot be properly represented as a string. """

        def __str__(self):
            raise ValueError("I have no str representation.")

        def __repr__(self):
            raise ValueError("I have no repr representation.")

    x = c.submit(NoReprObj, workers=a.address)
    await wait(x)
    assert not a.executing
    assert a.data

    def bad_func(*args, **kwargs):
        1 / 0

    class MockLoggingHandler(logging.Handler):
        """Mock logging handler to check for expected logs."""

        def __init__(self, *args, **kwargs):
            self.reset()
            logging.Handler.__init__(self, *args, **kwargs)

        def emit(self, record):
            self.messages[record.levelname.lower()].append(record.getMessage())

        def reset(self):
            self.messages = {
                "debug": [],
                "info": [],
                "warning": [],
                "error": [],
                "critical": [],
            }

    hdlr = MockLoggingHandler()
    old_level = logger.level
    logger.setLevel(logging.DEBUG)
    logger.addHandler(hdlr)
    y = c.submit(bad_func, x, k=x, workers=b.address)
    await wait(y)

    assert not b.executing
    assert y.status == "error"
    # Make sure job died because of bad func and not because of bad
    # argument.
    with pytest.raises(ZeroDivisionError):
        await y

    tb = await y._traceback()
    assert any("1 / 0" in line for line in pluck(3, traceback.extract_tb(tb)) if line)
    assert "Compute Failed" in hdlr.messages["warning"][0]
    logger.setLevel(old_level)

    # Now we check that both workers are still alive.

    xx = c.submit(add, 1, 2, workers=a.address)
    yy = c.submit(add, 3, 4, workers=b.address)

    results = await c._gather([xx, yy])

    assert tuple(results) == (3, 7)


@pytest.mark.slow
@gen_cluster()
async def dont_test_delete_data_with_missing_worker(c, a, b):
    bad = "127.0.0.1:9001"  # this worker doesn't exist
    c.who_has["z"].add(bad)
    c.who_has["z"].add(a.address)
    c.has_what[bad].add("z")
    c.has_what[a.address].add("z")
    a.data["z"] = 5

    cc = rpc(ip=c.ip, port=c.port)

    await cc.delete_data(keys=["z"])  # TODO: this hangs for a while
    assert "z" not in a.data
    assert not c.who_has["z"]
    assert not c.has_what[bad]
    assert not c.has_what[a.address]

    await cc.close_rpc()


@gen_cluster(client=True)
async def test_upload_file(c, s, a, b):
    assert not os.path.exists(os.path.join(a.local_directory, "foobar.py"))
    assert not os.path.exists(os.path.join(b.local_directory, "foobar.py"))
    assert a.local_directory != b.local_directory

    with rpc(a.address) as aa, rpc(b.address) as bb:
        await asyncio.gather(
            aa.upload_file(filename="foobar.py", data=b"x = 123"),
            bb.upload_file(filename="foobar.py", data="x = 123"),
        )

    assert os.path.exists(os.path.join(a.local_directory, "foobar.py"))
    assert os.path.exists(os.path.join(b.local_directory, "foobar.py"))

    def g():
        import foobar

        return foobar.x

    future = c.submit(g, workers=a.address)
    result = await future
    assert result == 123

    await c.close()
    await s.close(close_workers=True)
    assert not os.path.exists(os.path.join(a.local_directory, "foobar.py"))


@pytest.mark.skip(reason="don't yet support uploading pyc files")
@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)])
async def test_upload_file_pyc(c, s, w):
    with tmpfile() as dirname:
        os.mkdir(dirname)
        with open(os.path.join(dirname, "foo.py"), mode="w") as f:
            f.write("def f():\n    return 123")

        sys.path.append(dirname)
        try:
            import foo

            assert foo.f() == 123
            pyc = importlib.util.cache_from_source(os.path.join(dirname, "foo.py"))
            assert os.path.exists(pyc)
            await c.upload_file(pyc)

            def g():
                import foo

                return foo.x

            future = c.submit(g)
            result = await future
            assert result == 123
        finally:
            sys.path.remove(dirname)


@gen_cluster(client=True)
async def test_upload_egg(c, s, a, b):
    eggname = "testegg-1.0.0-py3.4.egg"
    local_file = __file__.replace("test_worker.py", eggname)
    assert not os.path.exists(os.path.join(a.local_directory, eggname))
    assert not os.path.exists(os.path.join(b.local_directory, eggname))
    assert a.local_directory != b.local_directory

    await c.upload_file(filename=local_file)

    assert os.path.exists(os.path.join(a.local_directory, eggname))
    assert os.path.exists(os.path.join(b.local_directory, eggname))

    def g(x):
        import testegg

        return testegg.inc(x)

    future = c.submit(g, 10, workers=a.address)
    result = await future
    assert result == 10 + 1

    await c.close()
    await s.close()
    await a.close()
    await b.close()
    assert not os.path.exists(os.path.join(a.local_directory, eggname))


@gen_cluster(client=True)
async def test_upload_pyz(c, s, a, b):
    pyzname = "mytest.pyz"
    local_file = __file__.replace("test_worker.py", pyzname)
    assert not os.path.exists(os.path.join(a.local_directory, pyzname))
    assert not os.path.exists(os.path.join(b.local_directory, pyzname))
    assert a.local_directory != b.local_directory

    await c.upload_file(filename=local_file)

    assert os.path.exists(os.path.join(a.local_directory, pyzname))
    assert os.path.exists(os.path.join(b.local_directory, pyzname))

    def g(x):
        from mytest import mytest

        return mytest.inc(x)

    future = c.submit(g, 10, workers=a.address)
    result = await future
    assert result == 10 + 1

    await c.close()
    await s.close()
    await a.close()
    await b.close()
    assert not os.path.exists(os.path.join(a.local_directory, pyzname))


@pytest.mark.xfail(reason="Still lose time to network I/O")
@gen_cluster(client=True)
async def test_upload_large_file(c, s, a, b):
    pytest.importorskip("crick")
    await asyncio.sleep(0.05)
    with rpc(a.address) as aa:
        await aa.upload_file(filename="myfile.dat", data=b"0" * 100000000)
        await asyncio.sleep(0.05)
        assert a.digests["tick-duration"].components[0].max() < 0.050


@gen_cluster()
async def test_broadcast(s, a, b):
    with rpc(s.address) as cc:
        results = await cc.broadcast(msg={"op": "ping"})
        assert results == {a.address: b"pong", b.address: b"pong"}


@gen_test()
async def test_worker_with_port_zero():
    s = await Scheduler(port=8007)
    w = await Worker(s.address)
    assert isinstance(w.port, int)
    assert w.port > 1024

    await w.close()


@pytest.mark.asyncio
async def test_worker_port_range(cleanup):
    async with Scheduler() as s:
        port = "9867:9868"
        async with Worker(s.address, port=port) as w1:
            assert w1.port == 9867  # Selects first port in range
            async with Worker(s.address, port=port) as w2:
                assert w2.port == 9868  # Selects next port in range
                with pytest.raises(
                    ValueError, match="Could not start Worker"
                ):  # No more ports left
                    async with Worker(s.address, port=port):
                        pass


@pytest.mark.slow
@pytest.mark.asyncio
async def test_worker_waits_for_scheduler(cleanup):
    w = Worker("127.0.0.1:8724")
    try:
        await asyncio.wait_for(w, 3)
    except TimeoutError:
        pass
    else:
        assert False
    assert w.status not in ("closed", "running")
    await w.close(timeout=0.1)


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)])
async def test_worker_task_data(c, s, w):
    x = delayed(2)
    xx = c.persist(x)
    await wait(xx)
    assert w.data[x.key] == 2


def test_error_message():
    class MyException(Exception):
        def __init__(self, a, b):
            self.args = (a + b,)

        def __str__(self):
            return "MyException(%s)" % self.args

    msg = error_message(MyException("Hello", "World!"))
    assert "Hello" in str(msg["exception"])

    max_error_len = 100
    with dask.config.set({"distributed.admin.max-error-length": max_error_len}):
        msg = error_message(RuntimeError("-" * max_error_len))
        assert len(msg["text"]) <= max_error_len
        assert len(msg["text"]) < max_error_len * 2
        msg = error_message(RuntimeError("-" * max_error_len * 20))
        cut_text = msg["text"].replace("('Long error message', '", "")[:-2]
        assert len(cut_text) == max_error_len

    max_error_len = 1000000
    with dask.config.set({"distributed.admin.max-error-length": max_error_len}):
        msg = error_message(RuntimeError("-" * max_error_len * 2))
        cut_text = msg["text"].replace("('Long error message', '", "")[:-2]
        assert len(cut_text) == max_error_len
        assert len(msg["text"]) > 10100  # default + 100


@gen_cluster(client=True)
async def test_chained_error_message(c, s, a, b):
    def chained_exception_fn():
        class MyException(Exception):
            def __init__(self, msg):
                self.msg = msg

            def __str__(self):
                return "MyException(%s)" % self.msg

        exception = MyException("Foo")
        inner_exception = MyException("Bar")

        try:
            raise inner_exception
        except Exception as e:
            raise exception from e

    f = c.submit(chained_exception_fn)
    try:
        await f
    except Exception as e:
        assert e.__cause__ is not None
        assert "Bar" in str(e.__cause__)


@gen_cluster()
async def test_gather(s, a, b):
    b.data["x"] = 1
    b.data["y"] = 2
    with rpc(a.address) as aa:
        resp = await aa.gather(who_has={"x": [b.address], "y": [b.address]})
        assert resp["status"] == "OK"

        assert a.data["x"] == b.data["x"]
        assert a.data["y"] == b.data["y"]


@pytest.mark.asyncio
async def test_io_loop(cleanup):
    async with Scheduler(port=0) as s:
        async with Worker(s.address, loop=s.loop) as w:
            assert w.io_loop is s.loop


@gen_cluster(client=True, nthreads=[])
async def test_spill_to_disk(c, s):
    np = pytest.importorskip("numpy")
    w = await Worker(
        s.address,
        loop=s.loop,
        memory_limit=1200 / 0.6,
        memory_pause_fraction=None,
        memory_spill_fraction=None,
    )

    x = c.submit(np.random.randint, 0, 255, size=500, dtype="u1", key="x")
    await wait(x)
    y = c.submit(np.random.randint, 0, 255, size=500, dtype="u1", key="y")
    await wait(y)

    assert set(w.data) == {x.key, y.key}
    assert set(w.data.memory) == {x.key, y.key}
    assert set(w.data.fast) == set(w.data.memory)

    z = c.submit(np.random.randint, 0, 255, size=500, dtype="u1", key="z")
    await wait(z)
    assert set(w.data) == {x.key, y.key, z.key}
    assert set(w.data.memory) == {y.key, z.key}
    assert set(w.data.disk) == {x.key} or set(w.data.slow) == {x.key, y.key}
    assert set(w.data.fast) == set(w.data.memory)
    assert set(w.data.slow) == set(w.data.disk)

    await x
    assert set(w.data.memory) == {x.key, z.key}
    assert set(w.data.disk) == {y.key} or set(w.data.slow) == {x.key, y.key}
    assert set(w.data.fast) == set(w.data.memory)
    assert set(w.data.slow) == set(w.data.disk)
    await w.close()


@gen_cluster(client=True)
async def test_access_key(c, s, a, b):
    def f(i):
        from distributed.worker import thread_state

        return thread_state.key

    futures = [c.submit(f, i, key="x-%d" % i) for i in range(20)]
    results = await c._gather(futures)
    assert list(results) == ["x-%d" % i for i in range(20)]


@gen_cluster(client=True)
async def test_run_dask_worker(c, s, a, b):
    def f(dask_worker=None):
        return dask_worker.id

    response = await c._run(f)
    assert response == {a.address: a.id, b.address: b.id}


@gen_cluster(client=True)
async def test_run_coroutine_dask_worker(c, s, a, b):
    async def f(dask_worker=None):
        await asyncio.sleep(0.001)
        return dask_worker.id

    response = await c.run(f)
    assert response == {a.address: a.id, b.address: b.id}


@gen_cluster(client=True, nthreads=[])
async def test_Executor(c, s):
    with ThreadPoolExecutor(2) as e:
        w = Worker(s.address, executor=e)
        assert w.executor is e
        w = await w

        future = c.submit(inc, 1)
        result = await future
        assert result == 2

        assert e._threads  # had to do some work

        await w.close()


@pytest.mark.skip(
    reason="Other tests leak memory, so process-level checks trigger immediately"
)
@gen_cluster(
    client=True,
    nthreads=[("127.0.0.1", 1)],
    timeout=30,
    worker_kwargs={"memory_limit": 10e6},
)
async def test_spill_by_default(c, s, w):
    da = pytest.importorskip("dask.array")
    x = da.ones(int(10e6 * 0.7), chunks=1e6, dtype="u1")
    y = c.persist(x)
    await wait(y)
    assert len(w.data.disk)  # something is on disk
    del x, y


@gen_cluster(nthreads=[("127.0.0.1", 1)], worker_kwargs={"reconnect": False})
async def test_close_on_disconnect(s, w):
    await s.close()

    start = time()
    while w.status != "closed":
        await asyncio.sleep(0.01)
        assert time() < start + 5


@pytest.mark.asyncio
async def test_memory_limit_auto():
    async with Scheduler() as s:
        async with Worker(s.address, nthreads=1) as a, Worker(
            s.address, nthreads=2
        ) as b, Worker(s.address, nthreads=100) as c, Worker(
            s.address, nthreads=200
        ) as d:
            assert isinstance(a.memory_limit, Number)
            assert isinstance(b.memory_limit, Number)

            if CPU_COUNT > 1:
                assert a.memory_limit < b.memory_limit

            assert c.memory_limit == d.memory_limit


@gen_cluster(client=True)
async def test_inter_worker_communication(c, s, a, b):
    [x, y] = await c._scatter([1, 2], workers=a.address)

    future = c.submit(add, x, y, workers=b.address)
    result = await future
    assert result == 3


@gen_cluster(client=True)
async def test_clean(c, s, a, b):
    x = c.submit(inc, 1, workers=a.address)
    y = c.submit(inc, x, workers=b.address)

    await y

    collections = [
        a.tasks,
        a.task_state,
        a.startstops,
        a.data,
        a.nbytes,
        a.durations,
        a.priorities,
        a.types,
        a.threads,
    ]
    for c in collections:
        assert c

    x.release()
    y.release()

    while x.key in a.task_state:
        await asyncio.sleep(0.01)

    for c in collections:
        assert not c


@gen_cluster(client=True)
async def test_message_breakup(c, s, a, b):
    n = 100000
    a.target_message_size = 10 * n
    b.target_message_size = 10 * n
    xs = [c.submit(mul, b"%d" % i, n, workers=a.address) for i in range(30)]
    y = c.submit(lambda *args: None, xs, workers=b.address)
    await y

    assert 2 <= len(b.incoming_transfer_log) <= 20
    assert 2 <= len(a.outgoing_transfer_log) <= 20

    assert all(msg["who"] == b.address for msg in a.outgoing_transfer_log)
    assert all(msg["who"] == a.address for msg in a.incoming_transfer_log)


@gen_cluster(client=True)
async def test_types(c, s, a, b):
    assert not a.types
    assert not b.types
    x = c.submit(inc, 1, workers=a.address)
    await wait(x)
    assert a.types[x.key] == int

    y = c.submit(inc, x, workers=b.address)
    await wait(y)
    assert b.types == {x.key: int, y.key: int}

    await c._cancel(y)

    start = time()
    while y.key in b.data:
        await asyncio.sleep(0.01)
        assert time() < start + 5

    assert y.key not in b.types


@gen_cluster()
async def test_system_monitor(s, a, b):
    assert b.monitor
    b.monitor.update()


@gen_cluster(
    client=True, nthreads=[("127.0.0.1", 2, {"resources": {"A": 1}}), ("127.0.0.1", 1)]
)
async def test_restrictions(c, s, a, b):
    # Resource restrictions
    x = c.submit(inc, 1, resources={"A": 1})
    await x
    assert a.resource_restrictions == {x.key: {"A": 1}}
    await c._cancel(x)

    while x.key in a.task_state:
        await asyncio.sleep(0.01)

    assert a.resource_restrictions == {}


@pytest.mark.xfail
@gen_cluster(client=True)
async def test_clean_nbytes(c, s, a, b):
    L = [delayed(inc)(i) for i in range(10)]
    for i in range(5):
        L = [delayed(add)(x, y) for x, y in sliding_window(2, L)]
    total = delayed(sum)(L)

    future = c.compute(total)
    await wait(future)

    await asyncio.sleep(1)
    assert len(a.nbytes) + len(b.nbytes) == 1


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 20)
async def test_gather_many_small(c, s, a, *workers):
    a.total_out_connections = 2
    futures = await c._scatter(list(range(100)))

    assert all(w.data for w in workers)

    def f(*args):
        return 10

    future = c.submit(f, *futures, workers=a.address)
    await wait(future)

    types = list(pluck(0, a.log))
    req = [i for i, t in enumerate(types) if t == "request-dep"]
    recv = [i for i, t in enumerate(types) if t == "receive-dep"]
    assert min(recv) > max(req)

    assert a.comm_nbytes == 0


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 3)
async def test_multiple_transfers(c, s, w1, w2, w3):
    x = c.submit(inc, 1, workers=w1.address)
    y = c.submit(inc, 2, workers=w2.address)
    z = c.submit(add, x, y, workers=w3.address)

    await wait(z)

    r = w3.startstops[z.key]
    transfers = [t for t in r if t["action"] == "transfer"]
    assert len(transfers) == 2


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 3)
async def test_share_communication(c, s, w1, w2, w3):
    x = c.submit(mul, b"1", int(w3.target_message_size + 1), workers=w1.address)
    y = c.submit(mul, b"2", int(w3.target_message_size + 1), workers=w2.address)
    await wait([x, y])
    await c._replicate([x, y], workers=[w1.address, w2.address])
    z = c.submit(add, x, y, workers=w3.address)
    await wait(z)
    assert len(w3.incoming_transfer_log) == 2
    assert w1.outgoing_transfer_log
    assert w2.outgoing_transfer_log


@gen_cluster(client=True)
async def test_dont_overlap_communications_to_same_worker(c, s, a, b):
    x = c.submit(mul, b"1", int(b.target_message_size + 1), workers=a.address)
    y = c.submit(mul, b"2", int(b.target_message_size + 1), workers=a.address)
    await wait([x, y])
    z = c.submit(add, x, y, workers=b.address)
    await wait(z)
    assert len(b.incoming_transfer_log) == 2
    l1, l2 = b.incoming_transfer_log

    assert l1["stop"] < l2["start"]


@pytest.mark.avoid_travis
@gen_cluster(client=True)
async def test_log_exception_on_failed_task(c, s, a, b):
    with tmpfile() as fn:
        fh = logging.FileHandler(fn)
        try:
            from distributed.worker import logger

            logger.addHandler(fh)

            future = c.submit(div, 1, 0)
            await wait(future)

            await asyncio.sleep(0.1)
            fh.flush()
            with open(fn) as f:
                text = f.read()

            assert "ZeroDivisionError" in text
            assert "Exception" in text
        finally:
            logger.removeHandler(fh)


@gen_cluster(client=True)
async def test_clean_up_dependencies(c, s, a, b):
    x = delayed(inc)(1)
    y = delayed(inc)(2)
    xx = delayed(inc)(x)
    yy = delayed(inc)(y)
    z = delayed(add)(xx, yy)

    zz = c.persist(z)
    await wait(zz)

    start = time()
    while len(a.data) + len(b.data) > 1:
        await asyncio.sleep(0.01)
        assert time() < start + 2

    assert set(a.data) | set(b.data) == {zz.key}


@gen_cluster(client=True)
async def test_hold_onto_dependents(c, s, a, b):
    x = c.submit(inc, 1, workers=a.address)
    y = c.submit(inc, x, workers=b.address)
    await wait(y)

    assert x.key in b.data

    await c._cancel(y)
    await asyncio.sleep(0.1)

    assert x.key in b.data


@pytest.mark.slow
@gen_cluster(client=False, nthreads=[])
async def test_worker_death_timeout(s):
    with dask.config.set({"distributed.comm.timeouts.connect": "1s"}):
        await s.close()
        w = Worker(s.address, death_timeout=1)

    with pytest.raises(TimeoutError) as info:
        await w

    assert "Worker" in str(info.value)
    assert "timed out" in str(info.value) or "failed to start" in str(info.value)

    assert w.status == "closed"


@gen_cluster(client=True)
async def test_stop_doing_unnecessary_work(c, s, a, b):
    futures = c.map(slowinc, range(1000), delay=0.01)
    await asyncio.sleep(0.1)

    del futures

    start = time()
    while a.executing:
        await asyncio.sleep(0.01)
        assert time() - start < 0.5


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)])
async def test_priorities(c, s, w):
    values = []
    for i in range(10):
        a = delayed(slowinc)(i, dask_key_name="a-%d" % i, delay=0.01)
        a1 = delayed(inc)(a, dask_key_name="a1-%d" % i)
        a2 = delayed(inc)(a1, dask_key_name="a2-%d" % i)
        b1 = delayed(dec)(a, dask_key_name="b1-%d" % i)  # <<-- least favored

        values.append(a2)
        values.append(b1)

    futures = c.compute(values)
    await wait(futures)

    log = [
        t[0]
        for t in w.log
        if t[1] == "executing" and t[2] == "memory" and not t[0].startswith("finalize")
    ]

    assert any(key.startswith("b1") for key in log[: len(log) // 2])


@gen_cluster(client=True)
async def test_heartbeats(c, s, a, b):
    x = s.workers[a.address].last_seen
    start = time()
    await asyncio.sleep(a.periodic_callbacks["heartbeat"].callback_time / 1000 + 0.1)
    while s.workers[a.address].last_seen == x:
        await asyncio.sleep(0.01)
        assert time() < start + 2
    assert a.periodic_callbacks["heartbeat"].callback_time < 1000


@pytest.mark.parametrize("worker", [Worker, Nanny])
def test_worker_dir(worker):
    with tmpfile() as fn:

        @gen_cluster(client=True, worker_kwargs={"local_directory": fn})
        async def test_worker_dir(c, s, a, b):
            directories = [w.local_directory for w in s.workers.values()]
            assert all(d.startswith(fn) for d in directories)
            assert len(set(directories)) == 2  # distinct

        test_worker_dir()


@gen_cluster(client=True)
async def test_dataframe_attribute_error(c, s, a, b):
    class BadSize:
        def __init__(self, data):
            self.data = data

        def __sizeof__(self):
            raise TypeError("Hello")

    future = c.submit(BadSize, 123)
    result = await future
    assert result.data == 123


@gen_cluster(client=True)
async def test_fail_write_to_disk(c, s, a, b):
    class Bad:
        def __getstate__(self):
            raise TypeError()

        def __sizeof__(self):
            return int(100e9)

    future = c.submit(Bad)
    await wait(future)

    assert future.status == "error"

    with pytest.raises(TypeError):
        await future

    futures = c.map(inc, range(10))
    results = await c._gather(futures)
    assert results == list(map(inc, range(10)))


@pytest.mark.skip(reason="Our logic here is faulty")
@gen_cluster(
    nthreads=[("127.0.0.1", 2)], client=True, worker_kwargs={"memory_limit": 10e9}
)
async def test_fail_write_many_to_disk(c, s, a):
    a.validate = False
    await asyncio.sleep(0.1)
    assert not a.paused

    class Bad:
        def __init__(self, x):
            pass

        def __getstate__(self):
            raise TypeError()

        def __sizeof__(self):
            return int(2e9)

    futures = c.map(Bad, range(11))
    future = c.submit(lambda *args: 123, *futures)

    await wait(future)

    with pytest.raises(Exception) as info:
        await future

    # workers still operational
    result = await c.submit(inc, 1, workers=a.address)
    assert result == 2


@gen_cluster()
async def test_pid(s, a, b):
    assert s.workers[a.address].pid == os.getpid()


@gen_cluster(client=True)
async def test_get_client(c, s, a, b):
    def f(x):
        cc = get_client()
        future = cc.submit(inc, x)
        return future.result()

    assert default_client() is c

    future = c.submit(f, 10, workers=a.address)
    result = await future
    assert result == 11

    assert a._client
    assert not b._client

    assert a._client is c
    assert default_client() is c

    a_client = a._client

    for i in range(10):
        await wait(c.submit(f, i))

    assert a._client is a_client


def test_get_client_sync(client):
    def f(x):
        cc = get_client()
        future = cc.submit(inc, x)
        return future.result()

    future = client.submit(f, 10)
    assert future.result() == 11


@gen_cluster(client=True)
async def test_get_client_coroutine(c, s, a, b):
    async def f():
        client = await get_client()
        future = client.submit(inc, 10)
        result = await future
        return result

    results = await c.run(f)
    assert results == {a.address: 11, b.address: 11}


def test_get_client_coroutine_sync(client, s, a, b):
    async def f():
        client = await get_client()
        future = client.submit(inc, 10)
        result = await future
        return result

    results = client.run(f)
    assert results == {a["address"]: 11, b["address"]: 11}


@gen_cluster()
async def test_global_workers(s, a, b):
    n = len(Worker._instances)
    w = first(Worker._instances)
    assert w is a or w is b


@pytest.mark.skipif(WINDOWS, reason="file descriptors")
@gen_cluster(nthreads=[])
async def test_worker_fds(s):
    psutil = pytest.importorskip("psutil")
    await asyncio.sleep(0.05)
    start = psutil.Process().num_fds()

    worker = await Worker(s.address, loop=s.loop)
    await asyncio.sleep(0.1)
    middle = psutil.Process().num_fds()
    start = time()
    while middle > start:
        await asyncio.sleep(0.01)
        assert time() < start + 1

    await worker.close()

    start = time()
    while psutil.Process().num_fds() > start:
        await asyncio.sleep(0.01)
        assert time() < start + 0.5


@gen_cluster(nthreads=[])
async def test_service_hosts_match_worker(s):
    async with Worker(s.address, host="tcp://0.0.0.0") as w:
        sock = first(w.http_server._sockets.values())
        assert sock.getsockname()[0] in ("::", "0.0.0.0")

    async with Worker(
        s.address, host="tcp://127.0.0.1", dashboard_address="0.0.0.0:0"
    ) as w:
        sock = first(w.http_server._sockets.values())
        assert sock.getsockname()[0] in ("::", "0.0.0.0")

    async with Worker(s.address, host="tcp://127.0.0.1") as w:
        sock = first(w.http_server._sockets.values())
        assert sock.getsockname()[0] == "127.0.0.1"


@gen_cluster(nthreads=[])
async def test_start_services(s):
    async with Worker(s.address, dashboard_address=1234) as w:
        assert w.http_server.port == 1234


@gen_test()
async def test_scheduler_file():
    with tmpfile() as fn:
        s = await Scheduler(scheduler_file=fn, port=8009)
        w = await Worker(scheduler_file=fn)
        assert set(s.workers) == {w.address}
        await w.close()
        s.stop()


@gen_cluster(client=True)
async def test_scheduler_delay(c, s, a, b):
    old = a.scheduler_delay
    assert abs(a.scheduler_delay) < 0.3
    assert abs(b.scheduler_delay) < 0.3
    await asyncio.sleep(a.periodic_callbacks["heartbeat"].callback_time / 1000 + 0.3)
    assert a.scheduler_delay != old


@gen_cluster(client=True)
async def test_statistical_profiling(c, s, a, b):
    futures = c.map(slowinc, range(10), delay=0.1)
    await wait(futures)

    profile = a.profile_keys["slowinc"]
    assert profile["count"]


@pytest.mark.slow
@nodebug
@gen_cluster(
    client=True,
    timeout=30,
    config={
        "distributed.worker.profile.interval": "1ms",
        "distributed.worker.profile.cycle": "100ms",
    },
)
async def test_statistical_profiling_2(c, s, a, b):
    da = pytest.importorskip("dask.array")
    while True:
        x = da.random.random(1000000, chunks=(10000,))
        y = (x + x * 2) - x.sum().persist()
        await wait(y)

        profile = await a.get_profile()
        text = str(profile)
        if profile["count"] and "sum" in text and "random" in text:
            break


@gen_cluster(
    nthreads=[("127.0.0.1", 1)],
    client=True,
    worker_kwargs={"memory_monitor_interval": 10},
)
async def test_robust_to_bad_sizeof_estimates(c, s, a):
    np = pytest.importorskip("numpy")
    memory = psutil.Process().memory_info().rss
    a.memory_limit = memory / 0.7 + 400e6

    class BadAccounting:
        def __init__(self, data):
            self.data = data

        def __sizeof__(self):
            return 10

    def f(n):
        x = np.ones(int(n), dtype="u1")
        result = BadAccounting(x)
        return result

    futures = c.map(f, [100e6] * 8, pure=False)

    start = time()
    while not a.data.disk:
        await asyncio.sleep(0.1)
        assert time() < start + 5


@pytest.mark.slow
@pytest.mark.xfail(
    sys.version_info[:2] == (3, 8),
    reason="Sporadic failure on Python 3.8",
    strict=False,
)
@gen_cluster(
    nthreads=[("127.0.0.1", 2)],
    client=True,
    worker_kwargs={
        "memory_monitor_interval": 10,
        "memory_spill_fraction": False,  # don't spill
        "memory_target_fraction": False,
        "memory_pause_fraction": 0.5,
    },
    timeout=20,
)
async def test_pause_executor(c, s, a):
    memory = psutil.Process().memory_info().rss
    a.memory_limit = memory / 0.5 + 200e6
    np = pytest.importorskip("numpy")

    def f():
        x = np.ones(int(400e6), dtype="u1")
        sleep(1)

    with captured_logger(logging.getLogger("distributed.worker")) as logger:
        future = c.submit(f)
        futures = c.map(slowinc, range(30), delay=0.1)

        start = time()
        while not a.paused:
            await asyncio.sleep(0.01)
            assert time() < start + 4, (
                format_bytes(psutil.Process().memory_info().rss),
                format_bytes(a.memory_limit),
                len(a.data),
            )
        out = logger.getvalue()
        assert "memory" in out.lower()
        assert "pausing" in out.lower()

    assert sum(f.status == "finished" for f in futures) < 4

    await wait(futures)


@gen_cluster(client=True, worker_kwargs={"profile_cycle_interval": "50 ms"})
async def test_statistical_profiling_cycle(c, s, a, b):
    futures = c.map(slowinc, range(20), delay=0.05)
    await wait(futures)
    await asyncio.sleep(0.01)
    end = time()
    assert len(a.profile_history) > 3

    x = await a.get_profile(start=time() + 10, stop=time() + 20)
    assert not x["count"]

    x = await a.get_profile(start=0, stop=time() + 10)
    recent = a.profile_recent["count"]
    actual = sum(p["count"] for _, p in a.profile_history) + a.profile_recent["count"]
    x2 = await a.get_profile(start=0, stop=time() + 10)
    assert x["count"] <= actual <= x2["count"]

    y = await a.get_profile(start=end - 0.300, stop=time())
    assert 0 < y["count"] <= x["count"]


@gen_cluster(client=True)
async def test_get_current_task(c, s, a, b):
    def some_name():
        return get_worker().get_current_task()

    result = await c.submit(some_name)
    assert result.startswith("some_name")


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 2)
async def test_reschedule(c, s, a, b):
    s.extensions["stealing"]._pc.stop()
    a_address = a.address

    def f(x):
        sleep(0.1)
        if get_worker().address == a_address:
            raise Reschedule()

    futures = c.map(f, range(4))
    futures2 = c.map(slowinc, range(10), delay=0.1, workers=a.address)
    await wait(futures)

    assert all(f.key in b.data for f in futures)


@pytest.mark.asyncio
async def test_deque_handler(cleanup):
    from distributed.worker import logger

    async with Scheduler() as s:
        async with Worker(s.address) as w:
            deque_handler = w._deque_handler
            logger.info("foo456")
            assert deque_handler.deque
            msg = deque_handler.deque[-1]
            assert "distributed.worker" in deque_handler.format(msg)
            assert any(msg.msg == "foo456" for msg in deque_handler.deque)


@gen_cluster(nthreads=[], client=True)
async def test_avoid_memory_monitor_if_zero_limit(c, s):
    worker = await Worker(
        s.address, loop=s.loop, memory_limit=0, memory_monitor_interval=10
    )
    assert type(worker.data) is dict
    assert "memory" not in worker.periodic_callbacks

    future = c.submit(inc, 1)
    assert (await future) == 2
    await asyncio.sleep(worker.memory_monitor_interval / 1000)

    await c.submit(inc, 2)  # worker doesn't pause

    await worker.close()


@gen_cluster(
    nthreads=[("127.0.0.1", 1)],
    config={
        "distributed.worker.memory.spill": False,
        "distributed.worker.memory.target": False,
    },
)
async def test_dict_data_if_no_spill_to_disk(s, w):
    assert type(w.data) is dict


def test_get_worker_name(client):
    def f():
        get_client().submit(inc, 1).result()

    client.run(f)

    def func(dask_scheduler):
        return list(dask_scheduler.clients)

    start = time()
    while not any("worker" in n for n in client.run_on_scheduler(func)):
        sleep(0.1)
        assert time() < start + 10


@gen_cluster(nthreads=[("127.0.0.1", 1)], worker_kwargs={"memory_limit": "2e3 MB"})
async def test_parse_memory_limit(s, w):
    assert w.memory_limit == 2e9


@gen_cluster(nthreads=[], client=True)
async def test_scheduler_address_config(c, s):
    with dask.config.set({"scheduler-address": s.address}):
        worker = await Worker(loop=s.loop)
        assert worker.scheduler.address == s.address
    await worker.close()


@pytest.mark.slow
@gen_cluster(client=True)
async def test_wait_for_outgoing(c, s, a, b):
    np = pytest.importorskip("numpy")
    x = np.random.random(10000000)
    future = await c.scatter(x, workers=a.address)

    y = c.submit(inc, future, workers=b.address)
    await wait(y)

    assert len(b.incoming_transfer_log) == len(a.outgoing_transfer_log) == 1
    bb = b.incoming_transfer_log[0]["duration"]
    aa = a.outgoing_transfer_log[0]["duration"]
    ratio = aa / bb

    assert 1 / 3 < ratio < 3


@pytest.mark.skipif(
    not sys.platform.startswith("linux"), reason="Need 127.0.0.2 to mean localhost"
)
@gen_cluster(
    nthreads=[("127.0.0.1", 1), ("127.0.0.1", 1), ("127.0.0.2", 1)], client=True
)
async def test_prefer_gather_from_local_address(c, s, w1, w2, w3):
    x = await c.scatter(123, workers=[w1.address, w3.address], broadcast=True)

    y = c.submit(inc, x, workers=[w2.address])
    await wait(y)

    assert any(d["who"] == w2.address for d in w1.outgoing_transfer_log)
    assert not any(d["who"] == w2.address for d in w3.outgoing_transfer_log)


@gen_cluster(
    client=True,
    nthreads=[("127.0.0.1", 1)] * 20,
    timeout=30,
    config={"distributed.worker.connections.incoming": 1},
)
async def test_avoid_oversubscription(c, s, *workers):
    np = pytest.importorskip("numpy")
    x = c.submit(np.random.random, 1000000, workers=[workers[0].address])
    await wait(x)

    futures = [c.submit(len, x, pure=False, workers=[w.address]) for w in workers[1:]]

    await wait(futures)

    # Original worker not responsible for all transfers
    assert len(workers[0].outgoing_transfer_log) < len(workers) - 2

    # Some other workers did some work
    assert len([w for w in workers if len(w.outgoing_transfer_log) > 0]) >= 3


@gen_cluster(client=True, worker_kwargs={"metrics": {"my_port": lambda w: w.port}})
async def test_custom_metrics(c, s, a, b):
    assert s.workers[a.address].metrics["my_port"] == a.port
    assert s.workers[b.address].metrics["my_port"] == b.port


@gen_cluster(client=True)
async def test_register_worker_callbacks(c, s, a, b):
    # preload function to run
    def mystartup(dask_worker):
        dask_worker.init_variable = 1

    def mystartup2():
        import os

        os.environ["MY_ENV_VALUE"] = "WORKER_ENV_VALUE"
        return "Env set."

    # Check that preload function has been run
    def test_import(dask_worker):
        return hasattr(dask_worker, "init_variable")
        #       and dask_worker.init_variable == 1

    def test_startup2():
        import os

        return os.getenv("MY_ENV_VALUE", None) == "WORKER_ENV_VALUE"

    # Nothing has been run yet
    result = await c.run(test_import)
    assert list(result.values()) == [False] * 2
    result = await c.run(test_startup2)
    assert list(result.values()) == [False] * 2

    # Start a worker and check that startup is not run
    worker = await Worker(s.address, loop=s.loop)
    result = await c.run(test_import, workers=[worker.address])
    assert list(result.values()) == [False]
    await worker.close()

    # Add a preload function
    response = await c.register_worker_callbacks(setup=mystartup)
    assert len(response) == 2

    # Check it has been ran on existing worker
    result = await c.run(test_import)
    assert list(result.values()) == [True] * 2

    # Start a worker and check it is ran on it
    worker = await Worker(s.address, loop=s.loop)
    result = await c.run(test_import, workers=[worker.address])
    assert list(result.values()) == [True]
    await worker.close()

    # Register another preload function
    response = await c.register_worker_callbacks(setup=mystartup2)
    assert len(response) == 2

    # Check it has been run
    result = await c.run(test_startup2)
    assert list(result.values()) == [True] * 2

    # Start a worker and check it is ran on it
    worker = await Worker(s.address, loop=s.loop)
    result = await c.run(test_import, workers=[worker.address])
    assert list(result.values()) == [True]
    result = await c.run(test_startup2, workers=[worker.address])
    assert list(result.values()) == [True]
    await worker.close()


@gen_cluster(client=True)
async def test_register_worker_callbacks_err(c, s, a, b):
    with pytest.raises(ZeroDivisionError):
        await c.register_worker_callbacks(setup=lambda: 1 / 0)


@gen_cluster(nthreads=[])
async def test_data_types(s):
    w = await Worker(s.address, data=dict)
    assert isinstance(w.data, dict)
    await w.close()

    data = dict()
    w = await Worker(s.address, data=data)
    assert w.data is data
    await w.close()

    class Data(dict):
        def __init__(self, x, y):
            self.x = x
            self.y = y

    w = await Worker(s.address, data=(Data, {"x": 123, "y": 456}))
    assert w.data.x == 123
    assert w.data.y == 456
    await w.close()


@gen_cluster(nthreads=[])
async def test_local_directory(s):
    with tmpfile() as fn:
        with dask.config.set(temporary_directory=fn):
            w = await Worker(s.address)
            assert w.local_directory.startswith(fn)
            assert "dask-worker-space" in w.local_directory


@pytest.mark.skipif(
    not sys.platform.startswith("linux"), reason="Need 127.0.0.2 to mean localhost"
)
@gen_cluster(nthreads=[], client=True)
async def test_host_address(c, s):
    w = await Worker(s.address, host="127.0.0.2")
    assert "127.0.0.2" in w.address
    await w.close()

    n = await Nanny(s.address, host="127.0.0.3")
    assert "127.0.0.3" in n.address
    assert "127.0.0.3" in n.worker_address
    await n.close()


def test_resource_limit(monkeypatch):
    assert parse_memory_limit("250MiB", 1, total_cores=1) == 1024 * 1024 * 250

    new_limit = 1024 * 1024 * 200
    import distributed.worker

    monkeypatch.setattr(distributed.system, "MEMORY_LIMIT", new_limit)
    assert parse_memory_limit("250MiB", 1, total_cores=1) == new_limit


@pytest.mark.asyncio
@pytest.mark.parametrize("Worker", [Worker, Nanny])
async def test_interface_async(loop, Worker):
    from distributed.utils import get_ip_interface

    psutil = pytest.importorskip("psutil")
    if_names = sorted(psutil.net_if_addrs())
    for if_name in if_names:
        try:
            ipv4_addr = get_ip_interface(if_name)
        except ValueError:
            pass
        else:
            if ipv4_addr == "127.0.0.1":
                break
    else:
        pytest.skip(
            "Could not find loopback interface. "
            "Available interfaces are: %s." % (if_names,)
        )

    async with Scheduler(interface=if_name) as s:
        assert s.address.startswith("tcp://127.0.0.1")
        async with Worker(s.address, interface=if_name) as w:
            assert w.address.startswith("tcp://127.0.0.1")
            assert w.ip == "127.0.0.1"
            async with Client(s.address, asynchronous=True) as c:
                info = c.scheduler_info()
                assert "tcp://127.0.0.1" in info["address"]
                assert all("127.0.0.1" == d["host"] for d in info["workers"].values())


@pytest.mark.asyncio
@pytest.mark.parametrize("Worker", [Worker, Nanny])
async def test_protocol_from_scheduler_address(Worker):
    ucp = pytest.importorskip("ucp")

    async with Scheduler(protocol="ucx") as s:
        assert s.address.startswith("ucx://")
        async with Worker(s.address) as w:
            assert w.address.startswith("ucx://")
            async with Client(s.address, asynchronous=True) as c:
                info = c.scheduler_info()
                assert info["address"].startswith("ucx://")


@pytest.mark.asyncio
@pytest.mark.parametrize("Worker", [Worker, Nanny])
async def test_worker_listens_on_same_interface_by_default(Worker):
    async with Scheduler(host="localhost") as s:
        assert s.ip in {"127.0.0.1", "localhost"}
        async with Worker(s.address) as w:
            assert s.ip == w.ip


@gen_cluster(client=True)
async def test_close_gracefully(c, s, a, b):
    futures = c.map(slowinc, range(200), delay=0.1)
    while not b.data:
        await asyncio.sleep(0.1)

    mem = set(b.data)
    proc = set(b.executing)

    await b.close_gracefully()

    assert b.status == "closed"
    assert b.address not in s.workers
    assert mem.issubset(set(a.data))
    for key in proc:
        assert s.tasks[key].state in ("processing", "memory")


@pytest.mark.slow
@pytest.mark.asyncio
async def test_lifetime(cleanup):
    async with Scheduler() as s:
        async with Worker(s.address) as a, Worker(s.address, lifetime="1 seconds") as b:
            async with Client(s.address, asynchronous=True) as c:
                futures = c.map(slowinc, range(200), delay=0.1)
                await asyncio.sleep(1.5)
                assert b.status != "running"
                await b.finished()

                assert set(b.data).issubset(a.data)  # successfully moved data over


@gen_cluster(client=True, worker_kwargs={"lifetime": "10s", "lifetime_stagger": "2s"})
async def test_lifetime_stagger(c, s, a, b):
    assert a.lifetime != b.lifetime
    assert 8 <= a.lifetime <= 12
    assert 8 <= b.lifetime <= 12


@pytest.mark.asyncio
async def test_bad_metrics(cleanup):
    def bad_metric(w):
        raise Exception("Hello")

    async with Scheduler() as s:
        async with Worker(s.address, metrics={"bad": bad_metric}) as w:
            assert "bad" not in s.workers[w.address].metrics


@pytest.mark.asyncio
async def test_bad_startup(cleanup):
    def bad_startup(w):
        raise Exception("Hello")

    async with Scheduler() as s:
        try:
            w = await Worker(s.address, startup_information={"bad": bad_startup})
        except Exception:
            pytest.fail("Startup exception was raised")


@pytest.mark.asyncio
async def test_update_latency(cleanup):
    async with await Scheduler() as s:
        async with await Worker(s.address) as w:
            original = w.latency
            await w.heartbeat()
            assert original != w.latency

            if w.digests is not None:
                assert w.digests["latency"].size() > 0


@pytest.mark.asyncio
@pytest.mark.parametrize("reconnect", [True, False])
async def test_heartbeat_comm_closed(cleanup, monkeypatch, reconnect):
    with captured_logger("distributed.worker", level=logging.WARNING) as logger:
        async with await Scheduler() as s:

            def bad_heartbeat_worker(*args, **kwargs):
                raise CommClosedError()

            async with await Worker(s.address, reconnect=reconnect) as w:
                # Trigger CommClosedError during worker heartbeat
                monkeypatch.setattr(
                    w.scheduler, "heartbeat_worker", bad_heartbeat_worker
                )

                await w.heartbeat()
                if reconnect:
                    assert w.status == "running"
                else:
                    assert w.status == "closed"
    assert "Heartbeat to scheduler failed" in logger.getvalue()
