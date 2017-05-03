from __future__ import print_function, division, absolute_import

from operator import add
import random
import time

from concurrent.futures import (
    CancelledError, TimeoutError, Future, wait, as_completed,
    FIRST_COMPLETED, FIRST_EXCEPTION, ALL_COMPLETED)

import pytest
from toolz import take
from tornado import gen

from distributed.client import Client
from distributed.scheduler import Scheduler, KilledWorker
from distributed.sizeof import sizeof
from distributed.utils_test import (slow, slowinc, slowadd, slowdec,
        loop, inc, dec, div, throws, gen_cluster, cluster)


def number_of_processing_tasks(client):
    return sum(len(v) for k, v in client.processing().items())


def test_submit(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            with c.get_executor() as e:
                f1 = e.submit(slowadd, 1, 2)
                assert isinstance(f1, Future)
                f2 = e.submit(slowadd, 3, y=4)
                f3 = e.submit(throws, "foo")
                f4 = e.submit(slowadd, x=5, y=6)
                assert f1.result() == 3
                assert f2.result() == 7
                with pytest.raises(RuntimeError):
                    f3.result()
                assert f4.result() == 11


def test_as_completed(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            with c.get_executor() as e:
                N = 10
                fs = [e.submit(slowinc, i, delay=0.02) for i in range(N)]
                expected = set(range(1, N + 1))

                for f in as_completed(fs):
                    res = f.result()
                    assert res in expected
                    expected.remove(res)

                assert not expected


def test_wait(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            with c.get_executor(pure=False) as e:
                N = 10
                fs = [e.submit(slowinc, i, delay=0.05) for i in range(N)]
                res = wait(fs, timeout=0.01)
                assert len(res.not_done) > 0
                res = wait(fs)
                assert len(res.not_done) == 0
                assert res.done == set(fs)

                fs = [e.submit(slowinc, i, delay=0.05) for i in range(N)]
                res = wait(fs, return_when=FIRST_COMPLETED)
                assert len(res.not_done) > 0
                assert len(res.done) >= 1
                res = wait(fs)
                assert len(res.not_done) == 0
                assert res.done == set(fs)

                fs = [e.submit(slowinc, i, delay=0.05) for i in range(N)]
                fs += [e.submit(throws, None)]
                fs += [e.submit(slowdec, i, delay=0.05) for i in range(N)]
                res = wait(fs, return_when=FIRST_EXCEPTION)
                assert len(res.not_done) > 0
                assert len(res.done) >= N - 2  # likely, unless tasks get reordered

                errors = []
                for fs in res.done:
                    try:
                        fs.result()
                    except RuntimeError as e:
                        errors.append(e)

                assert len(errors) == 1
                assert "hello" in str(errors[0])


def test_cancellation(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            with c.get_executor(pure=False) as e:
                fut = e.submit(time.sleep, 2.0)
                start = time.time()
                while number_of_processing_tasks(c) == 0:
                    assert time.time() < start + 1
                    time.sleep(0.01)
                assert not fut.done()

                fut.cancel()
                assert fut.cancelled()
                start = time.time()
                while number_of_processing_tasks(c) != 0:
                    assert time.time() < start + 1
                    time.sleep(0.01)

                with pytest.raises(CancelledError):
                    fut.result()

            # With wait()
            with c.get_executor(pure=False) as e:
                N = 10
                fs = [e.submit(slowinc, i, delay=0.02) for i in range(N)]
                fs[3].cancel()
                res = wait(fs, return_when=FIRST_COMPLETED)
                assert len(res.not_done) > 0
                assert len(res.done) >= 1

                assert fs[3] in res.done
                assert fs[3].cancelled()

            # With as_completed()
            with c.get_executor(pure=False) as e:
                N = 10
                fs = [e.submit(slowinc, i, delay=0.02) for i in range(N)]
                fs[3].cancel()
                fs[8].cancel()

                n_cancelled = sum(f.cancelled() for f in as_completed(fs))
                assert n_cancelled == 2


def test_map(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            with c.get_executor() as e:
                N = 10
                it = e.map(inc, range(N))
                expected = set(range(1, N + 1))
                for x in it:
                    expected.remove(x)
                assert not expected

            with c.get_executor(pure=False) as e:
                N = 10
                it = e.map(slowinc, range(N), [0.1] * N, timeout=0.4)
                results = []
                with pytest.raises(TimeoutError):
                    for x in it:
                        results.append(x)
                assert 2 <= len(results) < 7

            with c.get_executor(pure=False) as e:
                N = 10
                # Not consuming the iterator will cancel remaining tasks
                it = e.map(slowinc, range(N), [0.1] * N)
                for x in take(2, it):
                    pass
                # Some tasks still processing
                assert number_of_processing_tasks(c) > 0
                # Garbage collect the iterator => remaining tasks are cancelled
                del it
                assert number_of_processing_tasks(c) == 0


def get_random():
    return random.random()


def test_pure(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            N = 10
            with c.get_executor() as e:
                fs = [e.submit(get_random) for i in range(N)]
                res = [fut.result() for fut in as_completed(fs)]
                assert len(set(res)) < len(res)
            with c.get_executor(pure=False) as e:
                fs = [e.submit(get_random) for i in range(N)]
                res = [fut.result() for fut in as_completed(fs)]
                assert len(set(res)) == len(res)


def test_workers(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            N = 10
            with c.get_executor(workers=[b['address']]) as e:
                fs = [e.submit(slowinc, i) for i in range(N)]
                wait(fs)
                has_what = c.has_what()
                assert not has_what.get(a['address'])
                assert len(has_what[b['address']]) == N


def test_unsupported_arguments(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            with pytest.raises(TypeError) as excinfo:
                c.get_executor(workers=[b['address']], foo=1, bar=2)
            assert ("unsupported arguments to ClientExecutor: ['bar', 'foo']"
                    in str(excinfo.value))


def test_shutdown(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            # shutdown(wait=True) waits for pending tasks to finish
            e = c.get_executor()
            fut = e.submit(time.sleep, 1.0)
            t1 = time.time()
            e.shutdown()
            dt = time.time() - t1
            assert 0.5 <= dt <= 2.0
            time.sleep(0.1)  # wait for future outcome to propagate
            assert fut.done()
            fut.result()  # doesn't raise

            with pytest.raises(RuntimeError):
                e.submit(time.sleep, 1.0)

            # shutdown(wait=False) cancels pending tasks
            e = c.get_executor()
            fut = e.submit(time.sleep, 2.0)
            t1 = time.time()
            e.shutdown(wait=False)
            dt = time.time() - t1
            assert dt < 0.3
            time.sleep(0.1)  # wait for future outcome to propagate
            assert fut.cancelled()

            with pytest.raises(RuntimeError):
                e.submit(time.sleep, 1.0)
