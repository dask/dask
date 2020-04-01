import pickle

import dask
from dask.distributed import Client

from distributed import Semaphore
from distributed.metrics import time
from distributed.utils_test import cluster, gen_cluster
from distributed.utils_test import client, loop, cluster_fixture  # noqa: F401
import pytest


@gen_cluster(client=True)
async def test_semaphore(c, s, a, b):
    semaphore = await Semaphore(max_leases=2, name="resource_we_want_to_limit")

    result = await semaphore.acquire()  # allowed_leases: 2 - 1 -> 1
    assert result is True

    second = await semaphore.acquire()  # allowed_leases: 1 - 1 -> 0
    assert second is True
    start = time()
    result = await semaphore.acquire(timeout=0.025)  # allowed_leases: 0 -> False
    stop = time()
    assert stop - start < 0.2
    assert result is False


@gen_cluster(client=True)
async def test_serializable(c, s, a, b):
    sem = await Semaphore(max_leases=2, name="x")
    res = await sem.acquire()
    assert len(s.extensions["semaphores"].leases["x"]) == 1
    assert res
    sem2 = pickle.loads(pickle.dumps(sem))
    assert sem2.name == sem.name
    assert sem2.client.scheduler.address == sem.client.scheduler.address

    # actual leases didn't change
    assert len(s.extensions["semaphores"].leases["x"]) == 1

    res = await sem2.acquire()
    assert res
    assert len(s.extensions["semaphores"].leases["x"]) == 2

    # Ensure that both objects access the same semaphore
    res = await sem.acquire(timeout=0.025)

    assert not res
    res = await sem2.acquire(timeout=0.025)

    assert not res


@gen_cluster(client=True)
async def test_release_simple(c, s, a, b):
    def f(x, semaphore):
        with semaphore:
            assert semaphore.name == "x"
            return x + 1

    sem = await Semaphore(max_leases=2, name="x")
    futures = c.map(f, list(range(10)), semaphore=sem)
    await c.gather(futures)


@gen_cluster(client=True)
async def test_acquires_with_timeout(c, s, a, b):
    sem = await Semaphore(1, "x")
    assert await sem.acquire(timeout=0.025)
    assert not await sem.acquire(timeout=0.025)
    await sem.release()
    assert await sem.acquire(timeout=0.025)
    await sem.release()


def test_timeout_sync(client):
    s = Semaphore(name="x")
    # Using the context manager already acquires a lease, so the line below won't be able to acquire another one
    with s:
        assert s.acquire(timeout=0.025) is False


@pytest.mark.slow
@gen_cluster(client=True, timeout=20)
async def test_release_semaphore_after_timeout(c, s, a, b):
    with dask.config.set(
        {"distributed.scheduler.locks.lease-validation-interval": "50ms"}
    ):
        sem = await Semaphore(name="x", max_leases=2)
        await sem.acquire()  # leases: 2 - 1 = 1
        semY = await Semaphore(name="y")

        async with Client(s.address, asynchronous=True, name="ClientB") as clientB:
            semB = await Semaphore(name="x", max_leases=2, client=clientB)
            semYB = await Semaphore(name="y", client=clientB)

            assert await semB.acquire()  # leases: 1 - 1 = 0
            assert await semYB.acquire()

            assert not (await sem.acquire(timeout=0.01))
            assert not (await semB.acquire(timeout=0.01))
            assert not (await semYB.acquire(timeout=0.01))

        # `ClientB` goes out of scope, leases should be released
        # At this point, we should be able to acquire x and y once
        assert await sem.acquire()
        assert await semY.acquire()

        assert not (await semY.acquire(timeout=0.01))
        assert not (await sem.acquire(timeout=0.01))

        assert clientB.id not in s.extensions["semaphores"].leases_per_client


@gen_cluster()
async def test_async_ctx(s, a, b):
    sem = await Semaphore(name="x")
    async with sem:
        assert not await sem.acquire(timeout=0.025)
    assert await sem.acquire()


@pytest.mark.slow
def test_worker_dies():
    with cluster(disconnect_timeout=10) as (scheduler, workers):
        with Client(scheduler["address"]) as client:
            sem = Semaphore(name="x", max_leases=1)

            def f(x, sem, kill_address):
                with sem:
                    from distributed.worker import get_worker

                    worker = get_worker()
                    if worker.address == kill_address:
                        import os

                        os.kill(os.getpid(), 15)
                    return x

            futures = client.map(
                f, range(100), sem=sem, kill_address=workers[0]["address"]
            )
            results = client.gather(futures)

            assert sorted(results) == list(range(100))


@gen_cluster(client=True)
async def test_access_semaphore_by_name(c, s, a, b):
    def f(x, release=True):
        sem = Semaphore(name="x")
        if not sem.acquire(timeout=0.1):
            return False
        if release:
            sem.release()

        return True

    sem = await Semaphore(name="x")
    futures = c.map(f, list(range(10)))
    assert all(await c.gather(futures))

    # Clean-up the state, otherwise we would get the same result when calling `f` with the same arguments
    del futures

    assert len(s.extensions["semaphores"].leases["x"]) == 0
    assert await sem.acquire()
    assert len(s.extensions["semaphores"].leases["x"]) == 1
    futures = c.map(f, list(range(10)))
    assert not any(await c.gather(futures))
    await sem.release()

    del futures

    futures = c.map(f, list(range(10)), release=False)
    result = await c.gather(futures)
    assert result.count(True) == 1
    assert result.count(False) == 9


@gen_cluster(client=True)
async def test_close_async(c, s, a, b):
    sem = await Semaphore(name="test")

    assert await sem.acquire()
    with pytest.warns(
        RuntimeWarning, match="Closing semaphore .* but client .* still has a lease"
    ):
        await sem.close()

    with pytest.raises(
        RuntimeError, match="Semaphore `test` not known or already closed."
    ):
        await sem.acquire()

    semaphore_object = s.extensions["semaphores"]
    assert not semaphore_object.max_leases
    assert not semaphore_object.leases
    assert not semaphore_object.events
    assert not any(semaphore_object.leases_per_client.values())


def test_close_sync(client):
    sem = Semaphore()
    sem.close()

    with pytest.raises(RuntimeError, match="Semaphore .* not known or already closed."):
        sem.acquire()


@gen_cluster(client=True)
async def test_release_once_too_many(c, s, a, b):
    sem = await Semaphore(name="x")
    assert await sem.acquire()
    await sem.release()

    with pytest.raises(
        ValueError, match="Tried to release semaphore but it was already released"
    ):
        await sem.release()

    assert await sem.acquire()
    await sem.release()


@gen_cluster(client=True)
async def test_release_once_too_many_resilience(c, s, a, b):
    def f(x, sem):
        sem.acquire()
        sem.release()
        with pytest.raises(
            ValueError, match="Tried to release semaphore but it was already released"
        ):
            sem.release()
        return x

    sem = await Semaphore(max_leases=3, name="x")

    inpt = list(range(20))
    futures = c.map(f, inpt, sem=sem)
    assert sorted(await c.gather(futures)) == inpt

    assert not s.extensions["semaphores"].leases["x"]
    await sem.acquire()
    assert len(s.extensions["semaphores"].leases["x"]) == 1
