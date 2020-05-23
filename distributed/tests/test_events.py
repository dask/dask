import pickle
from datetime import timedelta

from distributed import Event
from distributed.utils_test import gen_cluster
from distributed.utils_test import client, cluster_fixture, loop  # noqa F401


@gen_cluster(client=True, nthreads=[("127.0.0.1", 8)] * 2)
async def test_event_on_workers(c, s, a, b):
    # Test the "typical" use case of events:
    # workers set, clear and wait for it
    def wait_for_it_failing(x):
        event = Event("x")

        # Event is not set in another task so far
        assert not event.wait(timeout=0.05)
        assert not event.is_set()

    def wait_for_it_ok(x):
        event = Event("x")

        # Event is set in another task
        assert event.wait(timeout=0.5)
        assert event.is_set()

    def set_it():
        event = Event("x")
        event.set()

    def clear_it():
        event = Event("x")
        event.clear()

    wait_futures = c.map(wait_for_it_failing, range(10))
    await c.gather(wait_futures)

    set_future = c.submit(set_it)
    await c.gather(set_future)

    wait_futures = c.map(wait_for_it_ok, range(10))
    await c.gather(wait_futures)

    clear_future = c.submit(clear_it)
    await c.gather(clear_future)

    wait_futures = c.map(wait_for_it_ok, range(10))
    await c.gather(wait_futures)

    assert not s.extensions["events"]._events
    assert not s.extensions["events"]._waiter_count


@gen_cluster(client=True)
async def test_default_event(c, s, a, b):
    # The default flag for events should be false
    event = Event("x")
    assert not await event.is_set()

    await event.clear()

    # Cleanup should have happened
    assert not s.extensions["events"]._events
    assert not s.extensions["events"]._waiter_count


@gen_cluster(client=True)
async def test_set_not_set(c, s, a, b):
    # Set and unset the event and check if the flag is
    # propagated correctly
    event = Event("x")

    await event.clear()
    assert not await event.is_set()

    await event.set()
    assert await event.is_set()

    await event.set()
    assert await event.is_set()

    await event.clear()
    assert not await event.is_set()

    # Cleanup should have happened
    assert not s.extensions["events"]._events
    assert not s.extensions["events"]._waiter_count


@gen_cluster(client=True)
async def test_set_not_set_many_events(c, s, a, b):
    # Set and unset the event and check if the flag is
    # propagated correctly with many events
    events = [Event(name) for name in range(100)]

    for event in events:
        await event.clear()
        assert not await event.is_set()

    for i, event in enumerate(events):
        if i % 2 == 0:
            await event.set()
            assert await event.is_set()
        else:
            assert not await event.is_set()

    for event in events:
        await event.clear()
        assert not await event.is_set()

    # Cleanup should have happened
    assert not s.extensions["events"]._events
    assert not s.extensions["events"]._waiter_count


@gen_cluster(client=True)
async def test_timeout(c, s, a, b):
    # The event should not be set and the timeout should happen
    event = Event("x")
    assert not await Event("x").wait(timeout=0.1)

    await event.set()
    assert await Event("x").wait(timeout="100ms")

    await event.clear()
    assert not await Event("x").wait(timeout=timedelta(seconds=0.1))


def test_event_sync(client):
    # Assert that we call the client.sync correctly
    def wait_for_it_failing(x):
        event = Event("x")

        # Event is not set in another task so far
        assert not event.wait(timeout=0.05)
        assert not event.is_set()

    def wait_for_it_ok(x):
        event = Event("x")

        # Event is set in another task
        assert event.wait(timeout=0.5)
        assert event.is_set()

    def set_it():
        event = Event("x")
        event.set()

    wait_futures = client.map(wait_for_it_failing, range(10))
    client.gather(wait_futures)

    set_future = client.submit(set_it)
    client.gather(set_future)

    wait_futures = client.map(wait_for_it_ok, range(10))
    client.gather(wait_futures)


@gen_cluster(client=True)
async def test_event_types(c, s, a, b):
    # Event names could be strings, numbers or tuples
    for name in [1, ("a", 1), ["a", 1], b"123", "123"]:
        event = Event(name)
        assert event.name == name

        await event.set()
        await event.clear()
        result = await event.is_set()
        assert not result

    assert not s.extensions["events"]._events
    assert not s.extensions["events"]._waiter_count


@gen_cluster(client=True)
async def test_serializable(c, s, a, b):
    # Pickling an event should work properly
    def f(x, event=None):
        assert event.name == "x"
        return x + 1

    event = Event("x")
    futures = c.map(f, range(10), event=event)
    await c.gather(futures)

    event2 = pickle.loads(pickle.dumps(event))
    assert event2.name == event.name
    assert event2.client is event.client


@gen_cluster(client=True)
async def test_two_events_on_workers(c, s, a, b):
    # Longer test with multiple events and two workers
    def event_not_set(event_name):
        assert not Event(event_name).wait(timeout=0.05)

    def event_is_set(event_name):
        assert Event(event_name).wait(timeout=0.5)

    await c.gather(c.submit(event_not_set, "first_event"))
    await c.gather(c.submit(event_not_set, "second_event"))

    await Event("first_event").set()

    await c.gather(c.submit(event_is_set, "first_event"))
    await c.gather(c.submit(event_not_set, "second_event"))

    await Event("first_event").clear()
    await Event("second_event").set()

    await c.gather(c.submit(event_not_set, "first_event"))
    await c.gather(c.submit(event_is_set, "second_event"))

    await Event("first_event").clear()
    await Event("second_event").clear()

    await c.gather(c.submit(event_not_set, "first_event"))
    await c.gather(c.submit(event_not_set, "second_event"))

    assert not s.extensions["events"]._events
    assert not s.extensions["events"]._waiter_count
