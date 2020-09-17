import pytest

from distributed import Worker, WorkerPlugin
from distributed.utils_test import async_wait_for, gen_cluster, inc


class MyPlugin(WorkerPlugin):
    name = "MyPlugin"

    def __init__(self, data, expected_notifications=None):
        self.data = data
        self.expected_notifications = expected_notifications

    def setup(self, worker):
        assert isinstance(worker, Worker)
        self.worker = worker
        self.worker._my_plugin_status = "setup"
        self.worker._my_plugin_data = self.data

        self.observed_notifications = []

    def teardown(self, worker):
        self.worker._my_plugin_status = "teardown"

        if self.expected_notifications is not None:
            assert len(self.observed_notifications) == len(self.expected_notifications)
            for expected, real in zip(
                self.expected_notifications, self.observed_notifications
            ):
                assert expected == real

    def transition(self, key, start, finish, **kwargs):
        self.observed_notifications.append(
            {"key": key, "start": start, "finish": finish}
        )

    def release_key(self, key, state, cause, reason, report):
        self.observed_notifications.append({"key": key, "state": state})

    def release_dep(self, dep, state, report):
        self.observed_notifications.append({"dep": dep, "state": state})


@gen_cluster(client=True, nthreads=[])
async def test_create_with_client(c, s):
    await c.register_worker_plugin(MyPlugin(123))

    worker = await Worker(s.address, loop=s.loop)
    assert worker._my_plugin_status == "setup"
    assert worker._my_plugin_data == 123

    await worker.close()
    assert worker._my_plugin_status == "teardown"


@gen_cluster(client=True, worker_kwargs={"plugins": [MyPlugin(5)]})
async def test_create_on_construction(c, s, a, b):
    assert len(a.plugins) == len(b.plugins) == 1
    assert a._my_plugin_status == "setup"
    assert a._my_plugin_data == 5


@gen_cluster(nthreads=[("127.0.0.1", 1)], client=True)
async def test_normal_task_transitions_called(c, s, w):
    expected_notifications = [
        {"key": "task", "start": "waiting", "finish": "ready"},
        {"key": "task", "start": "ready", "finish": "executing"},
        {"key": "task", "start": "executing", "finish": "memory"},
        {"key": "task", "state": "memory"},
    ]

    plugin = MyPlugin(1, expected_notifications=expected_notifications)

    await c.register_worker_plugin(plugin)
    await c.submit(lambda x: x, 1, key="task")
    await async_wait_for(lambda: not w.task_state, timeout=10)


@gen_cluster(nthreads=[("127.0.0.1", 1)], client=True)
async def test_failing_task_transitions_called(c, s, w):
    def failing(x):
        raise Exception()

    expected_notifications = [
        {"key": "task", "start": "waiting", "finish": "ready"},
        {"key": "task", "start": "ready", "finish": "executing"},
        {"key": "task", "start": "executing", "finish": "error"},
    ]

    plugin = MyPlugin(1, expected_notifications=expected_notifications)

    await c.register_worker_plugin(plugin)

    with pytest.raises(Exception):
        await c.submit(failing, 1, key="task")


@gen_cluster(
    nthreads=[("127.0.0.1", 1)], client=True, worker_kwargs={"resources": {"X": 1}}
)
async def test_superseding_task_transitions_called(c, s, w):
    expected_notifications = [
        {"key": "task", "start": "waiting", "finish": "constrained"},
        {"key": "task", "start": "constrained", "finish": "executing"},
        {"key": "task", "start": "executing", "finish": "memory"},
        {"key": "task", "state": "memory"},
    ]

    plugin = MyPlugin(1, expected_notifications=expected_notifications)

    await c.register_worker_plugin(plugin)
    await c.submit(lambda x: x, 1, key="task", resources={"X": 1})
    await async_wait_for(lambda: not w.task_state, timeout=10)


@gen_cluster(nthreads=[("127.0.0.1", 1)], client=True)
async def test_release_dep_called(c, s, w):
    dsk = {"dep": 1, "task": (inc, "dep")}

    expected_notifications = [
        {"key": "dep", "start": "waiting", "finish": "ready"},
        {"key": "dep", "start": "ready", "finish": "executing"},
        {"key": "dep", "start": "executing", "finish": "memory"},
        {"key": "task", "start": "waiting", "finish": "ready"},
        {"key": "task", "start": "ready", "finish": "executing"},
        {"key": "task", "start": "executing", "finish": "memory"},
        {"key": "dep", "state": "memory"},
        {"dep": "dep", "state": "memory"},
        {"key": "task", "state": "memory"},
    ]

    plugin = MyPlugin(1, expected_notifications=expected_notifications)

    await c.register_worker_plugin(plugin)
    await c.get(dsk, "task", sync=False)
    await async_wait_for(lambda: not (w.task_state or w.dep_state), timeout=10)


@gen_cluster(nthreads=[("127.0.0.1", 1)], client=True)
async def test_registering_with_name_arg(c, s, w):
    class FooWorkerPlugin:
        def setup(self, worker):
            if hasattr(worker, "foo"):
                raise RuntimeError(f"Worker {worker.address} already has foo!")

            worker.foo = True

    responses = await c.register_worker_plugin(FooWorkerPlugin(), name="foo")
    assert list(responses.values()) == [{"status": "OK"}]

    async with Worker(s.address, loop=s.loop):
        responses = await c.register_worker_plugin(FooWorkerPlugin(), name="foo")
        assert list(responses.values()) == [{"status": "repeat"}] * 2


@gen_cluster(nthreads=[("127.0.0.1", 1)], client=True)
async def test_empty_plugin(c, s, w):
    class EmptyPlugin:
        pass

    await c.register_worker_plugin(EmptyPlugin())
