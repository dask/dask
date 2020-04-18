import pytest

from distributed import Worker, WorkerPlugin
from distributed.utils_test import gen_cluster


class MyPlugin(WorkerPlugin):
    name = "MyPlugin"

    def __init__(self, data, expected_transitions=None):
        self.data = data
        self.expected_transitions = expected_transitions

    def setup(self, worker):
        assert isinstance(worker, Worker)
        self.worker = worker
        self.worker._my_plugin_status = "setup"
        self.worker._my_plugin_data = self.data

        self.observed_transitions = []

    def teardown(self, worker):
        self.worker._my_plugin_status = "teardown"

        if self.expected_transitions is not None:
            assert len(self.observed_transitions) == len(self.expected_transitions)
            for expected, real in zip(
                self.expected_transitions, self.observed_transitions
            ):
                assert expected == real

    def transition(self, key, start, finish, **kwargs):
        self.observed_transitions.append((key, start, finish))


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
    expected_transitions = [
        ("task", "waiting", "ready"),
        ("task", "ready", "executing"),
        ("task", "executing", "memory"),
    ]

    plugin = MyPlugin(1, expected_transitions=expected_transitions)

    await c.register_worker_plugin(plugin)
    await c.submit(lambda x: x, 1, key="task")


@gen_cluster(nthreads=[("127.0.0.1", 1)], client=True)
async def test_failing_task_transitions_called(c, s, w):
    def failing(x):
        raise Exception()

    expected_transitions = [
        ("task", "waiting", "ready"),
        ("task", "ready", "executing"),
        ("task", "executing", "error"),
    ]

    plugin = MyPlugin(1, expected_transitions=expected_transitions)

    await c.register_worker_plugin(plugin)

    with pytest.raises(Exception):
        await c.submit(failing, 1, key="task")


@gen_cluster(nthreads=[("127.0.0.1", 1)], client=True)
async def test_empty_plugin(c, s, w):
    class EmptyPlugin:
        pass

    await c.register_worker_plugin(EmptyPlugin())
