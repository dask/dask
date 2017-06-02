from __future__ import print_function, division, absolute_import

from distributed import Worker
from distributed.utils_test import inc, gen_cluster
from distributed.diagnostics.plugin import SchedulerPlugin


@gen_cluster(client=True)
def test_simple(c, s, a, b):

    class Counter(SchedulerPlugin):
        def start(self, scheduler):
            self.scheduler = scheduler
            scheduler.add_plugin(self)
            self.count = 0

        def transition(self, key, start, finish, *args, **kwargs):
            if start == 'processing' and finish == 'memory':
                self.count += 1

    counter = Counter()
    counter.start(s)
    assert counter in s.plugins

    assert counter.count == 0

    x = c.submit(inc, 1)
    y = c.submit(inc, x)
    z = c.submit(inc, y)

    yield z

    assert counter.count == 3
    s.remove_plugin(counter)
    assert counter not in s.plugins


@gen_cluster(ncores=[], client=False)
def test_add_remove_worker(s):
    events = []

    class MyPlugin(SchedulerPlugin):
        def add_worker(self, worker, scheduler):
            assert scheduler is s
            events.append(('add_worker', worker))

        def remove_worker(self, worker, scheduler):
            assert scheduler is s
            events.append(('remove_worker', worker))

    plugin = MyPlugin()
    s.add_plugin(plugin)
    assert events == []

    a = Worker(s.address)
    b = Worker(s.address)
    yield a._start()
    yield b._start()
    yield a._close()
    yield b._close()

    assert events == [('add_worker', a.address),
                      ('add_worker', b.address),
                      ('remove_worker', a.address),
                      ('remove_worker', b.address),
                      ]

    events[:] = []
    s.remove_plugin(plugin)
    a = Worker(s.address)
    yield a._start()
    yield a._close()
    assert events == []
