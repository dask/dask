from __future__ import print_function, division, absolute_import

from distributed.utils_test import inc, gen_cluster
from distributed.diagnostics.plugin import SchedulerPlugin


@gen_cluster(executor=True)
def test_diagnostic(e, s, a, b):
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

    x = e.submit(inc, 1)
    y = e.submit(inc, x)
    z = e.submit(inc, y)

    yield z._result()

    assert counter.count == 3
    s.remove_plugin(counter)
    assert counter not in s.plugins
