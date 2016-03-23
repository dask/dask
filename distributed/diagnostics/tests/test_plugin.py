from __future__ import print_function, division, absolute_import

from tornado import gen
from tornado.queues import Queue

from distributed.worker import dumps_task
from distributed.utils_test import inc, gen_cluster
from distributed.diagnostics.plugin import SchedulerPlugin


@gen_cluster()
def test_diagnostic(s, a, b):
    sched, report = Queue(), Queue(); s.handle_queues(sched, report)
    msg = yield report.get(); assert msg['op'] == 'stream-start'

    class Counter(SchedulerPlugin):
        def start(self, scheduler):
            scheduler.add_plugin(self)
            self.count = 0

        def task_finished(self, scheduler, key, worker, nbytes, **kwargs):
            self.count += 1

    counter = Counter()
    counter.start(s)
    assert counter in s.plugins

    assert counter.count == 0
    sched.put_nowait({'op': 'update-graph',
                      'tasks': {'x': dumps_task((inc, 1)),
                                'y': dumps_task((inc, 'x')),
                                'z': dumps_task((inc, 'y'))},
                      'dependencies': {'y': ['x'], 'z': ['y']},
                      'keys': ['z']})

    while True:
        msg = yield report.get()
        if msg['op'] == 'key-in-memory' and msg['key'] == 'z':
            break

    assert counter.count == 3
    s.remove_plugin(counter)
    assert counter not in s.plugins
