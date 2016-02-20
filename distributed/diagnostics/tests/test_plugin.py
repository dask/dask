from tornado import gen
from tornado.queues import Queue

from distributed.scheduler import dumps_task
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

        def task_finished(self, scheduler, key, worker, nbytes):
            self.count += 1

    counter = Counter()
    counter.start(s)

    assert counter.count == 0
    sched.put_nowait({'op': 'update-graph',
                      'tasks': {b'x': dumps_task((inc, 1)),
                                b'y': dumps_task((inc, b'x')),
                                b'z': dumps_task((inc, b'y'))},
                      'dependencies': {b'y': [b'x'], b'z': [b'y']},
                      'keys': [b'z']})

    while True:
        msg = yield report.get()
        if msg['op'] == 'key-in-memory' and msg['key'] == b'z':
            break

    assert counter.count == 3
