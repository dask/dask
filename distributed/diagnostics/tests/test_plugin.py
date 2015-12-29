from tornado import gen
from tornado.queues import Queue

from distributed.scheduler import Scheduler
from distributed.utils_test import _test_cluster, loop, inc
from distributed.diagnostics.plugin import SchedulerPlugin


def test_diagnostic(loop):
    @gen.coroutine
    def f(c, a, b):
        s = Scheduler((c.ip, c.port), loop=loop)
        yield s.sync_center()
        done = s.start()
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
               'dsk': {'x': (inc, 1),
                       'y': (inc, 'x'),
                       'z': (inc, 'y')},
               'keys': ['z']})

        while True:
            msg = yield report.get()
            if msg['op'] == 'key-in-memory' and msg['key'] == 'z':
                break

        assert counter.count == 3

        sched.put_nowait({'op': 'close'})
        yield done

    _test_cluster(f, loop)

