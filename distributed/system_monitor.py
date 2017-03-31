from __future__ import print_function, division, absolute_import

from collections import deque
import psutil

from .compatibility import WINDOWS
from .metrics import time


class SystemMonitor(object):
    def __init__(self, n=10000):
        self.proc = psutil.Process()

        self.time = deque(maxlen=n)
        self.cpu = deque(maxlen=n)
        self.memory = deque(maxlen=n)
        self.read_bytes = deque(maxlen=n)
        self.write_bytes = deque(maxlen=n)

        self.last_time = time()
        self.count = 0

        self._last_io_counters = psutil.net_io_counters()
        self.quantities = {'cpu': self.cpu,
                           'memory': self.memory,
                           'time': self.time,
                           'read_bytes': self.read_bytes,
                           'write_bytes': self.write_bytes}
        if not WINDOWS:
            self.num_fds = deque(maxlen=n)
            self.quantities['num_fds'] = self.num_fds

    def update(self):
        cpu = self.proc.cpu_percent()
        memory = self.proc.memory_info().rss

        now = time()
        ioc = psutil.net_io_counters()
        last = self._last_io_counters
        duration = now - self.last_time
        read_bytes = (ioc.bytes_recv - last.bytes_recv) / (duration or 0.5)
        write_bytes = (ioc.bytes_sent - last.bytes_sent) / (duration or 0.5)
        self._last_io_counters = ioc
        self.last_time = now

        self.cpu.append(cpu)
        self.memory.append(memory)
        self.time.append(now)

        self.read_bytes.append(read_bytes)
        self.write_bytes.append(write_bytes)

        self.count += 1

        result = {'cpu': cpu, 'memory': memory, 'time': now,
                  'count': self.count, 'read_bytes': read_bytes,
                  'write_bytes': write_bytes}

        if not WINDOWS:
            num_fds = self.proc.num_fds()
            self.num_fds.append(num_fds)
            result['num_fds'] = num_fds

        return result

    def __str__(self):
        return '<SystemMonitor: cpu: %d memory: %d MB fds: %d>' % (
                self.cpu[-1], self.memory[-1] / 1e6,
                -1 if WINDOWS else self.num_fds[-1])

    __repr__ = __str__

    def range_query(self, start):
        if start == self.count:
            return {k: [] for k in self.quantities}

        istart = start - (self.count - len(self.cpu))
        istart = max(0, istart)

        seq = [i for i in range(istart, len(self.cpu))]

        d = {k: [v[i] for i in seq] for k, v in self.quantities.items()}
        return d
