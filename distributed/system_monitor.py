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
        self.count = 0

        self.quantities = {'cpu': self.cpu,
                           'memory': self.memory,
                           'time': self.time}

        try:
            ioc = psutil.net_io_counters()
        except Exception:
            self._collect_net_io_counters = False
        else:
            self.last_time = time()
            self.read_bytes = deque(maxlen=n)
            self.write_bytes = deque(maxlen=n)
            self.quantities['read_bytes'] = self.read_bytes
            self.quantities['write_bytes'] = self.write_bytes
            self._last_io_counters = ioc
            self._collect_net_io_counters = True

        if not WINDOWS:
            self.num_fds = deque(maxlen=n)
            self.quantities['num_fds'] = self.num_fds

        self.update()

    def recent(self):
        try:
            return {k: v[-1] for k, v in self.quantities.items()}
        except IndexError:
            return {k: None for k, v in self.quantities.items()}

    def update(self):
        cpu = self.proc.cpu_percent()
        memory = self.proc.memory_info().rss
        now = time()

        self.cpu.append(cpu)
        self.memory.append(memory)
        self.time.append(now)
        self.count += 1

        result = {'cpu': cpu,
                  'memory': memory,
                  'time': now,
                  'count': self.count}

        if self._collect_net_io_counters:
            try:
                ioc = psutil.net_io_counters()
            except Exception:
                pass
            else:
                last = self._last_io_counters
                duration = now - self.last_time
                read_bytes = (ioc.bytes_recv - last.bytes_recv) / (duration or 0.5)
                write_bytes = (ioc.bytes_sent - last.bytes_sent) / (duration or 0.5)
                self.last_time = now
                self._last_io_counters = ioc
                self.read_bytes.append(read_bytes)
                self.write_bytes.append(write_bytes)
                result['read_bytes'] = read_bytes
                result['write_bytes'] = write_bytes

        if not WINDOWS:
            num_fds = self.proc.num_fds()
            self.num_fds.append(num_fds)
            result['num_fds'] = num_fds

        return result

    def __repr__(self):
        return '<SystemMonitor: cpu: %d memory: %d MB fds: %d>' % (
            self.cpu[-1], self.memory[-1] / 1e6,
            -1 if WINDOWS else self.num_fds[-1])

    def range_query(self, start):
        if start == self.count:
            return {k: [] for k in self.quantities}

        istart = start - (self.count - len(self.cpu))
        istart = max(0, istart)

        seq = [i for i in range(istart, len(self.cpu))]

        d = {k: [v[i] for i in seq] for k, v in self.quantities.items()}
        return d
