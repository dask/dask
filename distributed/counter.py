from __future__ import print_function, division, absolute_import

from collections import defaultdict

from tornado.ioloop import IOLoop

from .utils import PeriodicCallback


try:
    from crick import TDigest
except ImportError:
    pass
else:
    class Digest(object):
        def __init__(self, loop=None, intervals=(5, 60, 3600)):
            self.intervals = intervals
            self.components = [TDigest() for i in self.intervals]

            self.loop = loop or IOLoop.current()
            self._pc = PeriodicCallback(self.shift, self.intervals[0] * 1000)
            self.loop.add_callback(self._pc.start)

        def add(self, item):
            self.components[0].add(item)

        def update(self, seq):
            self.components[0].update(seq)

        def shift(self):
            for i in range(len(self.intervals) - 1):
                frac = 0.2 * self.intervals[0] / self.intervals[i]
                part = self.components[i].scale(frac)
                rest = self.components[i].scale(1 - frac)

                self.components[i + 1].merge(part)
                self.components[i] = rest

        def size(self):
            return sum(d.size() for d in self.components)


class Counter(object):
    def __init__(self, loop=None, intervals=(5, 60, 3600)):
        self.intervals = intervals
        self.components = [defaultdict(lambda: 0) for i in self.intervals]

        self.loop = loop or IOLoop.current()
        self._pc = PeriodicCallback(self.shift, self.intervals[0] * 1000)
        self.loop.add_callback(self._pc.start)

    def add(self, item):
        self.components[0][item] += 1

    def shift(self):
        for i in range(len(self.intervals) - 1):
            frac = 0.2 * self.intervals[0] / self.intervals[i]
            part = {k: v * frac for k, v in self.components[i].items()}
            rest = {k: v * (1 - frac) for k, v in self.components[i].items()}

            for k, v in part.items():
                self.components[i + 1][k] += v
            d = defaultdict(lambda: 0)
            d.update(rest)
            self.components[i] = d

    def size(self):
        return sum(sum(d.values()) for d in self.components)
