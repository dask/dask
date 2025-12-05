from __future__ import annotations

from collections import defaultdict
from collections.abc import Hashable, Iterable, Sequence

try:
    from crick import TDigest
except ImportError:
    pass
else:

    class Digest:
        intervals: Sequence[float]
        components: list[TDigest]
        __slots__ = ("intervals", "components")

        def __init__(self, intervals: Sequence[float] = (5, 60, 3600)):
            self.intervals = intervals
            self.components = [TDigest() for _ in intervals]

        def add(self, item: float) -> None:
            self.components[0].add(item)

        def update(self, seq: Iterable[float]) -> None:
            self.components[0].update(seq)

        def shift(self) -> None:
            for i in range(len(self.intervals) - 1):
                frac = 0.2 * self.intervals[0] / self.intervals[i]
                part = self.components[i].scale(frac)
                rest = self.components[i].scale(1 - frac)

                self.components[i + 1].merge(part)
                self.components[i] = rest

        def size(self) -> float:
            return sum(d.size() for d in self.components)


class Counter:
    intervals: Sequence[float]
    components: list[defaultdict[Hashable, float]]
    __slots__ = ("intervals", "components")

    def __init__(self, intervals: Sequence[float] = (5, 60, 3600)):
        self.intervals = intervals
        self.components = [defaultdict(int) for _ in intervals]

    def add(self, item: Hashable) -> None:
        self.components[0][item] += 1

    def shift(self) -> None:
        for i in range(len(self.intervals) - 1):
            frac = 0.2 * self.intervals[0] / self.intervals[i]
            part = {k: v * frac for k, v in self.components[i].items()}
            rest = {k: v * (1 - frac) for k, v in self.components[i].items()}

            for k, v in part.items():
                self.components[i + 1][k] += v
            d: defaultdict[Hashable, float] = defaultdict(int)
            d.update(rest)
            self.components[i] = d

    def size(self) -> float:
        return sum(sum(d.values()) for d in self.components)
