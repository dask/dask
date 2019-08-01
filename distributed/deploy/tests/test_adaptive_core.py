import asyncio
import pytest

from distributed.deploy.adaptive_core import AdaptiveCore
from distributed.metrics import time


class MyAdaptive(AdaptiveCore):
    def __init__(self, *args, interval=None, **kwargs):
        super().__init__(*args, interval=interval, **kwargs)
        self._target = 0
        self._log = []

    async def target(self):
        return self._target

    async def scale_up(self, n=0):
        self.plan = self.requested = set(range(n))

    async def scale_down(self, workers=()):
        for collection in [self.plan, self.requested, self.observed]:
            for w in workers:
                collection.discard(w)


@pytest.mark.asyncio
async def test_safe_target():
    adapt = MyAdaptive(minimum=1, maximum=4)
    assert await adapt.safe_target() == 1
    adapt._target = 10
    assert await adapt.safe_target() == 4


@pytest.mark.asyncio
async def test_scale_up():
    adapt = MyAdaptive(minimum=1, maximum=4)
    await adapt.adapt()
    assert adapt.log[-1][1] == {"status": "up", "n": 1}
    assert adapt.plan == {0}

    adapt._target = 10
    await adapt.adapt()
    assert adapt.log[-1][1] == {"status": "up", "n": 4}
    assert adapt.plan == {0, 1, 2, 3}


@pytest.mark.asyncio
async def test_scale_down():
    adapt = MyAdaptive(minimum=1, maximum=4, wait_count=2)
    adapt._target = 10
    await adapt.adapt()
    assert len(adapt.log) == 1

    adapt.observed = {0, 1, 3}  # all but 2 have arrived

    adapt._target = 2
    await adapt.adapt()
    assert len(adapt.log) == 1  # no change after only one call
    await adapt.adapt()
    assert len(adapt.log) == 2  # no change after only one call
    assert adapt.log[-1][1]["status"] == "down"
    assert 2 in adapt.log[-1][1]["workers"]
    assert len(adapt.log[-1][1]["workers"]) == 2

    old = list(adapt.log)
    await adapt.adapt()
    await adapt.adapt()
    await adapt.adapt()
    await adapt.adapt()
    assert list(adapt.log) == old


@pytest.mark.asyncio
async def test_interval():
    adapt = MyAdaptive(interval="5 ms")
    assert not adapt.plan

    for i in [0, 3, 1]:
        start = time()
        adapt._target = i
        while len(adapt.plan) != i:
            await asyncio.sleep(0.001)
            assert time() < start + 2

    adapt.stop()
    await asyncio.sleep(0.050)

    adapt._target = 10
    await asyncio.sleep(0.020)
    assert len(adapt.plan) == 1  # last value from before, unchanged
