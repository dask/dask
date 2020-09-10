import collections
import logging
import math

from tornado.ioloop import IOLoop, PeriodicCallback
import tlz as toolz

from ..metrics import time
from ..utils import parse_timedelta


logger = logging.getLogger(__name__)


class AdaptiveCore:
    """
    The core logic for adaptive deployments, with none of the cluster details

    This class controls our adaptive scaling behavior.  It is intended to be
    used as a super-class or mixin.  It expects the following state and methods:

    **State**

    plan: set
        A set of workers that we think should exist.
        Here and below worker is just a token, often an address or name string

    requested: set
        A set of workers that the cluster class has successfully requested from
        the resource manager.  We expect that resource manager to work to make
        these exist.

    observed: set
        A set of workers that have successfully checked in with the scheduler

    These sets are not necessarily equivalent.  Often plan and requested will
    be very similar (requesting is usually fast) but there may be a large delay
    between requested and observed (often resource managers don't give us what
    we want).

    **Functions**

    target : -> int
        Returns the target number of workers that should exist.
        This is often obtained by querying the scheduler

    workers_to_close : int -> Set[worker]
        Given a target number of workers,
        returns a set of workers that we should close when we're scaling down

    scale_up : int -> None
        Scales the cluster up to a target number of workers, presumably
        changing at least ``plan`` and hopefully eventually also ``requested``

    scale_down : Set[worker] -> None
        Closes the provided set of workers

    Parameters
    ----------
    minimum: int
        The minimum number of allowed workers
    maximum: int
        The maximum number of allowed workers
    wait_count: int
        The number of scale-down requests we should receive before actually
        scaling down
    interval: str
        The amount of time, like ``"1s"`` between checks
    """

    def __init__(
        self,
        minimum: int = 0,
        maximum: int = math.inf,
        wait_count: int = 3,
        interval: str = "1s",
    ):
        self.minimum = minimum
        self.maximum = maximum
        self.wait_count = wait_count
        self.interval = parse_timedelta(interval, "seconds") if interval else interval
        self.periodic_callback = None

        def f():
            try:
                self.periodic_callback.start()
            except AttributeError:
                pass

        if self.interval:
            self.periodic_callback = PeriodicCallback(self.adapt, self.interval * 1000)
            try:
                self.loop.add_callback(f)
            except AttributeError:
                IOLoop.current().add_callback(f)

        try:
            self.plan = set()
            self.requested = set()
            self.observed = set()
        except Exception:
            pass

        # internal state
        self.close_counts = collections.defaultdict(int)
        self._adapting = False
        self.log = collections.deque(maxlen=10000)

    def stop(self):
        logger.info("Adaptive stop")

        if self.periodic_callback:
            self.periodic_callback.stop()
            self.periodic_callback = None

    async def target(self) -> int:
        """ The target number of workers that should exist """
        raise NotImplementedError()

    async def workers_to_close(self, target: int) -> list:
        """
        Give a list of workers to close that brings us down to target workers
        """
        # TODO, improve me with something that thinks about current load
        return list(self.observed)[target:]

    async def safe_target(self) -> int:
        """ Used internally, like target, but respects minimum/maximum """
        n = await self.target()
        if n > self.maximum:
            n = self.maximum

        if n < self.minimum:
            n = self.minimum

        return n

    async def recommendations(self, target: int) -> dict:
        """
        Make scale up/down recommendations based on current state and target
        """
        plan = self.plan
        requested = self.requested
        observed = self.observed

        if target == len(plan):
            self.close_counts.clear()
            return {"status": "same"}

        elif target > len(plan):
            self.close_counts.clear()
            return {"status": "up", "n": target}

        elif target < len(plan):
            not_yet_arrived = requested - observed
            to_close = set()
            if not_yet_arrived:
                to_close.update((toolz.take(len(plan) - target, not_yet_arrived)))

            if target < len(plan) - len(to_close):
                L = await self.workers_to_close(target=target)
                to_close.update(L)

            firmly_close = set()
            for w in to_close:
                self.close_counts[w] += 1
                if self.close_counts[w] >= self.wait_count:
                    firmly_close.add(w)

            for k in list(self.close_counts):  # clear out unseen keys
                if k in firmly_close or k not in to_close:
                    del self.close_counts[k]

            if firmly_close:
                return {"status": "down", "workers": list(firmly_close)}
            else:
                return {"status": "same"}

    async def adapt(self) -> None:
        """
        Check the current state, make recommendations, call scale

        This is the main event of the system
        """
        if self._adapting:  # Semaphore to avoid overlapping adapt calls
            return
        self._adapting = True

        try:

            target = await self.safe_target()
            recommendations = await self.recommendations(target)

            if recommendations["status"] != "same":
                self.log.append((time(), dict(recommendations)))

            status = recommendations.pop("status")
            if status == "same":
                return
            if status == "up":
                await self.scale_up(**recommendations)
            if status == "down":
                await self.scale_down(**recommendations)
        except OSError:
            self.stop()
        finally:
            self._adapting = False
