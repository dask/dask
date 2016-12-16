from __future__ import print_function, division, absolute_import

from collections import defaultdict, deque
import logging
from math import log
import random
from time import time

from tornado.iostream import StreamClosedError
from tornado.ioloop import PeriodicCallback

from .utils import key_split, log_errors
from .diagnostics.plugin import SchedulerPlugin

BANDWIDTH = 100e6
LATENCY = 10e-3

logger = logging.getLogger(__name__)


class WorkStealing(SchedulerPlugin):
    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.stealable = [set() for i in range(15)]
        self.key_stealable = dict()
        self.stealable_unknown_durations = defaultdict(set)

        self._pc = PeriodicCallback(callback=self.balance,
                                    callback_time=100,
                                    io_loop=self.scheduler.loop)
        self.scheduler.loop.add_callback(self._pc.start)
        self.scheduler.plugins.append(self)
        self.scheduler.extensions['stealing'] = self
        self.log = deque(maxlen=100000)

    def teardown(self):
        self._pc.stop()

    def transition(self, key, start, finish, compute_start=None,
            compute_stop=None, *args, **kwargs):
        if finish == 'processing':
            self.put_key_in_stealable(key)

        if start == 'processing' and finish == 'memory':
            self.remove_key_from_stealable(key)
            ks = key_split(key)
            if ks in self.stealable_unknown_durations:
                for key in self.stealable_unknown_durations.pop(ks):
                    self.put_key_in_stealable(key, split=ks)


    def put_key_in_stealable(self, key, split=None):
        ratio, loc = self.steal_time_ratio(key, split=split)
        if ratio is not None:
            self.stealable[loc].add(key)
            self.key_stealable[key] = loc

    def remove_key_from_stealable(self, key):
        loc = self.key_stealable.pop(key, None)
        if loc is not None:
            try:
                self.stealable[loc].remove(key)
            except:
                pass

    def steal_time_ratio(self, key, split=None):
        """ The compute to communication time ratio of a key

        Returns
        -------

        ratio: The compute/communication time ratio of the task
        loc: The self.stealable bin into which this key should go
        """
        if (key not in self.scheduler.loose_restrictions
                and (key in self.scheduler.host_restrictions or
                     key in self.scheduler.worker_restrictions) or
            key in self.scheduler.resource_restrictions):
            return None, None  # don't steal

        if not self.scheduler.dependencies[key]:  # no dependencies fast path
            return 10000, 0

        nbytes = sum(self.scheduler.nbytes.get(k, 1000)
                     for k in self.scheduler.dependencies[key])

        transfer_time = nbytes / BANDWIDTH + LATENCY
        split = split or key_split(key)
        if split in fast_tasks:
            return None, None
        try:
            compute_time = self.scheduler.task_duration[split]
        except KeyError:
            self.stealable_unknown_durations[split].add(key)
            return None, None
        else:
            try:
                ratio = compute_time / transfer_time
            except ZeroDivisionError:
                ratio = 10000
            if ratio == 10000:
                loc = 0
            elif ratio > 32:
                loc = 1
            elif ratio < 2**-8:
                loc = -1
            else:
                loc = int(-round(log(ratio) / log(2), 0) + 5) + 1
            return ratio, loc

    def move_task(self, key, victim, thief):
        with log_errors():
            if self.scheduler.validate:
                if victim not in self.scheduler.rprocessing[key]:
                    import pdb; pdb.set_trace()

            logger.info("Moved %s, %s: %2f -> %s: %2f", key,
                    victim, self.scheduler.occupancy[victim],
                    thief, self.scheduler.occupancy[thief])

            duration = self.scheduler.processing[victim].pop(key)
            self.scheduler.rprocessing[key].remove(victim)
            self.scheduler.occupancy[victim] -= duration
            self.scheduler.total_occupancy -= duration

            duration = self.scheduler.task_duration.get(key_split(key), 0.5)
            self.scheduler.processing[thief][key] = duration
            self.scheduler.rprocessing[key].add(thief)
            self.scheduler.occupancy[thief] += duration
            self.scheduler.total_occupancy += duration

            self.scheduler.worker_streams[victim].send({'op': 'release-task',
                                                        'key': key})

            try:
                self.scheduler.send_task_to_worker(thief, key)
            except StreamClosedError:
                self.scheduler.remove_worker(thief)

    def balance(self):
        with log_errors():
            if not self.scheduler.idle or not self.scheduler.saturated:
                return

            broken = False

            with log_errors():
                start = time()
                for level, stealable in enumerate(self.stealable[:-1]):
                    if broken or not stealable:
                        continue

                    original = stealable

                    ratio = 2 ** (level - 5 + 1)

                    n_stealable = sum(len(s) for s in self.stealable[level:-1])
                    duration_if_hold = n_stealable / len(self.scheduler.saturated)
                    duration_if_steal = ratio

                    if level > 1 and duration_if_hold < duration_if_steal:
                        break

                    for key in list(stealable):
                        if self.scheduler.task_state.get(key) != 'processing':
                            original.remove(key)
                            continue
                        victim = max(self.scheduler.rprocessing[key],
                                     key=self.scheduler.occupancy.get)
                        if victim not in self.scheduler.idle:
                            thief = random.choice(self.scheduler.idle)
                            self.move_task(key, victim, thief)
                            self.log.append((level, victim, thief, key))
                            self.scheduler.check_idle_saturated(victim)
                            self.scheduler.check_idle_saturated(thief)
                            original.remove(key)

                        if not self.scheduler.idle or not self.scheduler.saturated:
                            broken = True
                            break

                stop = time()
                if self.scheduler.digests:
                    self.scheduler.digests['steal-duration'].add(stop - start)

    def restart(self):
        for stealable in self.stealable:
            stealable.clear()

        self.key_stealable.clear()
        self.stealable_unknown_durations.clear()


fast_tasks = {'shuffle-split'}
