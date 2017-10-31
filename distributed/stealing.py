from __future__ import print_function, division, absolute_import

from collections import defaultdict, deque
import logging
from math import log
import os
from time import time

from .config import config
from .core import CommClosedError
from .diagnostics.plugin import SchedulerPlugin
from .utils import key_split, log_errors, PeriodicCallback

try:
    from cytoolz import topk
except ImportError:
    from toolz import topk

BANDWIDTH = 100e6
LATENCY = 10e-3
log_2 = log(2)

logger = logging.getLogger(__name__)


LOG_PDB = config.get('pdb-on-err') or os.environ.get('DASK_ERROR_PDB', False)


class WorkStealing(SchedulerPlugin):
    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.stealable_all = [set() for i in range(15)]
        self.stealable = dict()
        self.key_stealable = dict()
        self.stealable_unknown_durations = defaultdict(set)

        self.cost_multipliers = [1 + 2 ** (i - 6) for i in range(15)]
        self.cost_multipliers[0] = 1

        for worker in scheduler.workers:
            self.add_worker(worker=worker)

        pc = PeriodicCallback(callback=self.balance,
                              callback_time=100)
        self._pc = pc
        self.scheduler.loop.add_callback(pc.start)
        self.scheduler.plugins.append(self)
        self.scheduler.extensions['stealing'] = self
        self.scheduler.events['stealing'] = deque(maxlen=100000)
        self.count = 0
        self.in_flight = dict()

        self.scheduler.worker_handlers['steal-response'] = self.move_task_confirm

    @property
    def log(self):
        return self.scheduler.events['stealing']

    def add_worker(self, scheduler=None, worker=None):
        self.stealable[worker] = [set() for i in range(15)]

    def remove_worker(self, scheduler=None, worker=None):
        del self.stealable[worker]

    def teardown(self):
        self._pc.stop()

    def transition(self, key, start, finish, compute_start=None,
                   compute_stop=None, *args, **kwargs):
        if finish == 'processing':
            self.put_key_in_stealable(key)

        if start == 'processing':
            self.remove_key_from_stealable(key)
            if finish == 'memory':
                ks = key_split(key)
                if ks in self.stealable_unknown_durations:
                    for k in self.stealable_unknown_durations.pop(ks):
                        if k in self.in_flight:
                            continue
                        if self.scheduler.task_state[k] == 'processing':
                            self.put_key_in_stealable(k, split=ks)
            else:
                if key in self.in_flight:
                    del self.in_flight[key]

    def put_key_in_stealable(self, key, split=None):
        worker = self.scheduler.rprocessing[key]
        cost_multiplier, level = self.steal_time_ratio(key, split=split)
        self.log.append(('add-stealable', key, worker, level))
        if cost_multiplier is not None:
            self.stealable_all[level].add(key)
            self.stealable[worker][level].add(key)
            self.key_stealable[key] = (worker, level)

    def remove_key_from_stealable(self, key):
        result = self.key_stealable.pop(key, None)
        if result is None:
            return

        worker, level = result
        self.log.append(('remove-stealable', key, worker, level))
        try:
            self.stealable[worker][level].remove(key)
        except KeyError:
            pass
        try:
            self.stealable_all[level].remove(key)
        except KeyError:
            pass

    def steal_time_ratio(self, key, split=None):
        """ The compute to communication time ratio of a key

        Returns
        -------

        cost_multiplier: The increased cost from moving this task as a factor.
        For example a result of zero implies a task without dependencies.
        level: The location within a stealable list to place this value
        """
        if (key not in self.scheduler.loose_restrictions
                and (key in self.scheduler.host_restrictions or
                     key in self.scheduler.worker_restrictions) or
                key in self.scheduler.resource_restrictions):
            return None, None  # don't steal

        if not self.scheduler.dependencies[key]:  # no dependencies fast path
            return 0, 0

        nbytes = sum(self.scheduler.nbytes.get(k, 1000)
                     for k in self.scheduler.dependencies[key])

        transfer_time = nbytes / BANDWIDTH + LATENCY
        split = split or key_split(key)
        if split in fast_tasks:
            return None, None
        try:
            worker = self.scheduler.rprocessing[key]
            compute_time = self.scheduler.processing[worker][key]
        except KeyError:
            self.stealable_unknown_durations[split].add(key)
            return None, None
        else:
            if compute_time < 0.005:  # 5ms, just give up
                return None, None
            cost_multiplier = transfer_time / compute_time
            if cost_multiplier > 100:
                return None, None

            level = int(round(log(cost_multiplier) / log_2 + 6, 0))
            level = max(1, level)
            return cost_multiplier, level

    def move_task_request(self, key, victim, thief):
        try:
            if self.scheduler.validate:
                if victim != self.scheduler.rprocessing[key]:
                    import pdb
                    pdb.set_trace()

            self.remove_key_from_stealable(key)
            logger.debug("Request move %s, %s: %2f -> %s: %2f", key,
                         victim, self.scheduler.occupancy[victim],
                         thief, self.scheduler.occupancy[thief])

            victim_duration = self.scheduler.processing[victim][key]
            self.scheduler.occupancy[victim] -= victim_duration
            self.scheduler.total_occupancy -= victim_duration

            thief_duration = self.scheduler.task_duration.get(key_split(key), 0.5)
            thief_duration += sum(self.scheduler.nbytes[key] for key in
                                  self.scheduler.dependencies[key] -
                                  self.scheduler.has_what[thief]) / BANDWIDTH
            self.scheduler.occupancy[thief] += thief_duration
            self.scheduler.total_occupancy += thief_duration

            self.scheduler.worker_comms[victim].send({'op': 'steal-request',
                                                      'key': key})

            self.in_flight[key] = {'victim': victim,
                                   'thief': thief,
                                   'victim_duration': victim_duration,
                                   'thief_duration': thief_duration}
        except CommClosedError:
            logger.info("Worker comm closed while stealing: %s", victim)
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb
                pdb.set_trace()
            raise

    def move_task_confirm(self, key=None, worker=None, state=None):
        try:
            try:
                d = self.in_flight.pop(key)
            except KeyError:
                return
            thief = d['thief']
            victim = d['victim']
            if (self.scheduler.task_state.get(key) != 'processing' or
                    self.scheduler.rprocessing[key] != victim):
                self.scheduler.occupancy[thief] = sum(self.scheduler.processing[thief].values())
                self.scheduler.occupancy[victim] = sum(self.scheduler.processing[victim].values())
                return

            # One of the pair has left, punt and reschedule
            if (thief not in self.scheduler.workers or
                victim not in self.scheduler.workers):
                self.scheduler.reschedule(key)
                return

            # Victim had already started execution, reverse stealing
            if state in ('memory', 'executing', 'long-running'):
                self.scheduler.occupancy[thief] -= d['thief_duration']
                self.scheduler.total_occupancy -= d['thief_duration']
                self.scheduler.occupancy[victim] += d['victim_duration']
                self.scheduler.total_occupancy += d['victim_duration']
                self.log.append(('already-computing', key, victim, thief))
                self.scheduler.check_idle_saturated(thief)
                self.scheduler.check_idle_saturated(victim)

            # Victim was waiting, has given up task, enact steal
            elif state in ('waiting', 'ready'):
                self.remove_key_from_stealable(key)
                self.scheduler.rprocessing[key] = thief
                del self.scheduler.processing[victim][key]
                self.scheduler.processing[thief][key] = d['thief_duration']
                self.put_key_in_stealable(key)

                try:
                    self.scheduler.send_task_to_worker(thief, key)
                except CommClosedError:
                    self.scheduler.remove_worker(thief)
                self.log.append(('confirm', key, victim, thief))
            else:
                raise ValueError("Unexpected task state: %s" % state)
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb
                pdb.set_trace()
            raise

    def balance(self):
        with log_errors():
            i = 0
            s = self.scheduler
            occupancy = s.occupancy
            idle = s.idle
            saturated = s.saturated
            if not idle or len(idle) == len(self.scheduler.workers):
                return

            log = list()
            start = time()

            seen = False
            acted = False

            if not s.saturated:
                saturated = topk(10, s.workers, key=occupancy.get)
                saturated = [w for w in saturated
                             if occupancy[w] > 0.2
                             and len(s.processing[w]) > s.ncores[w]]
            elif len(s.saturated) < 20:
                saturated = sorted(saturated, key=occupancy.get, reverse=True)

            if len(idle) < 20:
                idle = sorted(idle, key=occupancy.get)

            for level, cost_multiplier in enumerate(self.cost_multipliers):
                if not idle:
                    break
                for sat in list(saturated):
                    stealable = self.stealable[sat][level]
                    if not stealable or not idle:
                        continue
                    else:
                        seen = True

                    for key in list(stealable):
                        i += 1
                        if not idle:
                            break
                        idl = idle[i % len(idle)]

                        durations = s.processing[sat]
                        try:
                            duration = durations[key]
                        except KeyError:
                            stealable.remove(key)
                            continue

                        if (occupancy[idl] + cost_multiplier * duration
                                <= occupancy[sat] - duration / 2):
                            self.move_task_request(key, sat, idl)
                            log.append((start, level, key, duration,
                                        sat, occupancy[sat],
                                        idl, occupancy[idl]))
                            self.scheduler.check_idle_saturated(sat)
                            self.scheduler.check_idle_saturated(idl)
                            seen = True

                if self.cost_multipliers[level] < 20:  # don't steal from public at cost
                    stealable = self.stealable_all[level]
                    if stealable:
                        seen = True
                    for key in list(stealable):
                        if not idle:
                            break

                        try:
                            sat = s.rprocessing[key]
                        except KeyError:
                            stealable.remove(key)
                            continue
                        if occupancy[sat] < 0.2:
                            continue
                        if len(s.processing[sat]) <= s.ncores[sat]:
                            continue

                        i += 1
                        idl = idle[i % len(idle)]
                        duration = s.processing[sat][key]

                        if (occupancy[idl] + cost_multiplier * duration
                                <= occupancy[sat] - duration / 2):
                            self.move_task_request(key, sat, idl)
                            log.append((start, level, key, duration,
                                        sat, occupancy[sat],
                                        idl, occupancy[idl]))
                            self.scheduler.check_idle_saturated(sat)
                            self.scheduler.check_idle_saturated(idl)
                            seen = True

                if seen and not acted:
                    break

            if log:
                self.log.append(log)
                self.count += 1
            stop = time()
            if self.scheduler.digests:
                self.scheduler.digests['steal-duration'].add(stop - start)

    def restart(self, scheduler):
        for stealable in self.stealable.values():
            for s in stealable:
                s.clear()

        for s in self.stealable_all:
            s.clear()
        self.key_stealable.clear()
        self.stealable_unknown_durations.clear()

    def story(self, *keys):
        keys = set(keys)
        out = []
        for L in self.log:
            if not isinstance(L, list):
                L = [L]
            for t in L:
                if any(x in keys for x in t):
                    out.append(t)
        return out


fast_tasks = {'shuffle-split'}
