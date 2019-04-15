from __future__ import print_function, division, absolute_import

from collections import deque
import logging

from .progress_stream import color_of
from .plugin import SchedulerPlugin
from ..utils import key_split, format_time, parse_timedelta
from ..metrics import time


logger = logging.getLogger(__name__)


class TaskStreamPlugin(SchedulerPlugin):
    def __init__(self, scheduler, maxlen=100000):
        self.buffer = deque(maxlen=maxlen)
        self.scheduler = scheduler
        scheduler.add_plugin(self)
        self.index = 0

    def transition(self, key, start, finish, *args, **kwargs):
        if start == "processing":
            if key not in self.scheduler.tasks:
                return
            kwargs["key"] = key
            if finish == "memory" or finish == "erred":
                self.buffer.append(kwargs)
                self.index += 1

    def collect(self, start=None, stop=None, count=None):
        def bisect(target, left, right):
            if left == right:
                return left

            mid = (left + right) // 2
            value = max(stop for _, start, stop in self.buffer[mid]["startstops"])

            if value < target:
                return bisect(target, mid + 1, right)
            else:
                return bisect(target, left, mid)

        if isinstance(start, str):
            start = time() - parse_timedelta(start)
        if start is not None:
            start = bisect(start, 0, len(self.buffer))

        if isinstance(stop, str):
            stop = time() - parse_timedelta(stop)
        if stop is not None:
            stop = bisect(stop, 0, len(self.buffer))

        if count is not None:
            if start is None and stop is None:
                stop = len(self.buffer)
                start = stop - count
            elif start is None and stop is not None:
                start = stop - count
            elif start is not None and stop is None:
                stop = start + count

        if stop is None:
            stop = len(self.buffer)
        if start is None:
            start = 0

        start = max(0, start)
        stop = min(stop, len(self.buffer))

        return [self.buffer[i] for i in range(start, stop)]

    def rectangles(self, istart, istop=None, workers=None, start_boundary=0):
        msgs = []
        diff = self.index - len(self.buffer)
        if istop is None:
            istop = len(self.buffer)
        for i in range((istart or 0) - diff, istop - diff if istop else istop):
            msg = self.buffer[i]
            msgs.append(msg)

        return rectangles(msgs, workers=workers, start_boundary=start_boundary)


def rectangles(msgs, workers=None, start_boundary=0):
    if workers is None:
        workers = {}

    L_start = []
    L_duration = []
    L_duration_text = []
    L_key = []
    L_name = []
    L_color = []
    L_alpha = []
    L_worker = []
    L_worker_thread = []
    L_y = []

    for msg in msgs:
        key = msg["key"]
        name = key_split(key)
        startstops = msg.get("startstops", [])
        try:
            worker_thread = "%s-%d" % (msg["worker"], msg["thread"])
        except Exception:
            continue
            logger.warning("Message contained bad information: %s", msg, exc_info=True)
            worker_thread = ""

        if worker_thread not in workers:
            workers[worker_thread] = len(workers) / 2

        for action, start, stop in startstops:
            if start < start_boundary:
                continue
            color = colors[action]
            if type(color) is not str:
                color = color(msg)

            L_start.append((start + stop) / 2 * 1000)
            L_duration.append(1000 * (stop - start))
            L_duration_text.append(format_time(stop - start))
            L_key.append(key)
            L_name.append(prefix[action] + name)
            L_color.append(color)
            L_alpha.append(alphas[action])
            L_worker.append(msg["worker"])
            L_worker_thread.append(worker_thread)
            L_y.append(workers[worker_thread])

    return {
        "start": L_start,
        "duration": L_duration,
        "duration_text": L_duration_text,
        "key": L_key,
        "name": L_name,
        "color": L_color,
        "alpha": L_alpha,
        "worker": L_worker,
        "worker_thread": L_worker_thread,
        "y": L_y,
    }


def color_of_message(msg):
    if msg["status"] == "OK":
        split = key_split(msg["key"])
        return color_of(split)
    else:
        return "black"


colors = {
    "transfer": "red",
    "disk-write": "orange",
    "disk-read": "orange",
    "deserialize": "gray",
    "compute": color_of_message,
}


alphas = {
    "transfer": 0.4,
    "compute": 1,
    "deserialize": 0.4,
    "disk-write": 0.4,
    "disk-read": 0.4,
}


prefix = {
    "transfer": "transfer-",
    "disk-write": "disk-write-",
    "disk-read": "disk-read-",
    "deserialize": "deserialize-",
    "compute": "",
}
