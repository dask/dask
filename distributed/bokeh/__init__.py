from collections import deque

from ..metrics import time

n = 60
m = 100000

messages = {
    'workers': {'interval': 1000,
                'deque': deque(maxlen=n),
                'times': deque(maxlen=n),
                'index': deque(maxlen=n),
                'plot-data': {'time': deque(maxlen=n),
                              'cpu': deque(maxlen=n),
                              'memory_percent': deque(maxlen=n),
                              'network-send': deque(maxlen=n),
                              'network-recv': deque(maxlen=n)}},

    'tasks': {'interval': 150,
              'deque': deque(maxlen=100),
              'times': deque(maxlen=100)},

    'progress': {},

    'processing': {'processing': {}, 'memory': 0, 'waiting': 0},

    'task-events': {'interval': 200,
                    'deque': deque(maxlen=m),
                    'times': deque(maxlen=m),
                    'index': deque(maxlen=m),
                    'rectangles':{name: deque(maxlen=m) for name in
                                 'start duration key name color worker worker_thread y alpha'.split()},
                    'workers': dict(),
                    'last_seen': [time()]}
}
