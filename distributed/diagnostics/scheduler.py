from __future__ import print_function, division, absolute_import

from datetime import datetime
import os

from toolz import countby, concat, dissoc

from ..utils import key_split


def tasks(s):
    """ Task and worker status of scheduler """
    processing = sum(map(len, s.processing.values()))

    return {'processing': processing,
            'total': len(s.tasks),
            'in-memory': len(s.who_has),
            'ready': len(s.ready)
                   + sum(map(len, s.stacks.values())),
            'waiting': len(s.waiting),
            'failed': len(s.exceptions_blame)}


def workers(s):
    """ Information about workers

    Examples
    --------
    >>> workers(my_scheduler)  # doctest: +SKIP
    {'127.0.0.1': {'cores': 3,
                   'cpu': 0.0,
                   'last-seen': 0.003068,
                   'latency': 0.01584628690034151,
                   'ports': ['54871', '50943'],
                   'processing': {'inc': 2, 'add': 1},
                   'disk-read': 1234,
                   'disk-write': 1234,
                   'network-send': 1234,
                   'network-recv': 1234,
                   'memory': 16701911040,
                   'memory-percent': 85}}
    """
    hosts = {host: ['%s:%s' % (host, port) for port in d['ports']]
                for host, d in s.host_info.items()}

    processing = {host: countby(key_split, concat(s.processing[w] for w in addrs))
                  for host, addrs in hosts.items()}

    now = datetime.now()

    result = {}
    for host, info in s.host_info.items():
        info = dissoc(info, 'heartbeat', 'heartbeat-port')
        info['processing'] = processing[host]
        result[host] = info
        info['ports'] = list(info['ports'])
        if 'last-seen' in info:
            info['last-seen'] = (now - info['last-seen']).total_seconds()

    return result


def scheduler_progress_df(d):
    """ Convert status response to DataFrame of total progress

    Consumes dictionary from status.json route

    Examples
    --------
    >>> d = {"ready": 5, "in-memory": 30, "waiting": 20,
    ...      "tasks": 70, "failed": 9,
    ...      "processing": 6,
    ...      "other-keys-are-fine-too": ''}

    >>> scheduler_progress_df(d)  # doctest: +SKIP
                 Count                                  Progress
    Tasks
    waiting         20  +++++++++++
    ready            5  ++
    failed           9  +++++
    processing       6  +++
    in-memory       30  +++++++++++++++++
    total           70  ++++++++++++++++++++++++++++++++++++++++
    """
    import pandas as pd
    d = d.copy()
    d['total'] = d.pop('tasks')
    names = ['waiting', 'ready', 'failed', 'processing', 'in-memory', 'total']
    df = pd.DataFrame(pd.Series({k: d[k] for k in names},
                                index=names, name='Count'))
    if d['total']:
        barlength = (40 * df.Count / d['total']).astype(int)
        df['Progress'] = barlength.apply(lambda n: ('%-40s' % (n * '+').rstrip(' ')))
    else:
        df['Progress'] = 0

    df.index.name = 'Tasks'

    return df


def worker_status_df(d):
    """ Status of workers as a Pandas DataFrame

    Consumes data from status.json route.

    Examples
    --------
    >>> d = {"other-keys-are-fine-too": '',
    ...      "ncores": {"192.168.1.107": 4,
    ...                 "192.168.1.108": 4},
    ...      "processing": {"192.168.1.108": {'inc': 3, 'add': 1},
    ...                     "192.168.1.107": {'inc': 2}},
    ...      "bytes": {"192.168.1.108": 1000,
    ...                "192.168.1.107": 2000}}

    >>> worker_status_df(d)
                   Ncores  Bytes  Processing
    Workers
    192.168.1.107       4   2000       [inc]
    192.168.1.108       4   1000  [add, inc]
    """
    import pandas as pd
    names = ['ncores', 'bytes', 'processing']
    df = pd.DataFrame({k: d[k] for k in names}, columns=names)
    df['processing'] = df['processing'].apply(sorted)
    df.columns = df.columns.map(str.title)
    df.index.name = 'Workers'
    df = df.sort_index()
    return df
