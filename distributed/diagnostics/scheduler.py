from __future__ import print_function, division, absolute_import

import os
import pandas as pd


def scheduler_status_str(d):
    """ Render scheduler status as a string

    Consumes data from status.json route

    Examples
    --------
    >>> d = {"address": "SCHEDULER_ADDRESS:9999",
    ...      "ready": 5,
    ...      "ncores": {"192.168.1.107:44544": 4,
    ...                 "192.168.1.107:36441": 4},
    ...      "in-memory": 30, "waiting": 20,
    ...      "processing": {"192.168.1.107:44544": {'inc': 3, 'add': 1},
    ...                     "192.168.1.107:36441": {'inc': 2}},
    ...      "tasks": 70,
    ...      "failed": 9,
    ...      "bytes": {"192.168.1.107:44544": 1000,
    ...                "192.168.1.107:36441": 2000}}

    >>> print(scheduler_status_str(d))  # doctest: +NORMALIZE_WHITESPACE
    Scheduler: SCHEDULER_ADDRESS:9999
    <BLANKLINE>
                 Count                                  Progress
    Tasks
    waiting         20  +++++++++++
    ready            5  ++
    failed           9  +++++
    in-progress      6  +++
    in-memory       30  +++++++++++++++++
    total           70  ++++++++++++++++++++++++++++++++++++++++
    <BLANKLINE>
                         Ncores  Bytes  Processing
    Workers
    192.168.1.107:36441       4   2000       [inc]
    192.168.1.107:44544       4   1000  [add, inc]
    """
    sched = scheduler_progress_df(d)
    workers = worker_status_df(d)
    s = "Scheduler: %s\n\n%s\n\n%s" % (d['address'], sched, workers)
    return os.linesep.join(map(str.rstrip, s.split(os.linesep)))


def scheduler_progress_df(d):
    """ Convert status response to DataFrame of total progress

    Consumes dictionary from status.json route

    Examples
    --------
    >>> d = {"ready": 5, "in-memory": 30, "waiting": 20,
    ...      "tasks": 70, "failed": 9,
    ...      "processing": {"192.168.1.107:44544": {'inc': 3, 'add': 1},
    ...                     "192.168.1.107:36441": {'inc': 2}},
    ...      "other-keys-are-fine-too": ''}

    >>> scheduler_progress_df(d)  # doctest: +SKIP
                 Count                                  Progress
    Tasks
    waiting         20  +++++++++++
    ready            5  ++
    failed           9  +++++
    in-progress      6  +++
    in-memory       30  +++++++++++++++++
    total           70  ++++++++++++++++++++++++++++++++++++++++
    """
    d = d.copy()
    d['in-progress'] = sum(v for vv in d['processing'].values() for v in vv.values())
    d['total'] = d.pop('tasks')
    names = ['waiting', 'ready', 'failed', 'in-progress', 'in-memory', 'total']
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
    ...      "ncores": {"192.168.1.107:44544": 4,
    ...                 "192.168.1.107:36441": 4},
    ...      "processing": {"192.168.1.107:44544": {'inc': 3, 'add': 1},
    ...                     "192.168.1.107:36441": {'inc': 2}},
    ...      "bytes": {"192.168.1.107:44544": 1000,
    ...                "192.168.1.107:36441": 2000}}

    >>> worker_status_df(d)  # doctest: +SKIP
                         Ncores  Bytes  Processing
    Workers
    192.168.1.107:36441       4   2000       [inc]
    192.168.1.107:44544       4   1000  [add, inc]
    """
    names = ['ncores', 'bytes', 'processing']
    df = pd.DataFrame({k: d[k] for k in names}, columns=names)
    df['processing'] = df['processing'].apply(sorted)
    df.columns = df.columns.map(str.title)
    df.index.name = 'Workers'
    df = df.sort_index()
    return df
