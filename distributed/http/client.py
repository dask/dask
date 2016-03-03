import json
import logging
import requests
import os
import pandas as pd

from tornado.ioloop import IOLoop, PeriodicCallback
from tornado.httpclient import AsyncHTTPClient
from tornado import gen


logger = logging.getLogger(__name__)


def scheduler_status_str(d):
    """ Render the result of the /status.json route as a string

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


def scheduler_status_widget(d, widget=None):
    """ IPython widget to display scheduler status

    See also:
        scheduler_status_str
    """
    from ipywidgets import HTML, VBox
    if widget is None:
        widget = VBox([HTML(''), HTML(''), HTML('')])
    header = '<h3>Scheduler: %s</h3>' % d['address']
    sched = scheduler_progress_df(d)
    workers = worker_status_df(d)
    widget.children[0].value = header
    widget.children[1].value = sched.to_html()
    widget.children[2].value = workers.to_html()
    logger.debug("Update scheduler status widget")
    return widget


@gen.coroutine
def update_status_widget(widget, ip, port):
    client = AsyncHTTPClient()
    try:
        response = yield client.fetch('http://%s:%d/status.json' % (ip, port))
        d = json.loads(response.body.decode())
        scheduler_status_widget(d, widget)
    except Exception as e:
        logger.exception(e)
        raise


def live_info(e, interval=200, port=9786, loop=None):
    from ipywidgets import HTML, VBox
    loop = loop or IOLoop.current()
    widget = VBox([HTML(''), HTML(''), HTML('')])
    cb = lambda: update_status_widget(widget, e.scheduler.ip, port)
    loop.add_callback(cb)
    pc = PeriodicCallback(cb, interval, io_loop=loop)
    pc.start()

    return widget


def scheduler_progress_df(d):
    """ Convert status response to DataFrame of total progress

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
    """

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
