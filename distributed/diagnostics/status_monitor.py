from __future__ import print_function, division, absolute_import

from collections import defaultdict
import json
import time
from tornado import gen

from ..core import rpc
from ..utils import ignoring, is_kernel
from ..executor import default_executor
from ..scheduler import Scheduler

from tornado.httpclient import AsyncHTTPClient
from tornado.ioloop import IOLoop, PeriodicCallback

with ignoring(ImportError):
    from bokeh.palettes import Spectral11
    from bokeh.models import ColumnDataSource
    from bokeh.models.widgets import DataTable, TableColumn
    from bokeh.plotting import vplot, output_notebook, show, figure
    from bokeh.io import curstate, push_notebook

class Status_Monitor(object):
    """ Display the tasks running and waiting on each worker

    Parameters
    ----------
    addr: tuple, optional
        (ip, port) of scheduler.  Defaults to scheduler of recent Executor
    interval: Number, optional
        Interval between updates.  Defaults to 1s
    """
    def __init__(self, addr=None, interval=1000.00):
        if addr is None:
            scheduler = default_executor().scheduler
            if isinstance(scheduler, rpc):
                addr = (scheduler.ip, scheduler.port) # doesn't work
            elif isinstance(scheduler, Scheduler):
                addr = ('127.0.0.1', scheduler.services['http'].port)
        self.addr = addr
        self.interval = interval

        self.display_notebook = False

        if is_kernel() and not curstate().notebook:
            output_notebook()
            assert curstate().notebook

        wkr_props = ['workers', 'ncores', 'nbytes', 'processing']
        self.wkr_source = ColumnDataSource({k: [] for k in wkr_props})

        wkr_columns = [TableColumn(field=prop, title=prop) for prop in wkr_props]
        self.worker_table = DataTable(source=self.wkr_source, columns=wkr_columns,
                                      width=600, height=100)

        task_props = ['waiting', 'ready', 'failed', 'in-progress',
                      'in-memory', 'total']
        self.task_source = ColumnDataSource({k: [] for k in task_props})

        task_columns = [TableColumn(field=prop, title=prop) for prop in task_props]
        self.task_table = DataTable(source=self.task_source, columns=task_columns,
                                    row_headers=False, width=600, height=60)

        ts_props = ['time','workers', 'nbytes']
        # self.ts_source = ColumnDataSource({k: [[0,1],[0,1]] for k in ts_props})
        self.ts_source = ColumnDataSource({k: [] for k in ts_props})
        self.plot = figure(height=400, width=600, x_axis_type='datetime')
        self.plot.multi_line(xs='time', ys='nbytes',
                             color=Spectral11,
                             source=self.ts_source)

        # For managing monitor state
        self.nbytes = defaultdict(list)
        self.times = []
        self.start = time.time()

        self.output = vplot(self.task_table, self.worker_table, self.plot)

        self.client = AsyncHTTPClient()

        loop = IOLoop.current()
        loop.add_callback(self.update)
        pc = PeriodicCallback(self.update, self.interval, io_loop=loop)
        pc.start()

    def _ipython_display_(self, **kwargs):
        show(self.output)
        # show(vplot(self.task_table, self.worker_table))
        self.display_notebook = True

    @gen.coroutine
    def update(self):
        """ Query the Scheduler, update the figure

        This opens a connection to the scheduler, sends it a function to run
        periodically, streams the results back and uses those results to update
        the bokeh figure
        """

        response = yield self.client.fetch('http://%s:%d/status.json' % self.addr)
        d = json.loads(response.body.decode())

        self.wkr_source.data['time'] = [time.time()]
        self.wkr_source.data['workers'] = [k for k, v in sorted(d['ncores'].items(),
                                           key=lambda x: x[0], reverse=True)]
        self.wkr_source.data['ncores'] = [v for k, v in sorted(d['ncores'].items(),
                                          key=lambda x: x[0], reverse=True)]
        self.wkr_source.data['nbytes'] = [v for k, v in sorted(d['bytes'].items(),
                                          key=lambda x: x[0], reverse=True)]
        self.wkr_source.data['processing'] = [list(v.keys()) for v in d['processing'].values()]

        self.task_source.data['waiting'] = [d['waiting']]
        self.task_source.data['ready'] = [d['ready']]
        self.task_source.data['failed'] = [d['failed']]
        self.task_source.data['in-progress'] = [sum(v for vv in d['processing'].values() for v in vv.values())]
        self.task_source.data['in-memory'] = [d['in-memory']]
        self.task_source.data['total'] = [d['tasks']]

        for k,v in d['bytes'].items():
            self.nbytes[k].append(v)
        self.times.append(time.time() * 1000 - self.start * 1000) #bokeh uses msse

        self.ts_source.data['time'] = [self.times for k in self.nbytes.keys()]
        self.ts_source.data['nbytes'] = [v for k, v in sorted(self.nbytes.items(),
                                         key=lambda x: x[0], reverse=True)]

        if self.display_notebook:
            push_notebook()
