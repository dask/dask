from toolz import get
from tornado import gen

from .core import connect, read, write
from .utils import ignoring

with ignoring(ImportError):
    from bokeh.plotting import figure, Figure, show, output_notebook, ColumnDataSource
    from bokeh.models import HoverTool


class ResourceMonitor(object):
    def __init__(self, addr=None, interval=1.00, notebook=True):
        if addr is None:
            scheduler = default_executor().scheduler
            addr = (scheduler.ip, scheduler.port)


        self.cds = ColumnDataSource({k: []
            for k in ['host', 'cpu', 'memory',
                      'zero', 'left', 'right']})

        self.notebook = notebook

        hover = HoverTool(
            tooltips=[
                ("host", "@host"),
                ("cpu", "@cpu"),
                ("memory", "@memory"),
            ]
        )

        self.figure = figure(height=200, width=800,
                             y_range=(0, 100), tools=[hover])
        self.figure.logo = None

        self.figure.quad(legend='cpu', left='left', right='right',
                         bottom='zero', top='cpu', source=self.cds,
                         color=(0, 0, 255, 0.5))
        self.figure.quad(legend='memory', left='left', right='right',
                         bottom='zero', top='memory', source=self.cds,
                         color=(255, 0, 0, 0.5))

        self.future = self.coroutine(addr, interval)

    def _ipython_display_(self, **kwargs):
        return show(self.figure)

    @gen.coroutine
    def coroutine(self, addr, interval):
        self.stream = yield connect(*addr)

        def func(scheduler):
            workers = [k for k, v in sorted(scheduler.ncores.items(),
                                            key=lambda x: x[0], reverse=True)]
            nannies = [(ip, scheduler.nannies[(ip, port)])
                       for ip, port in workers]
            dicts = [get(-1, scheduler.resource_logs[w], dict())
                     for w in nannies]

            return {'workers': workers,
                    'cpu': [d.get('cpu_percent', -1) for d in dicts],
                    'memory': [d.get('memory_percent', -1) for d in dicts]}

        yield write(self.stream, {'op': 'feed',
                                  'function': func,
                                  'interval': interval})
        while True:
            try:
                response = yield read(self.stream)
            except Exception:
                break

            self.cds.data['host'] = [host for host, port in response['workers']]
            self.cds.data['cpu'] = response['cpu']
            self.cds.data['memory'] = response['memory']

            n = len(response['workers'])

            self.cds.data['zero'] = [0] * n
            self.cds.data['left'] = [i + 0.00 for i in range(n)]
            self.cds.data['right'] = [i + 1.00 for i in range(n)]

            if self.notebook:
                self.cds.push_notebook()
