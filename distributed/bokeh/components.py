from __future__ import print_function, division, absolute_import

from bisect import bisect
from operator import add

from bokeh.layouts import row, column
from bokeh.models import (
    ColumnDataSource, Plot, Datetime, DataRange1d, Rect, LinearAxis,
    DatetimeAxis, Grid, BasicTicker, HoverTool, BoxZoomTool, ResetTool,
    PanTool, WheelZoomTool, Title, Range1d, Quad, Text, value, Line,
    NumeralTickFormatter, ToolbarBox, Legend, LegendItem, BoxSelectTool,
    Circle
)
from bokeh.models.widgets import DataTable, TableColumn, NumberFormatter
from bokeh.palettes import Spectral9
from bokeh.plotting import figure
from toolz import valmap

from distributed.diagnostics.progress_stream import progress_quads, nbytes_bar
from distributed.utils import log_errors

# from .export_tool import ExportTool


class DashboardComponent(object):
    """ Base class for Dask.distributed UI dashboard components.

    This class must have two attributes, ``root`` and ``source``, and one
    method ``update``:

    *  source: a Bokeh ColumnDataSource
    *  root: a Bokeh Model
    *  update: a method that consumes the messages dictionary found in
               distributed.bokeh.messages
    """
    def __init__(self):
        self.source = None
        self.root = None

    def update(self, messages):
        """ Reads from bokeh.distributed.messages and updates self.source """
        pass


class TaskStream(DashboardComponent):
    """ Task Stream

    The start and stop time of tasks as they occur on each core of the cluster.
    """
    def __init__(self, n_rectangles=1000, clear_interval=20000, **kwargs):
        """
        kwargs are applied to the bokeh.models.plots.Plot constructor
        """
        self.n_rectangles = n_rectangles
        self.clear_interval = clear_interval
        self.last = 0

        self.source = ColumnDataSource(data=dict(
            start=[], duration=[], key=[], name=[], color=[],
            worker=[], y=[], worker_thread=[], alpha=[])
        )

        x_range = DataRange1d()
        y_range = DataRange1d(range_padding=0)

        self.root = Plot(
            title=Title(text="Task Stream"), id='bk-task-stream-plot',
            x_range=x_range, y_range=y_range, toolbar_location="above",
            min_border_right=35, **kwargs
        )

        self.root.add_glyph(
            self.source,
            Rect(x="start", y="y", width="duration", height=0.8, fill_color="color",
                 line_color="color", line_alpha=0.6, fill_alpha="alpha", line_width=3)
        )

        self.root.add_layout(DatetimeAxis(axis_label="Time"), "below")

        ticker = BasicTicker(num_minor_ticks=0)
        self.root.add_layout(LinearAxis(axis_label="Worker Core", ticker=ticker), "left")
        self.root.add_layout(Grid(dimension=1, grid_line_alpha=0.4, ticker=ticker))

        hover = HoverTool(
            point_policy="follow_mouse",
            tooltips="""
                <div>
                    <span style="font-size: 12px; font-weight: bold;">@name:</span>&nbsp;
                    <span style="font-size: 10px; font-family: Monaco, monospace;">@duration</span>
                    <span style="font-size: 10px;">ms</span>&nbsp;
                </div>
                """
        )

        # export = ExportTool()
        # export.register_plot(self.root)

        self.root.add_tools(
            hover,
            # export,
            BoxZoomTool(),
            ResetTool(reset_size=False),
            PanTool(dimensions="width"),
            WheelZoomTool(dimensions="width")
        )

        # Required for update callback
        self.task_stream_index = [0]

    def update(self, messages):
        with log_errors():
            index = messages['task-events']['index']
            old = rectangles = messages['task-events']['rectangles']

            if not index or index[-1] == self.task_stream_index[0]:
                return

            ind = bisect(index, self.task_stream_index[0])
            rectangles = {k: [v[i] for i in range(ind, len(index))]
                          for k, v in rectangles.items()}
            self.task_stream_index[0] = index[-1]

            # If there has been a significant delay then clear old rectangles
            if rectangles['start']:
                m = min(map(add, rectangles['start'], rectangles['duration']))
                if m > self.last:
                    self.last, last = m, self.last
                    if m > last + self.clear_interval:
                        self.source.data.update(rectangles)
                        return

            self.source.stream(rectangles, self.n_rectangles)


class TaskProgress(DashboardComponent):
    """ Progress bars per task type """

    def __init__(self, **kwargs):
        data = progress_quads(dict(all={}, memory={}, erred={}, released={}))
        self.source = ColumnDataSource(data=data)

        x_range = DataRange1d()
        y_range = Range1d(-8, 0)

        self.root = Plot(
            id='bk-task-progress-plot',
            x_range=x_range, y_range=y_range, toolbar_location=None, **kwargs
        )
        self.root.add_glyph(
            self.source,
            Quad(top='top', bottom='bottom', left='left', right='right',
                 fill_color="#aaaaaa", line_color="#aaaaaa", fill_alpha=0.2)
        )
        self.root.add_glyph(
            self.source,
            Quad(top='top', bottom='bottom', left='left', right='released-loc',
                 fill_color="color", line_color="color", fill_alpha=0.6)
        )
        self.root.add_glyph(
            self.source,
            Quad(top='top', bottom='bottom', left='released-loc',
                 right='memory-loc', fill_color="color", line_color="color",
                 fill_alpha=1.0)
        )
        self.root.add_glyph(
            self.source,
            Quad(top='top', bottom='bottom', left='erred-loc',
                 right='erred-loc', fill_color='#000000', line_color='#000000',
                 fill_alpha=0.3)
        )
        self.root.add_glyph(
            self.source,
            Text(text='show-name', y='bottom', x='left', x_offset=5,
                 text_font_size=value('10pt'))
        )
        self.root.add_glyph(
            self.source,
            Text(text='done', y='bottom', x='right', x_offset=-5,
                 text_align='right', text_font_size=value('10pt'))
        )

        hover = HoverTool(
            point_policy="follow_mouse",
            tooltips="""
                <div>
                    <span style="font-size: 14px; font-weight: bold;">Name:</span>&nbsp;
                    <span style="font-size: 10px; font-family: Monaco, monospace;">@name</span>
                </div>
                <div>
                    <span style="font-size: 14px; font-weight: bold;">All:</span>&nbsp;
                    <span style="font-size: 10px; font-family: Monaco, monospace;">@all</span>
                </div>
                <div>
                    <span style="font-size: 14px; font-weight: bold;">Memory:</span>&nbsp;
                    <span style="font-size: 10px; font-family: Monaco, monospace;">@memory</span>
                </div>
                <div>
                    <span style="font-size: 14px; font-weight: bold;">Erred:</span>&nbsp;
                    <span style="font-size: 10px; font-family: Monaco, monospace;">@erred</span>
                </div>
                """
        )
        self.root.add_tools(hover)

    def update(self, messages):
        with log_errors():
            msg = messages['progress']
            if not msg:
                return
            d = progress_quads(msg)
            self.source.data.update(d)
            if messages['tasks']['deque']:
                self.root.title.text = ("Progress -- total: %(total)s, "
                    "in-memory: %(in-memory)s, processing: %(processing)s, "
                    "waiting: %(waiting)s, failed: %(failed)s"
                    % messages['tasks']['deque'][-1])


class MemoryUsage(DashboardComponent):
    """ The memory usage across the cluster, grouped by task type """
    def __init__(self, **kwargs):
        self.source = ColumnDataSource(data=dict(
            name=[], left=[], right=[], center=[], color=[],
            percent=[], MB=[], text=[])
        )

        self.root = Plot(
            id='bk-nbytes-plot', x_range=DataRange1d(), y_range=DataRange1d(),
            toolbar_location=None, outline_line_color=None, **kwargs
        )

        self.root.add_glyph(
            self.source,
            Quad(top=1, bottom=0, left='left', right='right',
                 fill_color='color', fill_alpha=1)
        )

        self.root.add_layout(LinearAxis(), 'left')
        self.root.add_layout(LinearAxis(), 'below')

        hover = HoverTool(
            point_policy="follow_mouse",
            tooltips="""
                <div>
                    <span style="font-size: 14px; font-weight: bold;">Name:</span>&nbsp;
                    <span style="font-size: 10px; font-family: Monaco, monospace;">@name</span>
                </div>
                <div>
                    <span style="font-size: 14px; font-weight: bold;">Percent:</span>&nbsp;
                    <span style="font-size: 10px; font-family: Monaco, monospace;">@percent</span>
                </div>
                <div>
                    <span style="font-size: 14px; font-weight: bold;">MB:</span>&nbsp;
                    <span style="font-size: 10px; font-family: Monaco, monospace;">@MB</span>
                </div>
                """
        )
        self.root.add_tools(hover)

    def update(self, messages):
        with log_errors():
            msg = messages['progress']
            if not msg:
                return
            nb = nbytes_bar(msg['nbytes'])
            self.source.data.update(nb)
            self.root.title.text = \
                    "Memory Use: %0.2f MB" % (sum(msg['nbytes'].values()) / 1e6)


class ResourceProfiles(DashboardComponent):
    """ Time plots of the current resource usage on the cluster

    This is two plots, one for CPU and Memory and another for Network I/O
    """
    def __init__(self, **kwargs):
        self.source = ColumnDataSource(data={'time': [], 'cpu': [],
            'memory_percent':[], 'network-send':[], 'network-recv':[]}
        )

        x_range = DataRange1d(follow='end', follow_interval=30000, range_padding=0)

        resource_plot = Plot(
            x_range=x_range, y_range=Range1d(start=0, end=1),
            toolbar_location=None, min_border_bottom=10, **kwargs
        )

        line_opts = dict(line_width=2, line_alpha=0.8)
        g1 = resource_plot.add_glyph(
            self.source,
            Line(x='time', y='memory_percent', line_color="#33a02c", **line_opts)
        )
        g2 = resource_plot.add_glyph(
            self.source,
            Line(x='time', y='cpu', line_color="#1f78b4", **line_opts)
        )

        resource_plot.add_layout(
            LinearAxis(formatter=NumeralTickFormatter(format="0 %")),
            'left'
        )

        legend_opts = dict(
            location='top_left', orientation='horizontal', padding=5, margin=5,
            label_height=5)

        resource_plot.add_layout(
            Legend(items=[('Memory', [g1]), ('CPU', [g2])], **legend_opts)
        )

        network_plot = Plot(
            x_range=x_range, y_range=DataRange1d(start=0),
            toolbar_location=None, **kwargs
        )
        g1 = network_plot.add_glyph(
            self.source,
            Line(x='time', y='network-send', line_color="#a6cee3", **line_opts)
        )
        g2 =network_plot.add_glyph(
            self.source,
            Line(x='time', y='network-recv', line_color="#b2df8a", **line_opts)
        )

        network_plot.add_layout(DatetimeAxis(axis_label="Time"), "below")
        network_plot.add_layout(LinearAxis(axis_label="MB/s"), 'left')
        network_plot.add_layout(
            Legend(items=[('Network Send', [g1]), ('Network Recv', [g2])], **legend_opts)
        )

        tools = [
            PanTool(dimensions='width'), WheelZoomTool(dimensions='width'),
            BoxZoomTool(), ResetTool()
        ]

        if 'sizing_mode' in kwargs:
            sizing_mode = {'sizing_mode': kwargs['sizing_mode']}
        else:
            sizing_mode = {}

        combo_toolbar = ToolbarBox(
            tools=tools, logo=None, toolbar_location='right', **sizing_mode
        )

        self.root = row(
            column(resource_plot, network_plot, **sizing_mode),
            column(combo_toolbar, **sizing_mode),
            id='bk-resource-profiles-plot',
            **sizing_mode
        )

        # Required for update callback
        self.resource_index = [0]

    def update(self, messages):
        with log_errors():
            index = messages['workers']['index']
            data = messages['workers']['plot-data']

            if not index or index[-1] == self.resource_index[0]:
                return

            if self.resource_index == [0]:
                data = valmap(list, data)

            ind = bisect(index, self.resource_index[0])
            indexes = list(range(ind, len(index)))
            data = {k: [v[i] for i in indexes] for k, v in data.items()}
            self.resource_index[0] = index[-1]
            self.source.stream(data, 1000)


class WorkerTable(DashboardComponent):
    """ Status of the current workers

    This is two plots, a text-based table for each host and a thin horizontal
    plot laying out hosts by their current memory use.
    """
    def __init__(self, **kwargs):
        names = ['processes', 'disk-read', 'cores', 'cpu', 'disk-write',
                 'memory', 'last-seen', 'memory_percent', 'host']
        self.source = ColumnDataSource({k: [] for k in names})

        columns = {name: TableColumn(field=name,
                                     title=name.replace('_percent', ' %'))
                   for name in names}

        cnames = ['host', 'cores', 'processes', 'memory', 'cpu', 'memory_percent']

        formatters = {'cpu': NumberFormatter(format='0.0 %'),
                      'memory_percent': NumberFormatter(format='0.0 %'),
                      'memory': NumberFormatter(format='0 b'),
                      'latency': NumberFormatter(format='0.00000'),
                      'last-seen': NumberFormatter(format='0.000'),
                      'disk-read': NumberFormatter(format='0 b'),
                      'disk-write': NumberFormatter(format='0 b'),
                      'net-send': NumberFormatter(format='0 b'),
                      'net-recv': NumberFormatter(format='0 b')}

        table = DataTable(
            source=self.source, columns=[columns[n] for n in cnames],
        )

        for name in cnames:
            if name in formatters:
                table.columns[cnames.index(name)].formatter = formatters[name]

        mem_plot = Plot(
            title=Title(text="Memory Usage (%)"), toolbar_location=None,
            x_range=Range1d(start=0, end=1), y_range=Range1d(start=-0.1, end=0.1),
            **kwargs
        )

        mem_plot.add_glyph(
            self.source,
            Circle(x='memory_percent', y=0, size=10, fill_alpha=0.5)
        )

        mem_plot.add_layout(LinearAxis(), 'below')

        hover = HoverTool(
            point_policy="follow_mouse",
            tooltips="""
                <div>
                  <span style="font-size: 10px; font-family: Monaco, monospace;">@host: </span>
                  <span style="font-size: 10px; font-family: Monaco, monospace;">@memory_percent</span>
                </div>
                """
        )
        mem_plot.add_tools(hover, BoxSelectTool())

        if 'sizing_mode' in kwargs:
            sizing_mode = {'sizing_mode': kwargs['sizing_mode']}
        else:
            sizing_mode = {}

        self.root = column(mem_plot, table, id='bk-worker-table', **sizing_mode)

    def update(self, messages):
        with log_errors():
            try:
                d = messages['workers']['deque'][-1]
            except IndexError:
                return

            workers = sorted(d)

            data = {}
            data['host'] = workers
            for name in ['cores', 'cpu', 'memory_percent', 'latency', 'last-seen',
                         'memory', 'disk-read', 'disk-write', 'net-send',
                         'net-recv']:
                try:
                    if name in ('cpu', 'memory_percent'):
                        data[name] = [d[w][name] / 100 for w in workers]
                    else:
                        data[name] = [d[w][name] for w in workers]
                except KeyError:
                    pass

            data['processing'] = [sorted(d[w]['processing']) for w in workers]
            data['processes'] = [len(d[w]['ports']) for w in workers]
            self.source.data.update(data)


class Processing(DashboardComponent):
    """ Processing and distribution per core

    This shows how many tasks are actively running on each worker and how many
    tasks are enqueued for each worker and how many are in the common pool
    """
    def __init__(self, **kwargs):
        data = self.processing_update({'processing': {}, 'ncores': {}})
        self.source = ColumnDataSource(data)

        x_range = Range1d(-1, 1)
        fig = figure(
            title='Processing and Pending', tools='resize',
             x_range=x_range, id='bk-processing-stacks-plot', **kwargs)
        fig.quad(source=self.source, left=0, right='right', color=Spectral9[0],
                 top='top', bottom='bottom')

        fig.xaxis.minor_tick_line_alpha = 0
        fig.yaxis.visible = False
        fig.ygrid.visible = False

        hover = HoverTool()
        fig.add_tools(hover)
        hover = fig.select(HoverTool)
        hover.tooltips = """
        <div>
            <span style="font-size: 14px; font-weight: bold;">Host:</span>&nbsp;
            <span style="font-size: 10px; font-family: Monaco, monospace;">@name</span>
        </div>
        <div>
            <span style="font-size: 14px; font-weight: bold;">Processing:</span>&nbsp;
            <span style="font-size: 10px; font-family: Monaco, monospace;">@processing</span>
        </div>
        """
        hover.point_policy = 'follow_mouse'

        self.root = fig

    def update(self, messages):
        with log_errors():
            msg = messages['processing']
            if not msg.get('ncores'):
                return
            data = self.processing_update(msg)
            x_range = self.root.x_range
            max_right = max(data['right'])
            cores = max(data['ncores'])
            if x_range.end < max_right:
                x_range.end = max_right + 2
            elif x_range.end > 2 * max_right + cores:  # way out there, walk back
                x_range.end = x_range.end * 0.95 + max_right * 0.05

            self.source.data.update(data)

    @staticmethod
    def processing_update(msg):
        with log_errors():
            names = sorted(msg['processing'])
            names = sorted(names)
            processing = msg['processing']
            processing = [processing[name] for name in names]
            ncores = msg['ncores']
            ncores = [ncores[name] for name in names]
            n = len(names)
            d = {'name': list(names),
                 'processing': processing,
                 'right': list(processing),
                 'top': list(range(n, 0, -1)),
                 'bottom': list(range(n - 1, -1, -1)),
                 'ncores': ncores}

            d['alpha'] = [0.7] * n

            return d
