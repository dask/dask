from __future__ import print_function, division, absolute_import

from collections import defaultdict
from itertools import chain
import logging

from toolz import pluck

from ..utils import ignoring, log_errors

logger = logging.getLogger(__file__)

with ignoring(ImportError):
    from bokeh.models import (
        ColumnDataSource, DataRange1d, Range1d, NumeralTickFormatter, ToolbarBox,
    )
    from bokeh.palettes import Spectral9, Viridis4
    from bokeh.models import HoverTool, Paragraph
    from bokeh.models.widgets import DataTable, TableColumn, NumberFormatter
    from bokeh.plotting import figure


def _format_resource_profile_plot(plot):
    plot.legend[0].location = 'top_left'
    plot.legend[0].orientation = 'horizontal'
    plot.legend[0].padding = 5
    plot.legend[0].margin = 5
    plot.legend[0].label_height = 5
    plot.toolbar.logo = None
    plot.yaxis[0].ticker.num_minor_ticks = 2
    plot.yaxis[0].ticker.desired_num_ticks = 2
    return plot


def resource_profile_plot(sizing_mode='fixed', **kwargs):
    names = ['time', 'cpu', 'memory_percent', 'network-send', 'network-recv']
    source = ColumnDataSource({k: [] for k in names})

    plot_props = dict(
        tools='xpan,xwheel_zoom,box_zoom,reset',
        toolbar_location=None,  # Because we're making a joint toolbar
        sizing_mode=sizing_mode,
        x_axis_type='datetime',
        x_range=DataRange1d(follow='end', follow_interval=30000, range_padding=0)
    )
    plot_props.update(kwargs)
    line_props = dict(x='time', line_width=2, line_alpha=0.8, source=source)

    y_range = Range1d(0, 1)

    p1 = figure(title=None, y_range=y_range, id='bk-resource-profile-plot',
                **plot_props)
    p1.line(y='memory_percent', color="#33a02c", legend='Memory', **line_props)
    p1.line(y='cpu', color="#1f78b4", legend='CPU', **line_props)
    p1 = _format_resource_profile_plot(p1)
    p1.yaxis.bounds = (0, 100)
    p1.yaxis.formatter = NumeralTickFormatter(format="0 %")
    p1.xaxis.visible = None
    p1.min_border_bottom = 10

    y_range = DataRange1d(start=0)
    p2 = figure(title=None, y_range=y_range, id='bk-network-profile-plot',
                **plot_props)
    p2.line(y='network-send', color="#a6cee3", legend='Network Send', **line_props)
    p2.line(y='network-recv', color="#b2df8a", legend='Network Recv', **line_props)
    p2 = _format_resource_profile_plot(p2)
    p2.yaxis.axis_label = "MB/s"

    from bokeh.models import DatetimeAxis
    for r in list(p1.renderers):
        if isinstance(r, DatetimeAxis):
            p1.renderers.remove(r)

    all_tools = p1.toolbar.tools + p2.toolbar.tools
    combo_toolbar = ToolbarBox(
        tools=all_tools, sizing_mode=sizing_mode, logo=None, toolbar_location='right'
    )
    return source, p1, p2, combo_toolbar


def resource_profile_update(source, worker_buffer, times_buffer):
    data = defaultdict(list)

    workers = sorted(list(set(chain(*list(w.keys() for w in worker_buffer)))))

    for name in ['cpu', 'memory_percent', 'network-send', 'network-recv']:
        data[name] = [[msg[w][name] if w in msg and name in msg[w] else 'null'
                       for msg in worker_buffer]
                       for w in workers]

    data['workers'] = workers
    data['times'] = [[t * 1000 if w in worker_buffer[i] else 'null'
                      for i, t in enumerate(times_buffer)]
                      for w in workers]

    source.data.update(data)


def resource_append(lists, msg):
    L = list(msg.values())
    if not L:
        return
    try:
        for k in ['cpu', 'memory_percent']:
            lists[k].append(mean(pluck(k, L)) / 100)
    except KeyError as e:  # initial messages sometimes lack resource data
        logger.exception(e)
        return        # this is safe to skip

    lists['time'].append(mean(pluck('time', L)) * 1000)
    if len(lists['time']) >= 2:
        t1, t2 = lists['time'][-2], lists['time'][-1]
        interval = (t2 - t1) / 1000
    else:
        interval = 0.5
    send = mean(pluck('network-send', L, 0))
    lists['network-send'].append(send / 2**20 / (interval or 0.5))
    recv = mean(pluck('network-recv', L, 0))
    lists['network-recv'].append(recv / 2**20 / (interval or 0.5))


def mean(seq):
    seq = list(seq)
    return sum(seq) / len(seq)


def worker_table_plot(**kwargs):
    """ Column data source and plot for host table """
    with log_errors():
        # names = ['host', 'cpu', 'memory_percent', 'memory', 'cores', 'processes',
        #          'processing', 'latency', 'last-seen', 'disk-read', 'disk-write',
        #          'network-send', 'network-recv']
        names = ['processes', 'disk-read', 'cores', 'cpu', 'disk-write',
                 'memory', 'last-seen', 'memory_percent', 'host']
        source = ColumnDataSource({k: [] for k in names})

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

        table = DataTable(source=source, columns=[columns[n] for n in cnames],
                          id='bk-worker-table', **kwargs)
        for name in cnames:
            if name in formatters:
                table.columns[cnames.index(name)].formatter = formatters[name]

        x_range = Range1d(0, 1)
        y_range = Range1d(-.1, .1)
        mem_plot = figure(title="Memory Usage (%)", tools='box_select',
                          height=90, width=600, x_range=x_range, y_range=y_range,
                          toolbar_location=None, id='bk-memory-usage-plot')
        mem_plot.circle(source=source, x='memory_percent', y=0, size=10,
                        alpha=0.5)
        mem_plot.yaxis.visible = False
        mem_plot.xaxis.minor_tick_line_width = 0
        mem_plot.ygrid.visible = False
        mem_plot.xaxis.minor_tick_line_alpha = 0

        hover = HoverTool()
        mem_plot.add_tools(hover)
        hover = mem_plot.select(HoverTool)
        hover.tooltips = """
        <div>
          <span style="font-size: 10px; font-family: Monaco, monospace;">@host: </span>
          <span style="font-size: 10px; font-family: Monaco, monospace;">@memory_percent</span>
        </div>
        """
        hover.point_policy = 'follow_mouse'

    return source, [mem_plot, table]


def worker_table_update(source, d):
    """ Update host table source """
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
    source.data.update(data)


def processing_plot(**kwargs):
    data = processing_update({'processing': {}, 'stacks': {}, 'ncores': {},
        'ready': 0})
    source = ColumnDataSource(data)

    x_range = Range1d(-1, 1)
    fig = figure(title='Processing and Pending', tools='resize',
                 x_range=x_range, id='bk-processing-plot', **kwargs)
    fig.quad(source=source, left=0, right='right', color=Spectral9[0],
             top='top', bottom='bottom')
    fig.quad(source=source, left='left', right=0, color=Spectral9[1],
             top='top', bottom='bottom', alpha='alpha')

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
        <span style="font-size: 14px; font-weight: bold;">Stacks:</span>&nbsp;
        <span style="font-size: 10px; font-family: Monaco, monospace;">@stacks</span>
    </div>
    <div>
        <span style="font-size: 14px; font-weight: bold;">Processing:</span>&nbsp;
        <span style="font-size: 10px; font-family: Monaco, monospace;">@processing</span>
    </div>
    """
    hover.point_policy = 'follow_mouse'
    return source, fig


def processing_update(msg):
    names = sorted(msg['stacks'])
    stacks = msg['stacks']
    stacks = [stacks[name] for name in names]
    names = sorted(names)
    processing = msg['processing']
    processing = [processing[name] for name in names]
    ncores = msg['ncores']
    ncores = [ncores[name] for name in names]
    n = len(names)
    d = {'name': list(names),
         'processing': processing,
         'stacks': list(stacks),
         'right': list(processing),
         'left': [-s for s in stacks],
         'top': list(range(n, 0, -1)),
         'bottom': list(range(n - 1, -1, -1)),
         'ncores': ncores}

    d['name'].append('ready')
    d['processing'].append(0)
    d['stacks'].append(msg['ready'])
    d['left'].append(-msg['ready'] / n if n else 0)
    d['right'].append(0)
    d['top'].append(n)
    d['bottom'].append(0)
    d['ncores'].append(sum(msg['ncores'].values()))

    d['alpha'] = [0.7] * n + [0.2]

    return d
