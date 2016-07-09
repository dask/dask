from __future__ import print_function, division, absolute_import

from collections import defaultdict
from itertools import chain

from toolz import pluck

from ..utils import ignoring

with ignoring(ImportError):
    from bokeh.models import (
        ColumnDataSource, DataRange1d, Range1d, NumeralTickFormatter, ToolbarBox,
    )
    from bokeh.palettes import Spectral9
    from bokeh.plotting import figure


def _format_resource_profile_plot(plot):
    plot.legend[0].location = 'top_left'
    plot.legend[0].orientation = 'horizontal'
    plot.legend[0].legend_padding = 5
    plot.legend[0].legend_margin = 5
    plot.legend[0].label_height = 5
    plot.toolbar.logo = None
    plot.yaxis[0].ticker.num_minor_ticks = 2
    plot.yaxis[0].ticker.desired_num_ticks = 2
    return plot


def resource_profile_plot(sizing_mode='fixed'):
    names = ['time', 'cpu', 'memory-percent', 'network-send', 'network-recv']
    source = ColumnDataSource({k: [] for k in names})

    plot_props = dict(
        tools='xpan,xwheel_zoom,box_zoom,reset',
        toolbar_location=None,  # Becaues we're making a joing toolbar
        sizing_mode=sizing_mode,
        x_axis_type='datetime',
        x_range=DataRange1d(follow='end', follow_interval=30000, range_padding=0)
    )
    line_props = dict(x='time', line_width=2, line_alpha=0.8, source=source)

    y_range = Range1d(0, 1)

    p1 = figure(title="Resource usage (%)", y_range=y_range, **plot_props)
    p1.line(y='memory-percent', color=Spectral9[7], legend='Memory', **line_props)
    p1.line(y='cpu', color=Spectral9[0], legend='CPU', **line_props)
    p1 = _format_resource_profile_plot(p1)
    p1.yaxis.bounds = (0, 100)
    p1.yaxis.formatter = NumeralTickFormatter(format="0 %")

    y_range = DataRange1d(start=0)
    p2 = figure(title="Throughput (MB/s)", y_range=y_range, **plot_props)
    p2.line(y='network-send', color=Spectral9[2], legend='Network Send', **line_props)
    p2.line(y='network-recv', color=Spectral9[3], legend='Network Recv', **line_props)
    p2 = _format_resource_profile_plot(p2)

    all_tools = p1.toolbar.tools + p2.toolbar.tools
    combo_toolbar = ToolbarBox(
        tools=all_tools, sizing_mode=sizing_mode, logo=None, toolbar_location='right'
    )
    return source, p1, p2, combo_toolbar


def resource_profile_update(source, worker_buffer, times_buffer):
    data = defaultdict(list)

    workers = sorted(list(set(chain(*list(w.keys() for w in worker_buffer)))))

    for name in ['cpu', 'memory-percent', 'network-send', 'network-recv']:
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
        for k in ['cpu', 'memory-percent']:
            lists[k].append(mean(pluck(k, L)) / 100)
    except KeyError:  # initial messages sometimes lack resource data
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
