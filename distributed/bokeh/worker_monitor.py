from __future__ import print_function, division, absolute_import

import logging

from toolz import pluck

from ..utils import ignoring

logger = logging.getLogger(__name__)

with ignoring(ImportError):
    from bokeh.models import (
        ColumnDataSource, DataRange1d, Range1d, NumeralTickFormatter, ToolbarBox,
    )
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
