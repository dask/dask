from __future__ import print_function, division, absolute_import

from collections import defaultdict
from itertools import chain

from toolz import pluck

from ..utils import ignoring

with ignoring(ImportError):
    from bokeh.models import (ColumnDataSource, DataRange1d, Range1d,
            NumeralTickFormatter, LinearAxis)
    from bokeh.palettes import Spectral9
    from bokeh.plotting import figure


def resource_profile_plot(width=600, height=300):
    names = ['time', 'cpu', 'memory-percent', 'network-send']
    source = ColumnDataSource({k: [] for k in names})

    x_range = DataRange1d(follow='end', follow_interval=30000, range_padding=0)
    y_range = Range1d(0, 1)
    p = figure(width=width, height=height, x_axis_type='datetime',
               responsive=True, tools='xpan,xwheel_zoom,box_zoom,resize,reset',
               x_range=x_range, y_range=y_range)
    p.line(x='time', y='memory-percent', line_width=2, line_alpha=0.8,
           color=Spectral9[7], legend='Avg Memory Usage', source=source)
    p.line(x='time', y='cpu', line_width=2, line_alpha=0.8,
           color=Spectral9[0], legend='Avg CPU Usage', source=source)
    p.legend[0].location = 'top_left'
    p.yaxis[0].formatter = NumeralTickFormatter(format="0 %")
    p.min_border_right = 10
    p.extra_y_ranges = {"send": DataRange1d(bounds=(0, None))}
    p.add_layout(LinearAxis(y_range_name="send", axis_label="Throughput (MB/s)"),
                 'right')
    p.yaxis.axis_label_text_font_size = "10pt"

    p.line(x='time', y='network-send', line_width=2, line_alpha=0.8,
           color=Spectral9[2], legend='Network', source=source,
           y_range_name="send")

    return source, p


def resource_profile_update(source, worker_buffer, times_buffer):
    data = defaultdict(list)

    workers = sorted(list(set(chain(*list(w.keys() for w in worker_buffer)))))

    for name in ['cpu', 'memory-percent', 'network-send']:
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
    for k in ['cpu', 'memory-percent']:
        lists[k].append(mean(pluck(k, L)) / 100)

    lists['time'].append(mean(pluck('time', L)) * 1000)
    net = mean(pluck('network-send', L, 0))
    if len(lists['time']) >= 2:
        t1, t2 = lists['time'][-2], lists['time'][-1]
        interval = (t2 - t1) / 1000
    else:
        interval = 0.5
    lists['network-send'].append(net / 2**20 / (interval or 0.5))


def mean(seq):
    seq = list(seq)
    return sum(seq) / len(seq)
