from __future__ import absolute_import

from collections import OrderedDict
from timeit import default_timer

from toolz import unique, groupby, valmap
from itertools import cycle
from operator import itemgetter

import dask.array as da
from dask.array.core import get
import numpy as np
from dask.dot import name

from bokeh.plotting import output_file, show, figure, ColumnDataSource
from bokeh.palettes import brewer
from bokeh.models import HoverTool

class Profiler(object):
    """A profiler for dask execution at the task level.

    Records the following information for each task:
        1. Key
        2. Task
        3. Start time in seconds since the epoch
        4. Finish time in seconds since the epoch
        5. Worker id
    """
    def __init__(self, get):
        """Create a profiler

        Parameters
        ----------
        get : callable
            The scheduler get function to profile.
        """
        self._get = get
        self._results = {}

    def _start_callback(self, key, dask, state):
        if key is not None:
            start = default_timer()
            self._results[key] = (key, dask[key], start)

    def _end_callback(self, key, dask, state, id):
        if key is not None:
            end = default_timer()
            self._results[key] += (end, id)

    def get(self, dsk, result, **kwargs):
        """Profiled get function.

        Note that this clears the results from the last run before executing
        the dask."""

        self._results = {}
        return self._get(dsk, result, start_callback=self._start_callback,
                         end_callback=self._end_callback, **kwargs)

    def results(self):
        """Returns a list containing tuples of:

        (key, task, start time, end time, worker_id)"""

        return list(self._results.values())

    def visualize(self):
        return visualize(self.results())


def get_colors(palette, names):
    unique_names = list(sorted(unique(names)))
    n_names = len(unique_names)
    palette_lookup = brewer[palette]
    keys = list(palette_lookup.keys())
    low, high = min(keys), max(keys)
    if n_names > high:
        colors = cycle(palette_lookup[high])
    elif n_names < low:
        colors = palette_lookup[low]
    else:
        colors = palette_lookup[n_names]
    color_lookup = dict(zip(unique_names, colors))
    return [color_lookup[n] for n in names]

label = lambda a: name(a[0])

def visualize(results, palette='GnBu', file_path="profile.html"):
    output_file(file_path)

    key, task, start, end, id = zip(*results)

    id_group = groupby(itemgetter(4), results)
    diff = lambda v: v[3] - v[2]
    f = lambda val: sum(map(diff, val))
    total_id = [i[0] for i in reversed(sorted(valmap(f, id_group).items(), key=itemgetter(1)))]
    

    name = map(label, task)

    left = min(start)
    right = max(end)

    p = figure(title="Profile Results", y_range=map(str, range(len(total_id))),
            x_range=[0, right - left],
            plot_width=1200, plot_height=800, tools="hover,save,reset,xwheel_zoom,xpan")

    data = {}
    data['x'] = [(e - s)/2 + s - left for (s, e) in zip(start, end)]
    data['y'] = [total_id.index(i) + 1 for i in id]
    data['height'] = [1 for i in id]
    data['width'] = [e - s for (s, e) in zip(start, end)]
    data['color'] = get_colors(palette, name)

    f = lambda a: str(a).replace("'", "") # Bokeh barfs on quotes in hovers...
    data['key'] = map(f, key)
    data['function'] = name
    source = ColumnDataSource(data=data)

    p.rect(source=source, x='x', y='y', height='height', width='width',
            color='color', line_color='gray')
    p.grid.grid_line_color = None
    p.axis.axis_line_color = None
    p.axis.major_tick_line_color = None
    p.yaxis.axis_label = "Worker ID"
    p.xaxis.axis_label = "Time (s)"

    hover = p.select(type=HoverTool)
    hover.tooltips = [("Key:", "@key"),
                      ("Function:", "@function")]

    hover.point_policy = 'follow_mouse'

    show(p)
    return p
