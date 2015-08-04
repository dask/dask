from __future__ import division, absolute_import

from itertools import cycle
from operator import itemgetter

from toolz import unique, groupby
import bokeh.plotting as bp
from bokeh.palettes import brewer
from bokeh.models import HoverTool

from ..dot import funcname
from ..core import istask


def pprint_task(task, keys):
    """Return a nicely formatted string for a task.

    Examples
    --------
    >>> from operator import add, mul
    >>> dsk = {'a': 1,
    ...        'b': 2,
    ...        'c': (add, 'a', 'b'),
    ...        'd': (add, (mul, 'a', 'b'), 'c'),
    ...        'e': (sum, ['a', 'b', 5])}

    >>> pprint_task(dsk['c'], dsk)
    'add(_, _)'
    >>> pprint_task(dsk['d'], dsk)
    'add(mul(_, _), _)'
    >>> pprint_task(dsk['e'], dsk)
    'sum([_, _, *])'
    """
    if istask(task):
        func = task[0]
        if hasattr(func, 'funcs'):
            head = '('.join(funcname(f) for f in func.funcs)
            tail = ')'*len(func.funcs)
        else:
            head = funcname(task[0])
            tail = ')'
        args = ', '.join(pprint_task(t, keys) for t in task[1:])
        return '{0}({1}{2}'.format(head, args, tail)
    elif isinstance(task, list):
        args = ', '.join(pprint_task(t, keys) for t in task)
        return '[{0}]'.format(args)
    else:
        try:
            if task in keys:
                return '_'
        except TypeError:
            pass
        return '*'


def get_colors(palette, funcs):
    """Get a dict mapping funcs to colors from palette.

    Parameters
    ----------
    palette : string
        Name of the palette. Must be a key in bokeh.palettes.brewer
    funcs : iterable
        Iterable of function names
    """
    unique_funcs = list(sorted(unique(funcs)))
    n_funcs = len(unique_funcs)
    palette_lookup = brewer[palette]
    keys = list(palette_lookup.keys())
    low, high = min(keys), max(keys)
    if n_funcs > high:
        colors = cycle(palette_lookup[high])
    elif n_funcs < low:
        colors = palette_lookup[low]
    else:
        colors = palette_lookup[n_funcs]
    color_lookup = dict(zip(unique_funcs, colors))
    return [color_lookup[n] for n in funcs]


def visualize(results, dsk, palette='GnBu', file_path="profile.html",
              show=True, **kwargs):
    """Visualize the results of profiling in a bokeh plot.

    Parameters
    ----------
    results : sequence
        Output of profiler.results().
    dsk : dict
        The dask graph being profiled.
    palette : string, optional
        Name of the bokeh palette to use, must be key in bokeh.palettes.brewer.
    file_path : string, optional
        Name of the plot output file.
    show : boolean, optional
        If True (default), the plot is opened in a browser.
    **kwargs
        Other keyword arguments, passed to bokeh.figure. These will override
        all defaults set by visualize.

    Returns
    -------
    The completed bokeh plot object.
    """

    bp.output_file(file_path)
    keys, tasks, starts, ends, ids = zip(*results)

    id_group = groupby(itemgetter(4), results)
    timings = dict((k, [i.end_time - i.start_time for i in v]) for (k, v) in
                   id_group.items())
    id_lk = dict((t[0], n) for (n, t) in enumerate(sorted(timings.items(),
                 key=itemgetter(1), reverse=True)))

    left = min(starts)
    right = max(ends)

    defaults = dict(title="Profile Results",
                    tools="hover,save,reset,resize,xwheel_zoom,xpan",
                    plot_width=800, plot_height=400)
    defaults.update(kwargs)

    p = bp.figure(y_range=[str(i) for i in range(len(id_lk))],
                  x_range=[0, right - left], **defaults)

    data = {}
    data['width'] = width = [e - s for (s, e) in zip(starts, ends)]
    data['x'] = [w/2 + s - left for (w, s) in zip(width, starts)]
    data['y'] = [id_lk[i] + 1 for i in ids]
    data['function'] = funcs = [pprint_task(i, dsk) for i in tasks]
    data['color'] = get_colors(palette, funcs)
    data['key'] = [str(i) for i in keys]

    source = bp.ColumnDataSource(data=data)

    p.rect(source=source, x='x', y='y', height=1, width='width',
           color='color', line_color='gray')
    p.grid.grid_line_color = None
    p.axis.axis_line_color = None
    p.axis.major_tick_line_color = None
    p.yaxis.axis_label = "Worker ID"
    p.xaxis.axis_label = "Time (s)"

    hover = p.select(HoverTool)
    hover.tooltips = """
    <div>
        <span style="font-size: 14px; font-weight: bold;">Key:</span>&nbsp;
        <span style="font-size: 10px; font-family: Monaco, monospace;">@key</span>
    </div>
    <div>
        <span style="font-size: 14px; font-weight: bold;">Task:</span>&nbsp;
        <span style="font-size: 10px; font-family: Monaco, monospace;">@function</span>
    </div>
    """
    hover.point_policy = 'follow_mouse'

    if show:
        bp.show(p)
    return p
