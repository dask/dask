from __future__ import division, absolute_import

from itertools import cycle
from operator import itemgetter

from toolz import unique, groupby, valmap
import bokeh.plotting as bp
from bokeh.palettes import brewer
from bokeh.models import HoverTool

from ..dot import name


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


def visualize(results, palette='GnBu', file_path="profile.html",
              show=True, **kwargs):
    """Visualize the results of profiling in a bokeh plot.

    Parameters
    ----------
    results : sequence
        Output of profiler.results().
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
    diff = lambda v: v[3] - v[2]
    timings = valmap(lambda val: sum(map(diff, val)), id_group)
    id_lk = {t[0]: n for (n, t) in enumerate(sorted(timings.items(),
                                             key=itemgetter(1), reverse=True))}

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
    data['function'] = funcs = [name(i[0]) for i in tasks]
    data['color'] = get_colors(palette, funcs)

    # Bokeh barfs on quotes in hovers...
    # https://github.com/bokeh/bokeh/issues/2094
    data['key'] = [str(k).replace("'", "") for k in keys]
    source = bp.ColumnDataSource(data=data)

    p.rect(source=source, x='x', y='y', height=1, width='width',
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

    if show:
        bp.show(p)
    return p
