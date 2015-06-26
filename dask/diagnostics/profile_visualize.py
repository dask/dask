from bokeh.plotting import output_file, show, figure, ColumnDataSource
from bokeh.palettes import brewer
from bokeh.models import HoverTool
from toolz import unique, groupby, valmap
from itertools import cycle
from operator import itemgetter


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
