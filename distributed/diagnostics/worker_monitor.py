from collections import defaultdict

from ..utils import ignoring

with ignoring(ImportError):
    from bokeh.plotting import figure
    from bokeh.models import ColumnDataSource, Range1d

def resource_profile_plot(width=600, height=400):
    names = ['times','workers', 'cpu', 'memory-percent']
    source = ColumnDataSource({k: [] for k in names})

    p = figure(width=width, height=height, x_axis_type='datetime')
    p.multi_line(xs='times', ys='cpu', line_width=3, color='red', source=source)
    p.multi_line(xs='times', ys='memory-percent', line_width=3, color='blue', source=source)

    return source, p

def resource_profile_update(source, worker_buffer, times_buffer):
    data = defaultdict(list)

    workers = sorted(worker_buffer[-1], reverse=True)

    import pdb; pdb.set_trace()

    for name in ['cpu', 'memory-percent']:
        try:
            data[name] = [[msg[w][name] for msg in worker_buffer if msg != {} and w in msg] for w in workers]
        except KeyError:
            pass

    data['workers'] = workers
    data['times'] = [[t for i, t in enumerate(times_buffer) if worker_buffer[i] != {}] for w in workers]

    source.data.update(data)
