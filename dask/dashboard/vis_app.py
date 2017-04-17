from __future__ import division, absolute_import

from operator import attrgetter, itemgetter
from threading import Thread
from time import sleep

from toolz import groupby, get, concat
from bokeh.models import Rect, Text, Circle, Bezier, ColumnDataSource
from bokeh.plotting import Figure
from bokeh.session import Session
from bokeh.document import Document
from bokeh.browserlib import view

from ..compatibility import Queue
from ..core import istask
from .graphparse import dask_to_graph
from .to_pydot import name


def node_data(g, font_size):
    data, funcs = get(['box', 'circle'], groupby(attrgetter('shape'), g.nodes))

    data_keys = ['name', 'x', 'y', 'height', 'width', 'label', 'color']
    func_keys = ['name', 'x', 'y', 'height', 'label', 'color']

    data_ls = sorted(map(attrgetter(*data_keys), data), key=itemgetter(0))
    func_ls = sorted(map(attrgetter(*func_keys), funcs), key=itemgetter(0))

    data_lk = dict(zip(data_keys, zip(*data_ls)))
    func_lk = dict(zip(func_keys, zip(*func_ls)))
    func_lk['height'] = tuple(i/2 for i in func_lk['height'])

    return data_lk, func_lk


def edge_data(g):
    cpoints = map(attrgetter('cpoints'), g.edges)
    data = map(lambda a: list(concat(a)), zip(*cpoints))
    keys = ['x0', 'y0', 'cx0', 'cy0', 'cx1', 'cy1', 'x1', 'y1']
    return dict(zip(keys, data))


def color_nodes(dsk, state):
    data = dict.fromkeys(map(name, dsk), 'black')
    f_keys = [k for (k, v) in dsk.items() if istask(v)]
    func = dict.fromkeys(map(lambda a: name((a, 'function')), f_keys), 'black')

    for key in state['released']:
        data[name(key)] = 'blue'

    for key in state['cache']:
        data[name(key)] = 'red'

    for key in state['finished']:
        func[name((key, 'function'))] = 'blue'
    for key in state['running']:
        func[name((key, 'function'))] = 'red'
    data_colors = get(sorted(data), data)
    func_colors = get(sorted(func), func)

    return dsk, {'color': data_colors}, {'color': func_colors}


class Dashboard(object):
    def __init__(self, get, pause=0, screen_width=1200, screen_height=800,
                 border=30):
        # Get function to wrap
        self._get = get
        self._dsk = {}

        # Time to pause for on each timestep
        self.pause = pause

        # Create document and session for this app
        self.document = Document()
        self.session = Session()
        self.session.use_doc()
        self.session.load_document(self.document)

        # Queue for the callback returns
        self._queue = Queue()

        # Handler to handle the callbacks
        self._handler = _Handler(self)

        # Data stores describing the visualization
        self.data_source = ColumnDataSource(data=dict())
        self.func_source = ColumnDataSource(data=dict())
        self.edge_source = ColumnDataSource(data=dict())

        # Vis Constants
        self.screen_width = screen_width
        self.screen_height = screen_height
        self.border = border

        # Initialize the glyphs
        self.init_glyphs()

        # Create an empty plot to remove lasting effects from previous session
        self.document.clear()
        plot = Figure(title=None, x_axis_type=None, y_axis_type=None,
                      x_range=[0, 10], y_range=[0, 10],
                      plot_width=self.screen_width,
                      plot_height=self.screen_height)

        self.document.add(plot)
        self.session.store_document(self.document)

        # Start the visualizer
        link = self.session.object_link(self.document.context)
        view(link)
        self._handler.start()

    def init_glyphs(self):
        # Data
        self.data_glyphs = Rect(x='x', y='y', height='height', width='width',
                                line_color='color', fill_color=None,
                                height_units='data', width_units='data')

        # Functions
        self.func_glyphs = Circle(x='x', y='y', radius='height',
                                  line_color='color', fill_color=None,
                                  radius_units='data')

        # Edges
        self.edge_glyphs = Bezier(x0='x0', y0='y0', cx0='cx0', cy0='cy0',
                                  cx1='cx1', cy1='cy1', x1='x1', y1='y1')

    def visualize(self, dsk):
        self._dsk = dsk
        g = dask_to_graph(dsk)

        x_center = g.width/2
        y_center = g.height/2
        h2w = self.screen_width/self.screen_height

        if g.width > g.height*h2w:
            scale = self.screen_width/(self.screen_width - 2*self.border)
            req_width = g.width * scale
            req_height = req_width/h2w
            text_scale = 12/req_width
        else:
            scale = self.screen_height/(self.screen_height - 2*self.border)
            req_width = g.height*h2w * scale
            req_height = req_width/h2w
            text_scale = 8/req_height

        left = x_center - req_width/2
        right = x_center + req_width/2
        top = y_center + req_height/2
        bottom = y_center - req_height/2
        font_size = str(round(text_scale, 2)) + 'em'

        # Plot
        self.plot = Figure(title=None, x_axis_type=None, y_axis_type=None,
                           x_range=[left, right], y_range=[bottom, top],
                           plot_width=self.screen_width,
                           plot_height=self.screen_height)

        # Font size can't be linked to data... :(
        func_text = Text(x='x', y='y', text='label', text_align='center',
                         text_baseline='middle', text_font_size=font_size)
        data_text = Text(x='x', y='y', text='label', text_align='center',
                         text_baseline='middle', text_font_size=font_size)

        # Add glyphs
        self.plot.add_glyph(self.data_source, self.data_glyphs)
        self.plot.add_glyph(self.data_source, data_text)
        self.plot.add_glyph(self.func_source, self.func_glyphs)
        self.plot.add_glyph(self.func_source, func_text)
        self.plot.add_glyph(self.edge_source, self.edge_glyphs)

        # Update the data sources
        edge_lk = edge_data(g)
        data_lk, func_lk = node_data(g, font_size)

        self.data_source.data.update(data_lk)
        self.func_source.data.update(func_lk)
        self.edge_source.data.update(edge_lk)

        self.document.clear()
        self.document.add(self.plot)
        self.session.store_document(self.document)
        self.session.store_objects(self.data_source, self.func_source,
                                   self.edge_source)

    def update_dask(self, dsk, data_lk, func_lk):
        if self._dsk is not dsk:
            self.visualize(dsk)
        self.data_source.data.update(data_lk)
        self.func_source.data.update(func_lk)
        self.session.store_objects(self.data_source, self.func_source)

    def scheduler_callback(self, key, dsk, state):
        sleep(self.pause)
        self._queue.put(color_nodes(dsk, state))

    def get(self, dsk, result, cache=None, **kwargs):
        return self._get(dsk, result, cache=cache,
                         scheduler_callback=self.scheduler_callback)


class _Handler(Thread):
    def __init__(self, vis):
        super(_Handler, self).__init__()
        self.queue = vis._queue
        self.update_dask = vis.update_dask
        self.daemon = True

    def run(self):
        while True:
            task = self.queue.get()
            self.update_dask(*task)
