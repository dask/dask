""" This module contains utility functions to construct and manipulate counting
data structures for frames.

When performing statistical profiling we obtain many call stacks.  We aggregate
these call stacks into data structures that maintain counts of how many times
each function in that call stack has been called.  Because these stacks will
overlap this aggregation counting structure forms a tree, such as is commonly
visualized by profiling tools.

We represent this tree as a nested dictionary with the following form:

    {
     'identifier': 'root',
     'description': 'A long description of the line of code being run.',
     'count': 10  # the number of times we have seen this line
     'children': {  # callers of this line. Recursive dicts
         'ident-b': {'description': ...
                   'identifier': 'ident-a',
                   'count': ...
                   'children': {...}},
         'ident-b': {'description': ...
                   'identifier': 'ident-b',
                   'count': ...
                   'children': {...}}}
    }
"""


from collections import defaultdict
import linecache
import itertools
import toolz

from .utils import format_time


def identifier(frame):
    """ A string identifier from a frame

    Strings are cheaper to use as indexes into dicts than tuples or dicts
    """
    if frame is None:
        return 'None'
    else:
        return ';'.join((frame.f_code.co_name,
                         frame.f_code.co_filename,
                         str(frame.f_code.co_firstlineno)))


def repr_frame(frame):
    """ Render a frame as a line for inclusion into a text traceback """
    co = frame.f_code
    text = '  File "%s", line %s, in %s' % (co.co_filename,
                                            frame.f_lineno,
                                            co.co_name)
    line = linecache.getline(co.co_filename, frame.f_lineno, frame.f_globals).lstrip()
    return text + '\n\t' + line


def info_frame(frame):
    co = frame.f_code
    line = linecache.getline(co.co_filename, frame.f_lineno, frame.f_globals).lstrip()
    return {'filename': co.co_filename,
            'name': co.co_name,
            'line_number': frame.f_lineno,
            'line': line}


def process(frame, child, state, stop=None):
    """ Add counts from a frame stack onto existing state

    This recursively adds counts to the existing state dictionary and creates
    new entries for new functions.

    Example
    -------
    >>> import sys, threading
    >>> ident = threading.get_ident()  # replace with your thread of interest
    >>> frame = sys._current_frames()[ident]
    >>> state = {'children': {}, 'count': 0, 'description': 'root',
    ...          'identifier': 'root'}
    >>> process(frame, None, state)
    >>> state
    {'count': 1,
     'identifier': 'root',
     'description': 'root',
     'children': {'...'}}
    """
    if frame.f_back is not None and (stop is None or not frame.f_back.f_code.co_filename.endswith(stop)):
        state = process(frame.f_back, frame, state, stop=stop)

    ident = identifier(frame)

    try:
        d = state['children'][ident]
    except KeyError:
        d = {'count': 0,
             'description': info_frame(frame),
             'children': {},
             'identifier': ident}
        state['children'][ident] = d

    state['count'] += 1

    if child is not None:
        return d
    else:
        d['count'] += 1


def merge(*args):
    """ Merge multiple frame states together """
    if not args:
        return create()
    s = {arg['identifier'] for arg in args}
    if len(s) != 1:
        raise ValueError("Expected identifiers, got %s" % str(s))
    children = defaultdict(list)
    for arg in args:
        for child in arg['children']:
            children[child].append(arg['children'][child])

    children = {k: merge(*v) for k, v in children.items()}
    count = sum(arg['count'] for arg in args)
    return {'description': args[0]['description'],
            'children': dict(children),
            'count': count,
            'identifier': args[0]['identifier']}


def create():
    return {'count': 0, 'children': {}, 'identifier': 'root', 'description':
            {'filename': '', 'name': '', 'line_number': 0, 'line': ''}}


def call_stack(frame):
    """ Create a call text stack from a frame

    Returns
    -------
    list of strings
    """
    L = []
    while frame:
        L.append(repr_frame(frame))
        frame = frame.f_back
    return L[::-1]


def plot_data(state, profile_interval=0.010):
    """ Convert a profile state into data useful by Bokeh

    See Also
    --------
    distributed.bokeh.components.ProfilePlot
    """
    starts = []
    stops = []
    heights = []
    widths = []
    colors = []
    states = []
    times = []

    filenames = []
    lines = []
    line_numbers = []
    names = []

    def traverse(state, start, stop, height):
        if not state['count']:
            return
        starts.append(start)
        stops.append(stop)
        heights.append(height)
        width = stop - start
        widths.append(width)
        states.append(state)
        times.append(format_time(state['count'] * profile_interval))

        desc = state['description']
        filenames.append(desc['filename'])
        lines.append(desc['line'])
        line_numbers.append(desc['line_number'])
        names.append(desc['name'])

        ident = state['identifier']

        try:
            colors.append(color_of(desc['filename']))
        except IndexError:
            colors.append(palette[-1])

        delta = (stop - start) / state['count']

        x = start

        for name, child in state['children'].items():
            width = child['count'] * delta
            traverse(child, x, x + width, height + 1)
            x += width

    traverse(state, 0, 1, 0)
    return {'left': starts,
            'right': stops,
            'bottom': heights,
            'width': widths,
            'top': [x + 1 for x in heights],
            'color': colors,
            'states': states,
            'filename': filenames,
            'line': lines,
            'line_number': line_numbers,
            'name': names,
            'time': times}


try:
    from bokeh.palettes import viridis
except ImportError:
    palette = ['red', 'green', 'blue', 'yellow']
else:
    palette = viridis(10)

counter = itertools.count()


@toolz.memoize
def color_of(x):
    return palette[next(counter) % len(palette)]
