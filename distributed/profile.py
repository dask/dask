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
from __future__ import print_function, division, absolute_import

import bisect
from collections import defaultdict, deque
import linecache
import sys
import threading
from time import sleep

import toolz

from .metrics import time
from .utils import format_time, color_of, parse_timedelta
from .compatibility import get_thread_identity


def identifier(frame):
    """ A string identifier from a frame

    Strings are cheaper to use as indexes into dicts than tuples or dicts
    """
    if frame is None:
        return "None"
    else:
        return ";".join(
            (
                frame.f_code.co_name,
                frame.f_code.co_filename,
                str(frame.f_code.co_firstlineno),
            )
        )


def repr_frame(frame):
    """ Render a frame as a line for inclusion into a text traceback """
    co = frame.f_code
    text = '  File "%s", line %s, in %s' % (co.co_filename, frame.f_lineno, co.co_name)
    line = linecache.getline(co.co_filename, frame.f_lineno, frame.f_globals).lstrip()
    return text + "\n\t" + line


def info_frame(frame):
    co = frame.f_code
    line = linecache.getline(co.co_filename, frame.f_lineno, frame.f_globals).lstrip()
    return {
        "filename": co.co_filename,
        "name": co.co_name,
        "line_number": frame.f_lineno,
        "line": line,
    }


def process(frame, child, state, stop=None, omit=None):
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
    if omit is not None and any(frame.f_code.co_filename.endswith(o) for o in omit):
        return False

    prev = frame.f_back
    if prev is not None and (
        stop is None or not prev.f_code.co_filename.endswith(stop)
    ):
        state = process(prev, frame, state, stop=stop)
        if state is False:
            return False

    ident = identifier(frame)

    try:
        d = state["children"][ident]
    except KeyError:
        d = {
            "count": 0,
            "description": info_frame(frame),
            "children": {},
            "identifier": ident,
        }
        state["children"][ident] = d

    state["count"] += 1

    if child is not None:
        return d
    else:
        d["count"] += 1


def merge(*args):
    """ Merge multiple frame states together """
    if not args:
        return create()
    s = {arg["identifier"] for arg in args}
    if len(s) != 1:
        raise ValueError("Expected identifiers, got %s" % str(s))
    children = defaultdict(list)
    for arg in args:
        for child in arg["children"]:
            children[child].append(arg["children"][child])

    children = {k: merge(*v) for k, v in children.items()}
    count = sum(arg["count"] for arg in args)
    return {
        "description": args[0]["description"],
        "children": dict(children),
        "count": count,
        "identifier": args[0]["identifier"],
    }


def create():
    return {
        "count": 0,
        "children": {},
        "identifier": "root",
        "description": {"filename": "", "name": "", "line_number": 0, "line": ""},
    }


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
    plot_figure
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
        if not state["count"]:
            return
        starts.append(start)
        stops.append(stop)
        heights.append(height)
        width = stop - start
        widths.append(width)
        states.append(state)
        times.append(format_time(state["count"] * profile_interval))

        desc = state["description"]
        filenames.append(desc["filename"])
        lines.append(desc["line"])
        line_numbers.append(desc["line_number"])
        names.append(desc["name"])

        ident = state["identifier"]

        try:
            fn = desc["filename"]
        except IndexError:
            colors.append("gray")
        else:
            if fn == "<low-level>":
                colors.append("lightgray")
            else:
                colors.append(color_of(fn))

        delta = (stop - start) / state["count"]

        x = start

        for name, child in state["children"].items():
            width = child["count"] * delta
            traverse(child, x, x + width, height + 1)
            x += width

    traverse(state, 0, 1, 0)
    percentages = ["{:.2f}%".format(100 * w) for w in widths]
    return {
        "left": starts,
        "right": stops,
        "bottom": heights,
        "width": widths,
        "top": [x + 1 for x in heights],
        "color": colors,
        "states": states,
        "filename": filenames,
        "line": lines,
        "line_number": line_numbers,
        "name": names,
        "time": times,
        "percentage": percentages,
    }


def _watch(thread_id, log, interval="20ms", cycle="2s", omit=None, stop=lambda: False):
    interval = parse_timedelta(interval)
    cycle = parse_timedelta(cycle)

    recent = create()
    last = time()

    while not stop():
        if time() > last + cycle:
            log.append((time(), recent))
            recent = create()
            last = time()
        try:
            frame = sys._current_frames()[thread_id]
        except KeyError:
            return

        process(frame, None, recent, omit=omit)
        sleep(interval)


def watch(
    thread_id=None,
    interval="20ms",
    cycle="2s",
    maxlen=1000,
    omit=None,
    stop=lambda: False,
):
    """ Gather profile information on a particular thread

    This starts a new thread to watch a particular thread and returns a deque
    that holds periodic profile information.

    Parameters
    ----------
    thread_id: int
    interval: str
        Time per sample
    cycle: str
        Time per refreshing to a new profile state
    maxlen: int
        Passed onto deque, maximum number of periods
    omit: str
        Don't include entries that start with this filename
    stop: callable
        Function to call to see if we should stop

    Returns
    -------
    deque
    """
    if thread_id is None:
        thread_id = get_thread_identity()

    log = deque(maxlen=maxlen)

    thread = threading.Thread(
        target=_watch,
        name="Profile",
        kwargs={
            "thread_id": thread_id,
            "interval": interval,
            "cycle": cycle,
            "log": log,
            "omit": omit,
            "stop": stop,
        },
    )
    thread.daemon = True
    thread.start()

    return log


def get_profile(history, recent=None, start=None, stop=None, key=None):
    """ Collect profile information from a sequence of profile states

    Parameters
    ----------
    history: Sequence[Tuple[time, Dict]]
        A list or deque of profile states
    recent: dict
        The most recent accumulating state
    start: time
    stop: time
    """
    now = time()
    if start is None:
        istart = 0
    else:
        istart = bisect.bisect_left(history, (start,))

    if stop is None:
        istop = None
    else:
        istop = bisect.bisect_right(history, (stop,)) + 1
        if istop >= len(history):
            istop = None  # include end

    if istart == 0 and istop is None:
        history = list(history)
    else:
        iistop = len(history) if istop is None else istop
        history = [history[i] for i in range(istart, iistop)]

    prof = merge(*toolz.pluck(1, history))

    if not history:
        return create()

    if recent:
        prof = merge(prof, recent)

    return prof


def plot_figure(data, **kwargs):
    """ Plot profile data using Bokeh

    This takes the output from the function ``plot_data`` and produces a Bokeh
    figure

    See Also
    --------
    plot_data
    """
    from bokeh.plotting import ColumnDataSource, figure
    from bokeh.models import HoverTool

    if "states" in data:
        data = toolz.dissoc(data, "states")

    source = ColumnDataSource(data=data)

    fig = figure(tools="tap", **kwargs)
    r = fig.quad(
        "left",
        "right",
        "top",
        "bottom",
        color="color",
        line_color="black",
        line_width=2,
        source=source,
    )

    r.selection_glyph = None
    r.nonselection_glyph = None

    hover = HoverTool(
        point_policy="follow_mouse",
        tooltips="""
            <div>
                <span style="font-size: 14px; font-weight: bold;">Name:</span>&nbsp;
                <span style="font-size: 10px; font-family: Monaco, monospace;">@name</span>
            </div>
            <div>
                <span style="font-size: 14px; font-weight: bold;">Filename:</span>&nbsp;
                <span style="font-size: 10px; font-family: Monaco, monospace;">@filename</span>
            </div>
            <div>
                <span style="font-size: 14px; font-weight: bold;">Line number:</span>&nbsp;
                <span style="font-size: 10px; font-family: Monaco, monospace;">@line_number</span>
            </div>
            <div>
                <span style="font-size: 14px; font-weight: bold;">Line:</span>&nbsp;
                <span style="font-size: 10px; font-family: Monaco, monospace;">@line</span>
            </div>
            <div>
                <span style="font-size: 14px; font-weight: bold;">Time:</span>&nbsp;
                <span style="font-size: 10px; font-family: Monaco, monospace;">@time</span>
            </div>
            <div>
                <span style="font-size: 14px; font-weight: bold;">Percentage:</span>&nbsp;
                <span style="font-size: 10px; font-family: Monaco, monospace;">@width</span>
            </div>
            """,
    )
    fig.add_tools(hover)

    fig.xaxis.visible = False
    fig.yaxis.visible = False
    fig.grid.visible = False

    return fig, source


def _remove_py_stack(frames):
    for entry in frames:
        if entry.is_python:
            break
        yield entry


def llprocess(frames, child, state):
    """ Add counts from low level profile information onto existing state

    This uses the ``stacktrace`` module to collect low level stack trace
    information and place it onto the given sttate.

    It is configured with the ``distributed.worker.profile.low-level`` config
    entry.

    See Also
    --------
    process
    ll_get_stack
    """
    if not frames:
        return
    frame = frames.pop()
    if frames:
        state = llprocess(frames, frame, state)

    addr = hex(frame.addr - frame.offset)
    ident = ";".join(map(str, (frame.name, "<low-level>", addr)))
    try:
        d = state["children"][ident]
    except KeyError:
        d = {
            "count": 0,
            "description": {
                "filename": "<low-level>",
                "name": frame.name,
                "line_number": 0,
                "line": str(frame),
            },
            "children": {},
            "identifier": ident,
        }
        state["children"][ident] = d

    state["count"] += 1

    if child is not None:
        return d
    else:
        d["count"] += 1


def ll_get_stack(tid):
    """ Collect low level stack information from thread id """
    from stacktrace import get_thread_stack

    frames = get_thread_stack(tid, show_python=False)
    llframes = list(_remove_py_stack(frames))[::-1]
    return llframes
