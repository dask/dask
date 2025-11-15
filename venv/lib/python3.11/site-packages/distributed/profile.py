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

from __future__ import annotations

import bisect
import dis
import linecache
import sys
import threading
from collections import defaultdict, deque
from collections.abc import Callable, Collection
from time import sleep
from types import FrameType
from typing import Any

import tlz as toolz

import dask.config
from dask.typing import NoDefault, no_default
from dask.utils import format_time, parse_timedelta

from distributed.metrics import time
from distributed.utils import color_of

#: This lock can be acquired to ensure that no instance of watch() is concurrently holding references to frames
lock = threading.Lock()


def identifier(frame: FrameType | None) -> str:
    """A string identifier from a frame

    Strings are cheaper to use as indexes into dicts than tuples or dicts
    """
    if frame is None:
        return "None"
    else:
        co = frame.f_code
        try:
            return ";".join(
                (
                    co.co_name,
                    co.co_filename,
                    str(co.co_firstlineno),
                )
            )
        except AttributeError:
            if co.__module__:
                return f"{co.__module__}.{co.__qualname__}:{_f_lineno(frame)}"  # type: ignore[attr-defined]
            else:
                return f"{co.__qualname__}:{_f_lineno(frame)}"  # type: ignore[attr-defined]


def _f_lineno(frame: FrameType) -> int:
    """Work around some frames lacking an f_lineno
    See: https://bugs.python.org/issue47085
    """
    f_lineno = frame.f_lineno
    if f_lineno is not None:
        return f_lineno

    f_lasti = frame.f_lasti
    code = frame.f_code
    try:
        prev_line = code.co_firstlineno

        for start, next_line in dis.findlinestarts(code):
            if f_lasti < start:
                return prev_line
            if next_line:
                prev_line = next_line

        return prev_line
    except Exception:
        return -1


def repr_frame(frame: FrameType) -> str:
    """Render a frame as a line for inclusion into a text traceback"""
    co = frame.f_code
    f_lineno = _f_lineno(frame)
    text = f'  File "{co.co_filename}", line {f_lineno}, in {co.co_name}'
    line = linecache.getline(co.co_filename, f_lineno, frame.f_globals).lstrip()
    return text + "\n\t" + line


def info_frame(frame: FrameType) -> dict[str, Any]:
    co = frame.f_code
    f_lineno = _f_lineno(frame)
    try:
        name = co.co_name
        filename = co.co_filename
        line = linecache.getline(co.co_filename, f_lineno, frame.f_globals).lstrip()
    except (AttributeError, IndexError):
        line = ""
        name = co.__qualname__  # type: ignore[attr-defined]
        filename = "<built-in>"
    return {
        "filename": filename,
        "name": name,
        "line_number": f_lineno,
        "line": line,
    }


def process(
    frame: FrameType,
    child: object | None,
    state: dict[str, Any],
    *,
    stop: str | None = None,
    omit: Collection[str] = (),
    depth: int | None = None,
) -> dict[str, Any] | None:
    """Add counts from a frame stack onto existing state

    This recursively adds counts to the existing state dictionary and creates
    new entries for new functions.

    Parameters
    ----------
    frame:
        The frame to process onto the state
    child:
        For internal use only
    state:
        The profile state to accumulate this frame onto, see ``create``
    stop:
        Filename suffix that should stop processing if we encounter it
    omit:
        Filenames that we should omit from processing
    depth:
        For internal use only, how deep we are in the call stack
        Used to prevent stack overflow

    Examples
    --------
    >>> import sys, threading
    >>> ident = threading.get_ident()  # replace with your thread of interest
    >>> frame = sys._current_frames()[ident]
    >>> state = create()
    >>> process(frame, None, state)
    >>> state
    {'count': 1,
     'identifier': 'root',
     'description': 'root',
     'children': {'...'}}

    See also
    --------
    create
    merge
    """
    if depth is None:
        # Cut off rather conservatively since the output of the profiling
        # sometimes need to be recursed into as well, e.g. for serialization
        # which can cause recursion errors later on since this can generate
        # deeply nested dictionaries
        depth = min(100, sys.getrecursionlimit() // 4)

    if any(frame.f_code.co_filename.endswith(o) for o in omit):
        return None

    prev = frame.f_back
    if (
        depth > 0
        and prev is not None
        and (stop is None or not prev.f_code.co_filename.endswith(stop))
    ):
        new_state = process(prev, frame, state, stop=stop, depth=depth - 1)
        if new_state is None:
            return None
        state = new_state

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
        return None


def merge(*args: dict[str, Any]) -> dict[str, Any]:
    """Merge multiple frame states together"""
    if not args:
        return create()
    s = {arg["identifier"] for arg in args}
    if len(s) != 1:  # pragma: no cover
        raise ValueError(f"Expected identifiers, got {s}")
    children = defaultdict(list)
    for arg in args:
        for child in arg["children"]:
            children[child].append(arg["children"][child])

    try:
        children_dict = {k: merge(*v) for k, v in children.items()}
    except RecursionError:  # pragma: no cover
        children_dict = {}
    count = sum(arg["count"] for arg in args)
    return {
        "description": args[0]["description"],
        "children": children_dict,
        "count": count,
        "identifier": args[0]["identifier"],
    }


def create() -> dict[str, Any]:
    return {
        "count": 0,
        "children": {},
        "identifier": "root",
        "description": {"filename": "", "name": "", "line_number": 0, "line": ""},
    }


def call_stack(frame: FrameType) -> list[str]:
    """Create a call text stack from a frame

    Returns
    -------
    list of strings
    """
    L = []
    cur_frame: FrameType | None = frame
    while cur_frame:
        L.append(repr_frame(cur_frame))
        cur_frame = cur_frame.f_back
    return L[::-1]


def plot_data(state, profile_interval=0.010):
    """Convert a profile state into data useful by Bokeh

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

        try:
            fn = desc["filename"]
        except IndexError:  # pragma: no cover
            colors.append("gray")
        else:
            if fn == "<low-level>":  # pragma: no cover
                colors.append("lightgray")
            else:
                colors.append(color_of(fn))

        delta = (stop - start) / state["count"]

        x = start

        for _, child in state["children"].items():
            width = child["count"] * delta
            traverse(child, x, x + width, height + 1)
            x += width

    traverse(state, 0, 1, 0)
    percentages = [f"{100 * w:.1f}%" for w in widths]
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


def _watch(
    thread_id: int,
    log: deque[tuple[float, dict[str, Any]]],  # [(timestamp, output of create()), ...]
    interval: float,
    cycle: float,
    omit: Collection[str],
    stop: Callable[[], bool],
) -> None:
    recent = create()
    last = time()

    while not stop():
        if time() > last + cycle:
            recent = create()
            with lock:
                log.append((time(), recent))
                last = time()
                try:
                    frame = sys._current_frames()[thread_id]
                except KeyError:
                    return

                process(frame, None, recent, omit=omit)
                del frame
        sleep(interval)


def watch(
    thread_id: int | None = None,
    interval: str = "20ms",
    cycle: str = "2s",
    maxlen: int | None | NoDefault = no_default,
    omit: Collection[str] = (),
    stop: Callable[[], bool] = lambda: False,
) -> deque[tuple[float, dict[str, Any]]]:
    """Gather profile information on a particular thread

    This starts a new thread to watch a particular thread and returns a deque
    that holds periodic profile information.

    Parameters
    ----------
    thread_id : int, optional
        Defaults to current thread
    interval : str
        Time per sample
    cycle : str
        Time per refreshing to a new profile state
    maxlen : int
        Passed onto deque, maximum number of periods
    omit : collection of str
        Don't include entries whose filename includes any of these substrings
    stop : callable
        Function to call to see if we should stop. It must
        accept no arguments and return a bool (True to stop,
        False to continue).

    Returns
    -------
    deque of tuples:

    - timestamp
    - dict[str, Any] (output of ``create()``)
    """
    if maxlen is no_default:
        maxlen = dask.config.get("distributed.admin.low-level-log-length")
        assert isinstance(maxlen, int) or maxlen is None
    log: deque[tuple[float, dict[str, Any]]] = deque(maxlen=maxlen)

    thread = threading.Thread(
        target=_watch,
        name="Profile",
        kwargs={
            "thread_id": thread_id or threading.get_ident(),
            "interval": parse_timedelta(interval),
            "cycle": parse_timedelta(cycle),
            "log": log,
            "omit": omit,
            "stop": stop,
        },
    )
    thread.daemon = True
    thread.start()

    return log


def get_profile(history, recent=None, start=None, stop=None, key=None):
    """Collect profile information from a sequence of profile states

    Parameters
    ----------
    history : Sequence[Tuple[time, Dict]]
        A list or deque of profile states
    recent : dict
        The most recent accumulating state
    start : time
    stop : time
    """
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
    """Plot profile data using Bokeh

    This takes the output from the function ``plot_data`` and produces a Bokeh
    figure

    See Also
    --------
    plot_data
    """
    from bokeh.models import HoverTool
    from bokeh.plotting import ColumnDataSource, figure

    if "states" in data:
        data = toolz.dissoc(data, "states")

    source = ColumnDataSource(data=data)

    fig = figure(tools="tap,box_zoom,xwheel_zoom,reset", **kwargs)
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
                <span style="font-size: 10px; font-family: Monaco, monospace;">@percentage</span>
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


def llprocess(  # type: ignore[no-untyped-def]
    frames,
    child: object | None,
    state: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Add counts from low level profile information onto existing state

    This uses the ``stacktrace`` module to collect low level stack trace
    information and place it onto the given state.

    It is configured with the ``distributed.worker.profile.low-level`` config
    entry.

    See Also
    --------
    process
    ll_get_stack
    """
    if not frames:
        return None
    frame = frames.pop()
    if frames:
        state = llprocess(frames, frame, state)
    assert state

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
        return None


def ll_get_stack(tid):
    """Collect low level stack information from thread id"""
    from stacktrace import get_thread_stack

    frames = get_thread_stack(tid, show_python=False)
    llframes = list(_remove_py_stack(frames))[::-1]
    return llframes
