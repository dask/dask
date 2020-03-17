import asyncio
from bisect import bisect
from operator import add
from time import time
import weakref

from bokeh.layouts import row, column
from bokeh.models import (
    ColumnDataSource,
    Plot,
    DataRange1d,
    LinearAxis,
    HoverTool,
    BoxZoomTool,
    ResetTool,
    PanTool,
    WheelZoomTool,
    Range1d,
    Quad,
    TapTool,
    OpenURL,
    Button,
    Select,
)
from bokeh.palettes import Spectral9
from bokeh.plotting import figure
import dask
from tornado import gen

from distributed.dashboard.utils import without_property_validation, BOKEH_VERSION
from distributed import profile
from distributed.utils import log_errors, parse_timedelta

if dask.config.get("distributed.dashboard.export-tool"):
    from distributed.dashboard.export_tool import ExportTool
else:
    ExportTool = None


profile_interval = dask.config.get("distributed.worker.profile.interval")
profile_interval = parse_timedelta(profile_interval, default="ms")


class DashboardComponent:
    """ Base class for Dask.distributed UI dashboard components.

    This class must have two attributes, ``root`` and ``source``, and one
    method ``update``:

    *  source: a Bokeh ColumnDataSource
    *  root: a Bokeh Model
    *  update: a method that consumes the messages dictionary found in
               distributed.bokeh.messages
    """

    def __init__(self):
        self.source = None
        self.root = None

    def update(self, messages):
        """ Reads from bokeh.distributed.messages and updates self.source """


def add_periodic_callback(doc, component, interval):
    """ Add periodic callback to doc in a way that avoids reference cycles

    If we instead use ``doc.add_periodic_callback(component.update, 100)`` then
    the component stays in memory as a reference cycle because its method is
    still around.  This way we avoid that and let things clean up a bit more
    nicely.

    TODO: we still have reference cycles.  Docs seem to be referred to by their
    add_periodic_callback methods.
    """
    ref = weakref.ref(component)

    doc.add_periodic_callback(lambda: update(ref), interval)
    _attach(doc, component)


def update(ref):
    comp = ref()
    if comp is not None:
        comp.update()


def _attach(doc, component):
    if not hasattr(doc, "components"):
        doc.components = set()

    doc.components.add(component)
