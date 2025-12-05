from __future__ import annotations

import weakref

from bokeh.core.properties import without_property_validation


class DashboardComponent:
    """Base class for Dask.distributed UI dashboard components.

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
        """Reads from bokeh.distributed.messages and updates self.source"""


def add_periodic_callback(doc, component, interval):
    """Add periodic callback to doc in a way that avoids reference cycles

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


@without_property_validation
def update(ref):
    comp = ref()
    if comp is not None:
        comp.update()


def _attach(doc, component):
    if not hasattr(doc, "components"):
        doc.components = set()

    doc.components.add(component)
