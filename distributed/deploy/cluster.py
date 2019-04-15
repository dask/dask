import logging
import os
from weakref import ref

import dask

from .adaptive import Adaptive

from ..utils import format_bytes, PeriodicCallback, log_errors, ignoring

logger = logging.getLogger(__name__)


class Cluster(object):
    """ Superclass for cluster objects

    This expects a local Scheduler defined on the object.  It provides
    common methods and an IPython widget display.

    Clusters inheriting from this class should provide the following:

    1.  A local ``Scheduler`` object at ``.scheduler``
    2.  scale_up and scale_down methods as defined below::

        def scale_up(self, n: int):
            ''' Brings total worker count up to ``n`` '''

        def scale_down(self, workers: List[str]):
            ''' Close the workers with the given addresses '''

    This will provide a general ``scale`` method as well as an IPython widget
    for display.

    Examples
    --------

    >>> from distributed.deploy import Cluster
    >>> class MyCluster(cluster):
    ...     def scale_up(self, n):
    ...         ''' Bring the total worker count up to n '''
    ...         pass
    ...     def scale_down(self, workers):
    ...         ''' Close the workers with the given addresses '''
    ...         pass

    >>> cluster = MyCluster()
    >>> cluster.scale(5)                       # scale manually
    >>> cluster.adapt(minimum=1, maximum=100)  # scale automatically

    See Also
    --------
    LocalCluster: a simple implementation with local workers
    """

    def adapt(self, **kwargs):
        """ Turn on adaptivity

        For keyword arguments see dask.distributed.Adaptive

        Examples
        --------
        >>> cluster.adapt(minimum=0, maximum=10, interval='500ms')
        """
        with ignoring(AttributeError):
            self._adaptive.stop()
        if not hasattr(self, "_adaptive_options"):
            self._adaptive_options = {}
        self._adaptive_options.update(kwargs)
        self._adaptive = Adaptive(self.scheduler, self, **self._adaptive_options)
        return self._adaptive

    @property
    def scheduler_address(self):
        return self.scheduler.address

    @property
    def dashboard_link(self):
        template = dask.config.get("distributed.dashboard.link")
        host = self.scheduler.address.split("://")[1].split(":")[0]
        port = self.scheduler.services["bokeh"].port
        return template.format(host=host, port=port, **os.environ)

    def scale(self, n):
        """ Scale cluster to n workers

        Parameters
        ----------
        n: int
            Target number of workers

        Example
        -------
        >>> cluster.scale(10)  # scale cluster to ten workers

        See Also
        --------
        Cluster.scale_up
        Cluster.scale_down
        """
        with log_errors():
            if n >= len(self.scheduler.workers):
                self.scheduler.loop.add_callback(self.scale_up, n)
            else:
                to_close = self.scheduler.workers_to_close(
                    n=len(self.scheduler.workers) - n
                )
                logger.debug("Closing workers: %s", to_close)
                self.scheduler.loop.add_callback(
                    self.scheduler.retire_workers, workers=to_close
                )
                self.scheduler.loop.add_callback(self.scale_down, to_close)

    def _widget_status(self):
        workers = len(self.scheduler.workers)
        cores = sum(ws.ncores for ws in self.scheduler.workers.values())
        memory = sum(ws.memory_limit for ws in self.scheduler.workers.values())
        memory = format_bytes(memory)
        text = """
<div>
  <style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
  </style>
  <table style="text-align: right;">
    <tr><th>Workers</th> <td>%d</td></tr>
    <tr><th>Cores</th> <td>%d</td></tr>
    <tr><th>Memory</th> <td>%s</td></tr>
  </table>
</div>
""" % (
            workers,
            cores,
            memory,
        )
        return text

    def _widget(self):
        """ Create IPython widget for display within a notebook """
        try:
            return self._cached_widget
        except AttributeError:
            pass

        from ipywidgets import Layout, VBox, HBox, IntText, Button, HTML, Accordion

        layout = Layout(width="150px")

        if "bokeh" in self.scheduler.services:
            link = self.dashboard_link
            link = '<p><b>Dashboard: </b><a href="%s" target="_blank">%s</a></p>\n' % (
                link,
                link,
            )
        else:
            link = ""

        title = "<h2>%s</h2>" % type(self).__name__
        title = HTML(title)
        dashboard = HTML(link)

        status = HTML(self._widget_status(), layout=Layout(min_width="150px"))

        request = IntText(0, description="Workers", layout=layout)
        scale = Button(description="Scale", layout=layout)

        minimum = IntText(0, description="Minimum", layout=layout)
        maximum = IntText(0, description="Maximum", layout=layout)
        adapt = Button(description="Adapt", layout=layout)

        accordion = Accordion(
            [HBox([request, scale]), HBox([minimum, maximum, adapt])],
            layout=Layout(min_width="500px"),
        )
        accordion.selected_index = None
        accordion.set_title(0, "Manual Scaling")
        accordion.set_title(1, "Adaptive Scaling")

        box = VBox([title, HBox([status, accordion]), dashboard])

        self._cached_widget = box

        def adapt_cb(b):
            self.adapt(minimum=minimum.value, maximum=maximum.value)

        adapt.on_click(adapt_cb)

        def scale_cb(b):
            with log_errors():
                n = request.value
                with ignoring(AttributeError):
                    self._adaptive.stop()
                self.scale(n)

        scale.on_click(scale_cb)

        scheduler_ref = ref(self.scheduler)

        def update():
            status.value = self._widget_status()

        pc = PeriodicCallback(update, 500, io_loop=self.scheduler.loop)
        self.scheduler.periodic_callbacks["cluster-repr"] = pc
        pc.start()

        return box

    def _ipython_display_(self, **kwargs):
        return self._widget()._ipython_display_(**kwargs)
