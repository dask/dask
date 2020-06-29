from ..utils import RequestHandler, redirect
from ...utils import log_errors


class MissingBokeh(RequestHandler):
    def get(self):
        with log_errors():
            self.write(
                "<p>Dask needs bokeh >= 0.13.0 for the dashboard.</p>"
                "<p>Install with conda: conda install bokeh>=0.13.0</p>"
                "<p>Install with pip: pip install bokeh>=0.13.0</p>"
            )


routes = [(r"/", redirect("status"), {}), (r"status", MissingBokeh, {})]
