from __future__ import annotations

from distributed.http.utils import RequestHandler, redirect
from distributed.utils import log_errors
from distributed.versions import BOKEH_REQUIREMENT


class MissingBokeh(RequestHandler):
    @log_errors
    def get(self):
        self.write(
            f"<p>Dask needs {BOKEH_REQUIREMENT} for the dashboard.</p>"
            f'<p>Install with conda: <code>conda install "{BOKEH_REQUIREMENT}"</code></p>'
            f'<p>Install with pip: <code>pip install "{BOKEH_REQUIREMENT}"</code></p>'
        )


routes: list[tuple] = [(r"/", redirect("status"), {}), (r"status", MissingBokeh, {})]
