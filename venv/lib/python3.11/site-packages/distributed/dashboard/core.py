from __future__ import annotations

import functools
import warnings

from bokeh.application import Application
from bokeh.application.handlers.function import FunctionHandler
from bokeh.resources import Resources
from bokeh.server.util import create_hosts_allowlist
from packaging.version import parse as parse_version

import dask

from distributed.dashboard.utils import BOKEH_VERSION
from distributed.versions import BOKEH_REQUIREMENT

# Set `prereleases=True` to allow for use with dev versions of `bokeh`
if not BOKEH_REQUIREMENT.specifier.contains(BOKEH_VERSION, prereleases=True):
    warnings.warn(
        f"\nDask needs {BOKEH_REQUIREMENT} for the dashboard."
        f"\nYou have bokeh={BOKEH_VERSION}."
        "\nContinuing without the dashboard."
    )
    raise ImportError(
        f"Dask needs {BOKEH_REQUIREMENT} for the dashboard, not bokeh={BOKEH_VERSION}"
    )


if BOKEH_VERSION < parse_version("3.3.0"):
    from bokeh.server.server import BokehTornado as DaskBokehTornado
else:
    from bokeh.server.server import BokehTornado

    class DaskBokehTornado(BokehTornado):  # type: ignore[no-redef]
        def resources(self, absolute_url: str | bool | None = True) -> Resources:
            return super().resources(absolute_url)


def BokehApplication(applications, server, prefix="/", template_variables=None):
    template_variables = template_variables or {}
    prefix = "/" + prefix.strip("/") + "/" if prefix else "/"

    extra = {"prefix": prefix, **template_variables}

    funcs = {k: functools.partial(v, server, extra) for k, v in applications.items()}
    apps = {k: Application(FunctionHandler(v)) for k, v in funcs.items()}

    kwargs = dask.config.get("distributed.scheduler.dashboard.bokeh-application").copy()
    extra_websocket_origins = create_hosts_allowlist(
        kwargs.pop("allow_websocket_origin"), server.http_server.port
    )

    return DaskBokehTornado(
        apps,
        prefix=prefix,
        use_index=False,
        extra_websocket_origins=extra_websocket_origins,
        absolute_url="",
        **kwargs,
    )
