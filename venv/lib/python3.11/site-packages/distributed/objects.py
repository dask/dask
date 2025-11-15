"""This file contains custom objects.
These are mostly regular objects with more useful _repr_ and _repr_html_ methods."""

from __future__ import annotations

from urllib.parse import urlparse

from dask.widgets import get_environment, get_template

from distributed.utils import format_dashboard_link


class HasWhat(dict):
    """A dictionary of all workers and which keys that worker has."""

    def _repr_html_(self):
        return get_template("has_what.html.j2").render(has_what=self)


class WhoHas(dict):
    """A dictionary of all keys and which workers have that key."""

    def _repr_html_(self):
        return get_template("who_has.html.j2").render(who_has=self)


class SchedulerInfo(dict):
    """A dictionary of information about the scheduler and workers."""

    def _repr_html_(self):
        def _format_dashboard_address(server):
            try:
                host = (
                    server["host"]
                    if "host" in server
                    else urlparse(server["address"]).hostname
                )
                return format_dashboard_link(host, server["services"]["dashboard"])
            except KeyError:
                return None

        environment = get_environment()
        environment.filters["format_dashboard_address"] = _format_dashboard_address
        return environment.get_template("scheduler_info.html.j2").render(
            scheduler=self,
            **self,
        )
