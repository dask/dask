from __future__ import annotations

import json
import logging
import os
import os.path
from datetime import datetime
from typing import TYPE_CHECKING, Any

from tlz import first, merge
from tornado import escape
from tornado.websocket import WebSocketHandler

import dask
from dask.typing import Key
from dask.utils import format_bytes, format_time

from distributed.diagnostics.websocket import WebsocketPlugin
from distributed.http.utils import RequestHandler, redirect
from distributed.metrics import time
from distributed.utils import log_errors

if TYPE_CHECKING:
    from distributed import Scheduler

ns = {
    func.__name__: func
    for func in [format_bytes, format_time, datetime.fromtimestamp, time]
}

rel_path_statics = {"rel_path_statics": "../../.."}


logger = logging.getLogger(__name__)

API_ENABLED = "distributed.http.scheduler.api" in dask.config.get(
    "distributed.scheduler.http.routes"
)


class Workers(RequestHandler):
    @log_errors
    def get(self):
        self.render(
            "workers.html",
            title="Workers",
            scheduler=self.server,
            api_enabled=API_ENABLED,
            **merge(
                self.server.__dict__,
                self.server.__pdict__,
                ns,
                self.extra,
                rel_path_statics,
            ),
        )


class Worker(RequestHandler):
    @log_errors
    def get(self, worker):
        worker = escape.url_unescape(worker)
        if worker not in self.server.workers:
            self.send_error(404)
            return

        self.render(
            "worker.html",
            title="Worker: " + worker,
            scheduler=self.server,
            api_enabled=API_ENABLED,
            Worker=worker,
            **merge(
                self.server.__dict__,
                self.server.__pdict__,
                ns,
                self.extra,
                rel_path_statics,
            ),
        )


class Exceptions(RequestHandler):
    @log_errors
    def get(self):
        self.render(
            "exceptions.html",
            title="Exceptions",
            scheduler=self.server,
            **merge(
                self.server.__dict__,
                self.server.__pdict__,
                ns,
                self.extra,
                rel_path_statics,
            ),
        )


def _get_actual_scheduler_key(key: str, scheduler: Scheduler) -> Key:
    key = escape.url_unescape(key, plus=False)

    if key in scheduler.tasks:
        return key  # Basic str key

    # Tuple, bytes, or int key
    # First try safely reverting str(Key)
    def lists_to_tuples(o: object) -> Any:
        if isinstance(o, list):
            return tuple(lists_to_tuples(i) for i in o)
        else:
            return o

    key2 = key.replace("(", "[").replace(")", "]").replace("'", '"')
    try:
        key2 = lists_to_tuples(json.loads(key2))
        if key2 in scheduler.tasks:
            return key2
    except json.JSONDecodeError:
        pass

    # Edge case of keys with string elements containing [ ] ( ) ' " or bytes
    for key3 in scheduler.tasks:
        if str(key3) == key:
            return key3

    raise KeyError(key)


class Task(RequestHandler):
    @log_errors
    def get(self, task: str) -> None:
        try:
            key = _get_actual_scheduler_key(task, self.server)
        except KeyError:
            self.send_error(404)
            return

        self.render(
            "task.html",
            title=f"Task: {key!r}",
            Task=key,
            scheduler=self.server,
            api_enabled=API_ENABLED,
            **merge(
                self.server.__dict__,
                self.server.__pdict__,
                ns,
                self.extra,
                rel_path_statics,
            ),
        )


class Logs(RequestHandler):
    @log_errors
    def get(self):
        logs = self.server.get_logs()
        self.render(
            "logs.html",
            title="Logs",
            logs=logs,
            **merge(self.extra, rel_path_statics),
        )


class WorkerLogs(RequestHandler):
    @log_errors
    async def get(self, worker):
        worker = escape.url_unescape(worker)
        try:
            logs = await self.server.get_worker_logs(workers=[worker])
        except Exception:
            if not any(worker == w.address for w in self.server.workers.values()):
                self.send_error(404)
                return
            raise
        logs = logs[worker]
        self.render(
            "logs.html",
            title="Logs: " + worker,
            logs=logs,
            **merge(self.extra, rel_path_statics),
        )


class WorkerCallStacks(RequestHandler):
    @log_errors
    async def get(self, worker):
        worker = escape.url_unescape(worker)
        try:
            keys = {ts.key for ts in self.server.workers[worker].processing}
        except KeyError:
            self.send_error(404)
            return
        call_stack = await self.server.get_call_stack(keys=keys)
        self.render(
            "call-stack.html",
            title="Call Stacks: " + worker,
            call_stack=call_stack,
            **merge(self.extra, rel_path_statics),
        )


class TaskCallStack(RequestHandler):
    @log_errors
    async def get(self, task: str) -> None:
        try:
            key = _get_actual_scheduler_key(task, self.server)
        except KeyError:
            self.send_error(404)
            return

        call_stack = await self.server.get_call_stack(keys=[key])
        if not call_stack:
            self.write(
                "<p>Task not actively running. "
                "It may be finished or not yet started</p>"
            )
        else:
            self.render(
                "call-stack.html",
                title=f"Call Stack: {key!r}",
                call_stack=call_stack,
                **merge(self.extra, rel_path_statics),
            )


class IndividualPlots(RequestHandler):
    def get(self):
        try:
            from bokeh.server.tornado import BokehTornado

            bokeh_application = first(
                app
                for app in self.server.http_application.applications
                if isinstance(app, BokehTornado)
            )
            individual_bokeh = {
                uri.strip("/").replace("-", " ").title(): uri
                for uri in bokeh_application.app_paths
                if uri.lstrip("/").startswith("individual-")
                and not uri.endswith(".json")
            }
            individual_static = {
                uri.strip("/")
                .replace(".html", "")
                .replace("-", " ")
                .title(): "/statics/"
                + uri
                for uri in os.listdir(
                    os.path.join(os.path.dirname(__file__), "..", "static")
                )
                if uri.lstrip("/").startswith("individual-") and uri.endswith(".html")
            }
            result = {**individual_bokeh, **individual_static}
            self.write(result)
        except (ImportError, StopIteration):
            self.write({})


class EventstreamHandler(WebSocketHandler):
    def initialize(self, dask_server=None, extra=None):
        self.server = dask_server
        self.extra = extra or {}
        self.plugin = WebsocketPlugin(self, self.server)
        self.server.add_plugin(self.plugin)

    def send(self, name, data):
        data["name"] = name
        for k in list(data):
            # Drop bytes objects for now
            if isinstance(data[k], bytes):
                del data[k]
        self.write_message(data)

    def open(self):
        for worker in self.server.workers:
            self.plugin.add_worker(self.server, worker)

    def on_message(self, message):
        message = json.loads(message)
        if message["name"] == "ping":
            self.send("pong", {"timestamp": str(datetime.now())})

    def on_close(self):
        self.server.remove_plugin(name=self.plugin.name)


routes: list[tuple] = [
    (r"info", redirect("info/main/workers.html"), {}),
    (r"info/main/workers.html", Workers, {}),
    (r"info/main/exceptions.html", Exceptions, {}),
    (r"info/worker/(.*).html", Worker, {}),
    (r"info/task/(.*).html", Task, {}),
    (r"info/main/logs.html", Logs, {}),
    (r"info/call-stacks/(.*).html", WorkerCallStacks, {}),
    (r"info/call-stack/(.*).html", TaskCallStack, {}),
    (r"info/logs/(.*).html", WorkerLogs, {}),
    (r"individual-plots.json", IndividualPlots, {}),
    (r"eventstream", EventstreamHandler, {}),
]
