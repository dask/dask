from datetime import datetime
import os

import toolz
from tornado import escape
from tornado import gen
from tornado import web

from ..utils import log_errors, format_bytes, format_time

dirname = os.path.dirname(__file__)

ns = {func.__name__: func for func in [format_bytes, format_time, datetime.fromtimestamp]}


class RequestHandler(web.RequestHandler):
    def initialize(self, server=None, extra=None):
        self.server = server
        self.extra = extra or {}

    def get_template_path(self):
        return os.path.join(dirname, 'templates')


class Workers(RequestHandler):
    def get(self):
        with log_errors():
            self.render('workers.html',
                        title='Workers',
                        **toolz.merge(self.server.__dict__, ns, self.extra))


class Worker(RequestHandler):
    def get(self, worker):
        worker = escape.url_unescape(worker)
        with log_errors():
            self.render('worker.html',
                        title='Worker: ' + worker, Worker=worker,
                        **toolz.merge(self.server.__dict__, ns, self.extra))


class Task(RequestHandler):
    def get(self, task):
        task = escape.url_unescape(task)
        with log_errors():
            self.render('task.html',
                        title='Task: ' + task,
                        Task=task,
                        server=self.server,
                        **toolz.merge(self.server.__dict__, ns, self.extra))


class Logs(RequestHandler):
    def get(self):
        with log_errors():
            logs = self.server.get_logs()
            self.render('logs.html', title="Logs", logs=logs, **self.extra)


class WorkerLogs(RequestHandler):
    @gen.coroutine
    def get(self, worker):
        with log_errors():
            worker = escape.url_unescape(worker)
            logs = yield self.server.get_worker_logs(workers=[worker])
            logs = logs[worker]
            self.render('logs.html', title="Logs: " + worker, logs=logs,
                        **self.extra)


class WorkerCallStacks(RequestHandler):
    @gen.coroutine
    def get(self, worker):
        with log_errors():
            worker = escape.url_unescape(worker)
            keys = self.server.processing[worker]
            call_stack = yield self.server.get_call_stack(keys=keys)
            self.render('call-stack.html', title="Call Stacks: " + worker,
                        call_stack=call_stack, **self.extra)


class TaskCallStack(RequestHandler):
    @gen.coroutine
    def get(self, key):
        with log_errors():
            key = escape.url_unescape(key)
            call_stack = yield self.server.get_call_stack(keys=[key])
            if not call_stack:
                self.write("<p>Task not actively running. "
                           "It may be finished or not yet started</p>")
            else:
                self.render('call-stack.html', title="Call Stack: " + key,
                            call_stack=call_stack, **self.extra)


class CountsJSON(RequestHandler):
    def get(self):
        scheduler = self.server
        erred = 0
        nbytes = 0
        ncores = 0
        memory = 0
        processing = 0
        released = 0
        waiting = 0
        waiting_data = 0

        for ts in scheduler.tasks.values():
            if ts.exception_blame is not None:
                erred += 1
            elif ts.state == 'released':
                released += 1
            if ts.waiting_on:
                waiting += 1
            if ts.waiters:
                waiting_data += 1
        for ws in scheduler.workers.values():
            ncores += ws.ncores
            memory += len(ws.has_what)
            nbytes += ws.nbytes
            processing += len(ws.processing)

        response = {
            'bytes': nbytes,
            'clients': len(scheduler.clients),
            'cores': ncores,
            'erred': erred,
            'hosts': len(scheduler.host_info),
            'idle': len(scheduler.idle),
            'memory': memory,
            'processing': processing,
            'released': released,
            'saturated': len(scheduler.saturated),
            'tasks': len(scheduler.tasks),
            'unrunnable': len(scheduler.unrunnable),
            'waiting': waiting,
            'waiting_data': waiting_data,
            'workers': len(scheduler.workers),
        }
        self.write(response)


class IdentityJSON(RequestHandler):
    def get(self):
        self.write(self.server.identity())


class IndexJSON(RequestHandler):
    def get(self):
        with log_errors():
            r = [url for url, _ in routes if url.endswith('.json')]
            self.render('json-index.html', routes=r, title='Index of JSON routes', **self.extra)


routes = [
        (r'info/main/workers.html', Workers),
        (r'info/worker/(.*).html', Worker),
        (r'info/task/(.*).html', Task),
        (r'info/main/logs.html', Logs),
        (r'info/call-stacks/(.*).html', WorkerCallStacks),
        (r'info/call-stack/(.*).html', TaskCallStack),
        (r'info/logs/(.*).html', WorkerLogs),
        (r'json/counts.json', CountsJSON),
        (r'json/identity.json', IdentityJSON),
        (r'json/index.html', IndexJSON),
]


def get_handlers(server):
    return [(url, cls, {'server': server}) for url, cls in routes]
