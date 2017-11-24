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
    def initialize(self, server=None):
        self.server = server

    def get_template_path(self):
        return os.path.join(dirname, 'templates')


class Workers(RequestHandler):
    def get(self):
        with log_errors():
            self.render('workers.html',
                        title='Workers',
                        **toolz.merge(self.server.__dict__, ns))


class Worker(RequestHandler):
    def get(self, worker):
        worker = escape.url_unescape(worker)
        with log_errors():
            self.render('worker.html',
                        title='Worker: ' + worker, Worker=worker,
                        **toolz.merge(self.server.__dict__, ns))


class Task(RequestHandler):
    def get(self, task):
        task = escape.url_unescape(task)
        with log_errors():
            self.render('task.html',
                        title='Task: ' + task,
                        Task=task,
                        server=self.server,
                        **toolz.merge(self.server.__dict__, ns))


class Logs(RequestHandler):
    def get(self):
        with log_errors():
            logs = self.server.get_logs()
            self.render('logs.html', title="Logs", logs=logs)


class WorkerLogs(RequestHandler):
    @gen.coroutine
    def get(self, worker):
        with log_errors():
            worker = escape.url_unescape(worker)
            logs = yield self.server.get_worker_logs(workers=[worker])
            logs = logs[worker]
            self.render('logs.html', title="Logs: " + worker, logs=logs)


class WorkerCallStacks(RequestHandler):
    @gen.coroutine
    def get(self, worker):
        with log_errors():
            worker = escape.url_unescape(worker)
            keys = self.server.processing[worker]
            call_stack = yield self.server.get_call_stack(keys=keys)
            self.render('call-stack.html', title="Call Stacks: " + worker,
                        call_stack=call_stack)


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
                            call_stack=call_stack)


class CountsJSON(RequestHandler):
    def get(self):
        response = {
            'bytes': sum(self.server.nbytes.values()),
            'clients': len(self.server.wants_what),
            'cores': sum(self.server.ncores.values()),
            'erred': len(self.server.exceptions_blame),
            'hosts': len(self.server.host_info),
            'idle': len(self.server.idle),
            'memory': len(self.server.who_has),
            'processing': len(self.server.rprocessing),
            'ready': len(self.server.ready),
            'released': len(self.server.released),
            'saturated': len(self.server.saturated),
            'tasks': len(self.server.tasks),
            'unrunnable': len(self.server.unrunnable),
            'waiting': len(self.server.waiting),
            'waiting_data': len(self.server.waiting_data),
            'workers': len(self.server.workers),
        }
        self.write(response)


class IdentityJSON(RequestHandler):
    def get(self):
        self.write(self.server.identity())


class IndexJSON(RequestHandler):
    def get(self):
        with log_errors():
            r = [url for url, _ in routes if url.endswith('.json')]
            self.render('json-index.html', routes=r, title='Index of JSON routes')


routes = [
        (r'/info/workers.html', Workers),
        (r'/info/worker/(.*).html', Worker),
        (r'/info/task/(.*).html', Task),
        (r'/info/logs.html', Logs),
        (r'/info/call-stacks/(.*).html', WorkerCallStacks),
        (r'/info/call-stack/(.*).html', TaskCallStack),
        (r'/info/logs/(.*).html', WorkerLogs),
        (r'/json/counts.json', CountsJSON),
        (r'/json/identity.json', IdentityJSON),
        (r'/json/index.html', IndexJSON),
]


def get_handlers(server):
    return [(url, cls, {'server': server}) for url, cls in routes]
