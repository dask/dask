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


def get_handlers(server):
    return [
            (r'/info/workers.html', Workers, {'server': server}),
            (r'/info/worker/(.*).html', Worker, {'server': server}),
            (r'/info/task/(.*).html', Task, {'server': server}),
            (r'/info/logs.html', Logs, {'server': server}),
            (r'/info/logs/(.*).html', WorkerLogs, {'server': server}),
    ]
