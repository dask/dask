
import logging
import os

from tornado import web

from .core import JSON, MyApp


logger = logging.getLogger(__name__)



class Info(JSON):
    def get(self):
        resp = {'ncores': self.worker.ncores,
                'nkeys': len(self.worker.data),
                'status': self.worker.status}
        self.write(resp)

def resource_collect(pid=None):
    try:
        import psutil
    except ImportError:
        return {}

    p = psutil.Process(pid or os.getpid())
    return {'cpu_percent': psutil.cpu_percent(),
            'status': p.status(),
            'memory_percent': p.memory_percent(),
            'memory_info_ex': p.memory_info_ex(),
            'disk_io_counters': psutil.disk_io_counters(),
            'net_io_counters': psutil.net_io_counters()}

class Resources(JSON):
    def get(self):
        self.write(resource_collect())


def HTTPWorker(worker):
    application = MyApp(web.Application([
        (r'/info.json', Info, {'worker': worker}),
        (r'/resources.json', Resources, {'worker': worker}),
        ]))
    return application
