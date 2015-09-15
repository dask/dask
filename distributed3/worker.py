from __future__ import print_function, division

from concurrent.futures import ThreadPoolExecutor
from multiprocessing.pool import ThreadPool
import traceback



from toolz import merge
from tornado.gen import Return
from tornado import gen
from tornado.ioloop import IOLoop

from .core import rpc, connect_sync, read_sync, write_sync, connect, Server
from .client import collect_from_center

_ncores = ThreadPool()._processes

log = print


class Worker(Server):
    """ Worker node in a distributed network

    Workers do the following:

    1.  Manage and serve from a dictionary of local data
    2.  Perform computations on that data and on data from peers
    3.  Interact with peers and with a ``Center`` node to acheive 2

    A worker should connect to a ``Center`` node.  It can run in an event loop
    or separately in a thread.

    Example
    -------

    Set up a Center on a separate machine

    >>> c = Center('192.168.0.100', 8000)  # doctest: +SKIP

    Run in an event loop

    >>> w = Worker('192.168.0.101', 8001,
    ...            center_ip='192.168.0.100', center_port=8000) # doctest: +SKIP
    >>> coroutine = w.go()  # doctest: +SKIP

    Can run separately in a thread

    >>> w = Worker('192.168.0.101', 8001,
    ...            center_ip='192.168.0.100', center_port=8000,
    ...            start=True, block=False)  # doctest: +SKIP
    """

    def __init__(self, ip, port, center_ip, center_port, ncores=None):
        self.ip = ip
        self.port = port
        self.center_ip = center_ip
        self.center_port = center_port
        self.ncores = ncores or _ncores
        self.data = dict()
        self.status = None
        self.executor = ThreadPoolExecutor(10)

        handlers = {'compute': self.work,
                    'get_data': self.get_data,
                    'update_data': self.update_data,
                    'delete_data': self.delete_data,
                    'terminate': self.terminate}

        super(Worker, self).__init__(handlers)
        log('Start worker')
        self.status = 'running'

    @gen.coroutine
    def _start(self):
        self.listen(self.port)
        resp = yield rpc(self.center_ip, self.center_port).register(
                ncores=self.ncores, address=(self.ip, self.port))
        assert resp == b'OK'
        log('Registered with center')

    def start(self):
        IOLoop.current().add_callback(self._start)

    @gen.coroutine
    def _close(self):
        yield rpc(self.center_ip, self.center_port).unregister(
                address=(self.ip, self.port))
        self.stop()
        self.status = 'closed'

    @gen.coroutine
    def terminate(self, stream):
        yield self._close()

    @property
    def address(self):
        return (self.ip, self.port)

    @gen.coroutine
    def work(self, stream, function=None, key=None, args=(), kwargs={}, needed=[]):
        """ Execute function """
        center = yield connect(self.center_ip, self.center_port)

        needed = [n for n in needed if n not in self.data]

        # Collect data from peers
        if needed:
            log("Collect data from peers: %s" % str(needed))
            other = yield collect_from_center(center, needed=needed)
            data2 = merge(self.data, dict(zip(needed, other)))
        else:
            data2 = self.data

        # Fill args with data, compute in separate thread
        args2 = keys_to_data(args, data2)
        kwargs2 = keys_to_data(kwargs, data2)
        try:
            job_counter[0] += 1
            i = job_counter[0]
            log("Start job %d: %s" % (i, function.__name__))
            result = yield self.executor.submit(function, *args2, **kwargs2)
            log("Finish job %d: %s" % (i, function.__name__))
            out_response = b'success'
        except Exception as e:
            result = e
            out_response = b'error'
        self.data[key] = result

        # Tell center about our new data
        response = yield rpc(center).add_keys(address=(self.ip, self.port),
                                              close=True, keys=[key])
        if not response == b'OK':
            log('Could not report results of work to center: ' + response.decode())

        raise Return(out_response)

    @gen.coroutine
    def update_data(self, stream, data=None, report=True):
        self.data.update(data)
        if report:
            yield rpc(self.center_ip, self.center_port).add_keys(
                    address=(self.ip, self.port), keys=list(data))
        raise Return(b'OK')


    @gen.coroutine
    def delete_data(self, stream, keys=None, report=True):
        for key in keys:
            if key in self.data:
                del self.data[key]
        if report:
            yield rpc(self.center_ip, self.center_port).remove_keys(
                    address=(self.ip, self.port), keys=keys)
        raise Return(b'OK')

    def get_data(self, stream, keys=None):
        return {k: self.data[k] for k in keys}


job_counter = [0]


def keys_to_data(o, data):
    """ Merge known data into tuple or dict

    >>> data = {'x': 1}
    >>> keys_to_data(('x', 'y'), data)
    (1, 'y')
    >>> keys_to_data({'a': 'x', 'b': 'y'}, data)
    {'a': 1, 'b': 'y'}
    """
    if isinstance(o, (tuple, list)):
        result = []
        for arg in o:
            try:
                result.append(data[arg])
            except (TypeError, KeyError):
                result.append(arg)
        result = type(o)(result)

    if isinstance(o, dict):
        result = {}
        for k, v in o.items():
            try:
                result[k] = data[v]
            except (TypeError, KeyError):
                result[k] = v
    return result
