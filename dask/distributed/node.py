from __future__ import print_function

from zmqompute import ComputeNode
from threading import Thread
from multiprocessing.pool import ThreadPool
import multiprocessing
import zmq
import dask
from toolz import partial, get
from time import time
import sys
try:
    from cPickle import dumps, loads, HIGHEST_PROTOCOL
except ImportError:
    from pickle import dumps, loads, HIGHEST_PROTOCOL


DEBUG = True

context = zmq.Context()

class Worker(object):
    """ Asynchronous worker in a distributed dask computation pool


    See Also
    --------

    """
    def __init__(self, address, data, nthreads=100,
                 dumps=partial(dumps, protocol=HIGHEST_PROTOCOL),
                 loads=loads):
        self.data = data
        self.pool = ThreadPool(100)
        self.dumps = dumps
        self.loads = loads

        if '://' not in address:
            address = 'tcp://' + address
        self.address = address

        self.router = context.socket(zmq.ROUTER)
        self.router.setsockopt(zmq.IDENTITY, address)
        self.router.bind(address)

        self.functions = {'status': status,
                          'getitem': self.data.__getitem__,
                          'setitem': self.data.__setitem__,
                          'delitem': self.data.__delitem__}

        self._listen_thread = Thread(target=self.listen)
        self._listen_thread.start()

    def execute_and_reply(self, address, jobid, func, args, kwargs):
        try:
            result = func(*args, **kwargs)
            status = 'OK'
        except Exception as e:
            result = e
            status = 'Error'
        payload = self.dumps({'result': result,
                              'address': self.address,
                              'jobid': jobid,
                              'status': status})
        self.router.send_multipart([address, '', payload])  # TODO, send job id

    def listen(self):
        while True:
            address, empty, payload = self.router.recv_multipart()
            if payload == b'close':
                break

            payload2 = self.loads(payload)
            func = payload2['function']
            assert func in self.functions
            func = self.functions[func]

            jobid = payload2.get('jobid', None)
            args = payload2.get('args', ())
            if not isinstance(args, tuple):
                args = (args,)
            kwargs = payload2.get('kwargs', dict())

            future = self.pool.apply_async(self.execute_and_reply,
                                  args=(address, jobid, func, args, kwargs))


    def close(self):
        if self.pool._state == multiprocessing.pool.RUN:
            req = context.socket(zmq.REQ)
            req.connect(self.address)
            req.send(b'close')
            self.pool.close()
            self.pool.join()

    def __del__(self):
        self.close()


def status():
    return 'OK'

def ishashable(x):
    try:
        hash(x)
        return True
    except TypeError:
        return False
