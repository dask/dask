from __future__ import print_function

from zmqompute import ComputeNode
from threading import Thread, Lock
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

def log(*args):
    pass


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

        self.lock = Lock()

        self.functions = {'status': status,
                          'collect': self.collect,
                          'getitem': self.data.__getitem__,
                          'setitem': self.data.__setitem__,
                          'delitem': self.data.__delitem__}

        self._listen_thread = Thread(target=self.listen)
        self._listen_thread.start()

    def execute_and_reply(self, address, jobid, func, args, kwargs):
        """ Execute function, return result

        This is intended to be run asynchronously in a separate thread
        Returns the result of calling func(*args, **kwargs) to the given
        address along with the given jobid.  The jobid is to help the recipient
        of the result figure out what data this corresponds to.
        """
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
        log('Computed and returning result:', address, payload)
        with self.lock:
            self.router.send_multipart([address, '', payload])  # TODO, send job id

    def listen(self):
        """
        Main event loop - listen for requests and dispatch to worker functions

        We expect requests like what a REQ sends out

            Address
            -empty-
            Payload

        The payload should deserialize into a dict of the following form:

            {'function': name of function to call, see self.functions,
             'jobid': job identifier, defaults to None,
             'args': arguments to pass to function, defaults to (),
             'kwargs': keyword argument dict, defauls to {}}

        So the minimal request would be as follows:

        >>> sock = context.socket(zmq.REQ)  # doctest: +SKIP
        >>> sock.connect('tcp://my-address')  # doctest: +SKIP
        >>> sock.send(dumps({'function': 'status'}))  # doctest: +SKIP

        Or a more complex packet might be as follows:

        >>> sock.send(dumps({'function': 'setitem',
        ...                  'args': ('x', 10),
        ...                  'jobid': 123}))  # doctest: +SKIP

        The function strings check against ``self.functions``.  They are run
        asynchronously using a thread from ``self.pool``.  Their results are
        sent back to the sender (see ``execute_and_reply``.)
        """
        while True:
            # Wait on request
            address, empty, payload = self.router.recv_multipart()

            if payload == b'close':
                break

            # Unpack payload
            payload2 = self.loads(payload)
            log("Received payload: ", self.address, payload2)
            func = payload2['function']
            assert func in self.functions
            func = self.functions[func]
            jobid = payload2.get('jobid', None)
            args = payload2.get('args', ())
            if not isinstance(args, tuple):
                args = (args,)
            kwargs = payload2.get('kwargs', dict())

            # Execute job in thread
            future = self.pool.apply_async(self.execute_and_reply,
                                  args=(address, jobid, func, args, kwargs))

    def collect(self, locations):
        """ Collect data from peers

        Given a dictionary of desired data and who holds that data

        >>> locations = {'x': ['tcp://alice:5000', 'tcp://bob:5000'],
        ...              'y': ['tcp://bob:5000']}

        This fires off getitem reqeusts to one of the hosts for each piece of
        data then blocks on all of the responses, then inserts this data into
        ``self.data``.
        """
        socks = []

        log('Collecting data from peers:', self.address, locations)
        # Send out requests for data
        for key, locs in locations.items():
            sock = context.socket(zmq.REQ)
            sock.connect(locs[0])
            payload = {'function': 'getitem',
                       'args': (key,),
                       'jobid': key}
            sock.send(self.dumps(payload))
            socks.append(sock)

        # Wait on replies.  Store results in self.data.
        for sock in socks:
            payload = self.loads(sock.recv())
            log('Received data:', self.address, payload)
            self.data[payload['jobid']] = payload['result']

    def close(self):
        if self.pool._state == multiprocessing.pool.RUN:
            log('Closing', self.address)
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
