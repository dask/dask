from __future__ import print_function

import os
import itertools
import uuid
from datetime import datetime

import zmq
import dill
from .scheduler import pickle
from ..compatibility import unicode

context = zmq.Context()

jobids = ('schedule-%d' % i for i in itertools.count())


with open('log.client', 'w') as f:  # delete file
    pass

def log(*args):
    with open('log.client', 'a') as f:
        print(*args, file=f)



class Client(object):
    def __init__(self, scheduler, address=None):
        self.address_to_scheduler = scheduler
        if address == None:
            address = 'client-' + str(uuid.uuid1())
        if isinstance(address, unicode):
            address = address.encode()
        self.address = address
        self.socket = context.socket(zmq.DEALER)
        self.socket.setsockopt(zmq.IDENTITY, self.address)
        self.socket.connect(self.address_to_scheduler)
        self.register_client()

    def get(self, dsk, keys, keep_results=False):
        header = {'function': 'schedule',
                  'jobid': next(jobids)}
        payload = {'dask': dsk, 'keys': keys, 'keep_results': keep_results}

        self.send_to_scheduler(header, payload)
        header2, payload2 = self.recv_from_scheduler()

        if header2['status'] != 'OK':
            raise payload2['result']

        return payload2['result']

    def scheduler_status(self):
        header = {'function': 'status'}
        payload = {}
        self.send_to_scheduler(header, payload)

        header2, payload2 = self.recv_from_scheduler()
        return payload2

    def send_to_scheduler(self, header, payload):
        log(self.address, 'Send to scheduler', header)
        if 'address' not in header:
            header['address'] = self.address
        header['timestamp'] = datetime.utcnow()
        header['loads'] = dill.loads
        self.socket.send_multipart([pickle.dumps(header), dill.dumps(payload)])

    def recv_from_scheduler(self):
        header, payload = self.socket.recv_multipart()
        header = pickle.loads(header)
        loads = header.get('loads', pickle.loads)
        payload = loads(payload)
        log(self.address, 'Received from scheduler', header)
        return header, payload

    def send_recv(self, header, payload):
        self.send_to_scheduler(header, payload)
        return self.recv_from_scheduler()

    def set_collection(self, name, collection):
        """ Store collection in scheduler

        See docstring for get_collection
        """
        header = {'function': 'set-collection',
                  'loads': dill.loads}
        payload = {'type': type(collection),
                   'args': collection._args,
                   'name': name}

        header2, payload2 = self.send_recv(header, payload)

        assert header2['status'] == 'OK'

    def get_collection(self, name):
        """ Get stored collection from scheduler

        Clients may share collections with other clients by registering them
        with the centralized scheduler.

        >>> import dask.bag as db  # doctest: +SKIP
        >>> b = db.from_sequence(...).map(...).filter(...) # doctest: +SKIP

        >>> from dask.distributed import Client  # doctest: +SKIP
        >>> client = Client('tcp://scheduler-hostname:5555')  # doctest: +SKIP
        >>> client.set_collection('mybag', b)  # doctest: +SKIP

        Other clients can connect to the same scheduler to collect that
        collection.

        >>> client2 = Client('tcp://scheduler-hostname:5555')  # doctest: +SKIP
        >>> b2 = client2.get_collection('mybag')  # doctest: +SKIP
        """
        header = {'function': 'get-collection'}
        payload = {'name': name}

        header2, payload2 = self.send_recv(header, payload)

        return payload2['type'](*payload2['args'])

    def close_scheduler(self):
        header = {'function': 'close'}
        self.send_to_scheduler(header, {})

    def close(self, close_scheduler=False):
        if close_scheduler:
            self.close_scheduler()
        self.socket.close(1)

    def register_client(self):
        header = {'function': 'register'}
        payload = {'pid': os.getpid()}
        header2, payload2 = self.send_recv(header, payload)
        self.registered_workers = payload2['workers']

    def get_registered_workers(self):
        self.send_to_scheduler({'function': 'get_workers'}, {})
        header, payload = self.recv_from_scheduler()
        return payload['workers']
