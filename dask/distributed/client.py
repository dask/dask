from __future__ import print_function

import zmq
import itertools
import uuid
from datetime import datetime
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

    def get(self, dsk, keys):
        header = {'function': 'schedule',
                  'jobid': next(jobids)}
        payload = {'dask': dsk, 'keys': keys}

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
        self.socket.send_multipart([dill.dumps(header), dill.dumps(payload)])

    def recv_from_scheduler(self):
        header, payload = self.socket.recv_multipart()
        header = pickle.loads(header)
        serializer = header.get('serializer', pickle)
        payload = serializer.loads(payload)
        log(self.address, 'Received from scheduler', header)
        return header, payload
