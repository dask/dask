from __future__ import print_function, division, absolute_import

import logging
import os
import socket
import subprocess
import tempfile
import sys

from tornado import gen

from tornado.ioloop import IOLoop

from distributed import rpc
from distributed.compatibility import unicode
from distributed.core import Server
from distributed.security import Security
from distributed.utils import get_ip


logger = logging.getLogger('distributed.remote')


class RemoteClient(Server):
    def __init__(self, ip=None, local_dir=tempfile.mkdtemp(prefix='client-'),
                 loop=None, security=None, **kwargs):
        self.ip = ip or get_ip()
        self.loop = loop or IOLoop.current()
        self.local_dir = local_dir
        handlers = {'upload_file': self.upload_file, 'execute': self.execute}

        self.security = security or Security()
        assert isinstance(self.security, Security)
        self.listen_args = self.security.get_listen_args('scheduler')

        super(RemoteClient, self).__init__(handlers, io_loop=self.loop, **kwargs)

    @gen.coroutine
    def _start(self, port=0):
        self.listen(port, listen_args=self.listen_args)

    def start(self, port=0):
        self.loop.add_callback(self._start, port)
        logger.info("Remote Client is running at {0}:{1}".format(self.ip, port))

    @gen.coroutine
    def execute(self, stream=None, filename=None):
        script_path = os.path.join(self.local_dir, filename)
        cmd = '{0} {1}'.format(sys.executable, script_path)
        process = subprocess.Popen(cmd, shell=True,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        out, err = process.communicate()
        return_code = process.returncode
        raise gen.Return({'stdout': out, 'stderr': err,
                          'returncode': return_code})

    def upload_file(self, stream, filename=None, file_payload=None):
        out_filename = os.path.join(self.local_dir, filename)
        if isinstance(file_payload, unicode):
            file_payload = file_payload.encode()
        with open(out_filename, 'wb') as f:
            f.write(file_payload)
        return {'status': 'OK', 'nbytes': len(file_payload)}

    @gen.coroutine
    def _close(self):
        self.stop()


def _remote(host, port, loop=IOLoop.current(), client=RemoteClient):
    host = host or get_ip()
    if ':' in host and port == 8788:
        host, port = host.rsplit(':', 1)
        port = int(port)
    ip = socket.gethostbyname(host)
    remote_client = client(ip=ip, loop=loop)
    remote_client.start(port=port)
    loop.start()
    loop.close()
    remote_client.stop()
    logger.info("End remote client at %s:%d", host, port)


@gen.coroutine
def _submit(remote_client_address, filepath, connection_args=None):
    rc = rpc(remote_client_address, connection_args=connection_args)
    remote_file = os.path.basename(filepath)
    with open(filepath, 'rb') as f:
        bytes_read = f.read()
    yield rc.upload_file(filename=remote_file, file_payload=bytes_read)
    result = yield rc.execute(filename=remote_file)
    raise gen.Return((result['stdout'], result['stderr']))
