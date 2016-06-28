import os
import sys
import click
from tornado import gen
from tornado.ioloop import IOLoop
from distributed import rpc


@gen.coroutine
def _submit(remote_client_address, filepath):
    rc = rpc(addr=remote_client_address)
    remote_file = os.path.basename(filepath)
    with open(filepath, 'rb') as f:
        bytes_read = f.read()
    yield rc.upload_file(filename=remote_file, file_payload=bytes_read)
    result = yield rc.execute(filename=remote_file)
    if result['stdout']:
        sys.stdout.write(str(result['stdout']))
    if result['stderr']:
        sys.stderr.write(str(result['stderr']))


@click.command()
@click.argument('remote_client_address', type=str, required=True)
@click.argument('filepath', type=str, required=True)
def submit(remote_client_address, filepath):
    @gen.coroutine
    def f():
        yield _submit(remote_client_address, filepath)

    IOLoop.instance().run_sync(f)
