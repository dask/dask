import os

from tornado import gen

from distributed import rpc
from distributed.submit import RemoteClient
from distributed.utils_test import (loop, valid_python_script,
                                    invalid_python_script)  # flake8: noqa


def test_remote_client_uploads_a_file(loop, tmpdir):
    @gen.coroutine
    def test():
        remote_client = RemoteClient(ip='127.0.0.1', local_dir=str(tmpdir))
        yield remote_client._start(0)
        remote_process = rpc(remote_client.address)
        upload = yield remote_process.upload_file(filename='script.py', file_payload='x=1')

        assert upload == {'status': 'OK', 'nbytes': 3}
        assert tmpdir.join('script.py').read() == "x=1"

        yield remote_client._close()

    loop.run_sync(test, timeout=5)


def test_remote_client_execution_outputs_to_stdout(loop, tmpdir):
    @gen.coroutine
    def test():
        remote_client = RemoteClient(ip='127.0.0.1', local_dir=str(tmpdir))
        yield remote_client._start(0)
        rr = rpc(remote_client.address)
        yield rr.upload_file(filename='script.py', file_payload='print("hello world!")')

        message = yield rr.execute(filename='script.py')
        assert message['stdout'] == b'hello world!' + os.linesep.encode()
        assert message['returncode'] == 0

        yield remote_client._close()

    loop.run_sync(test, timeout=5)


def test_remote_client_execution_outputs_stderr(loop, tmpdir, invalid_python_script):
    @gen.coroutine
    def test():
        remote_client = RemoteClient(ip='127.0.0.1', local_dir=str(tmpdir))
        yield remote_client._start(0)
        rr = rpc(remote_client.address)
        yield rr.upload_file(filename='script.py', file_payload='a+1')

        message = yield rr.execute(filename='script.py')
        assert b'\'a\' is not defined' in message['stderr']
        assert message['returncode'] == 1

        yield remote_client._close()

    loop.run_sync(test, timeout=5)
