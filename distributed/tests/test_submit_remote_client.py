from tornado import gen

from distributed import rpc
from distributed.submit import RemoteClient
from distributed.utils_test import current_loop, valid_python_script,\
    invalid_python_script


def test_remote_client_uploads_a_file(current_loop, tmpdir, ):
    @gen.coroutine
    def test():
        remote_client = RemoteClient(ip='127.0.0.1', local_dir=str(tmpdir))
        yield remote_client._start(0)
        remote_process = rpc(ip='127.0.0.1', port=remote_client.port)
        upload = yield remote_process.upload_file(filename='script.py', file_payload='x=1')

        assert upload == {'status': 'OK', 'nbytes': 3}
        assert tmpdir.join('script.py').read() == "x=1"

        yield remote_client._close()

    current_loop.run_sync(test, timeout=5)


def test_remote_client_execution_outputs_to_stdout(current_loop, tmpdir):
    @gen.coroutine
    def test():
        remote_client = RemoteClient(ip='127.0.0.1', local_dir=str(tmpdir))
        yield remote_client._start(0)
        rr = rpc(ip='127.0.0.1', port=remote_client.port)
        yield rr.upload_file(filename='script.py', file_payload='print("hello world!")')

        message = yield rr.execute(filename='script.py')
        assert message['stdout'] == b'hello world!\n'
        assert message['returncode'] == 0

        yield remote_client._close()

    current_loop.run_sync(test, timeout=5)


def test_remote_client_execution_outputs_stderr(current_loop, tmpdir, invalid_python_script):
    @gen.coroutine
    def test():
        remote_client = RemoteClient(ip='127.0.0.1', local_dir=str(tmpdir))
        yield remote_client._start(0)
        rr = rpc(ip='127.0.0.1', port=remote_client.port)
        yield rr.upload_file(filename='script.py', file_payload='a+1')

        message = yield rr.execute(filename='script.py')
        assert b'\'a\' is not defined' in message['stderr']
        assert message['returncode'] == 1

        yield remote_client._close()

    current_loop.run_sync(test, timeout=5)
