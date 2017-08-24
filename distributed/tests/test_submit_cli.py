from __future__ import print_function, division, absolute_import
from mock import Mock

from tornado import gen
from tornado.ioloop import IOLoop
from distributed.submit import RemoteClient, _submit, _remote
from distributed.utils_test import (valid_python_script, invalid_python_script,
                                    loop)  # flake8: noqa


def test_dask_submit_cli_writes_result_to_stdout(loop, tmpdir,
                                                 valid_python_script):
    @gen.coroutine
    def test():
        remote_client = RemoteClient(ip='127.0.0.1', local_dir=str(tmpdir))
        yield remote_client._start()

        out, err = yield _submit('127.0.0.1:{0}'.format(remote_client.port),
                                 str(valid_python_script))
        assert b'hello world!' in out
        yield remote_client._close()

    loop.run_sync(test, timeout=5)


def test_dask_submit_cli_writes_traceback_to_stdout(loop, tmpdir,
                                                    invalid_python_script):
    @gen.coroutine
    def test():
        remote_client = RemoteClient(ip='127.0.0.1', local_dir=str(tmpdir))
        yield remote_client._start()

        out, err = yield _submit('127.0.0.1:{0}'.format(remote_client.port),
                                 str(invalid_python_script))
        assert b'Traceback' in err
        yield remote_client._close()

    loop.run_sync(test, timeout=5)


def test_cli_runs_remote_client():
    mock_remote_client = Mock(spec=RemoteClient)
    mock_ioloop = Mock(spec=IOLoop.current())

    _remote('127.0.0.1:8799', 8788, loop=mock_ioloop, client=mock_remote_client)

    mock_remote_client.assert_called_once_with(ip='127.0.0.1', loop=mock_ioloop)
    mock_remote_client().start.assert_called_once_with(port=8799)

    assert mock_ioloop.start.called
    assert mock_ioloop.close.called
    assert mock_remote_client().stop.called
