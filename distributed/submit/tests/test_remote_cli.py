from mock import Mock
from click.testing import CliRunner
from tornado.ioloop import IOLoop

from distributed.submit.remote_cli import remote, _remote
from distributed.submit.remote_client import RemoteClient


def test_dask_remote():
    runner = CliRunner()
    result = runner.invoke(remote, ['--help'])
    assert '--host TEXT     IP or hostname of this server' in result.output
    assert result.exit_code == 0


def test_cli_runs_remote_client():
    mock_remote_client = Mock(spec=RemoteClient)
    mock_ioloop = Mock(spec=IOLoop.current())

    _remote('127.0.0.1:8799', 8788, loop=mock_ioloop, client=mock_remote_client)

    mock_remote_client.assert_called_once_with(ip='127.0.0.1', loop=mock_ioloop)
    mock_remote_client().start.assert_called_once_with(port=8799)

    assert mock_ioloop.start.called
    assert mock_ioloop.close.called
    assert mock_remote_client().stop.called
