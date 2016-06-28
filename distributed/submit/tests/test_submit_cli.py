from __future__ import print_function, division, absolute_import
from click.testing import CliRunner
from tornado import gen
from distributed.submit.remote_client import RemoteClient
from distributed.submit.submit_cli import _submit, submit
from distributed.utils_test import valid_python_script, invalid_python_script, current_loop


def test_dask_submit_cli_writes_result_to_stdout(capsys, current_loop, tmpdir, valid_python_script):
    @gen.coroutine
    def test():
        remote_client = RemoteClient(ip='127.0.0.1', local_dir=str(tmpdir))
        yield remote_client._start()

        yield _submit('127.0.0.1:{0}'.format(remote_client.port), str(valid_python_script))
        out, err = capsys.readouterr()
        assert 'hello world!' in out
        yield remote_client._close()

    current_loop.run_sync(test, timeout=5)


def test_dask_submit_cli_writes_traceback_to_stdout(capsys, current_loop, tmpdir, invalid_python_script):
    @gen.coroutine
    def test():
        remote_client = RemoteClient(ip='127.0.0.1', local_dir=str(tmpdir))
        yield remote_client._start()

        yield _submit('127.0.0.1:{0}'.format(remote_client.port), str(invalid_python_script))
        out, err = capsys.readouterr()
        assert 'Traceback' in err
        yield remote_client._close()

    current_loop.run_sync(test, timeout=5)


def test_submit_runs_as_a_cli():
    runner = CliRunner()
    result = runner.invoke(submit, ['--help'])
    assert result.exit_code == 0
    assert 'Usage: submit [OPTIONS] REMOTE_CLIENT_ADDRESS FILEPATH' in result.output
