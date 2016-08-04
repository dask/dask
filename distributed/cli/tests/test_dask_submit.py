from click.testing import CliRunner
from distributed.cli.dask_submit import main


def test_submit_runs_as_a_cli():
    runner = CliRunner()
    result = runner.invoke(main, ['--help'])
    assert result.exit_code == 0
    assert 'Usage: main [OPTIONS] REMOTE_CLIENT_ADDRESS FILEPATH' in result.output
