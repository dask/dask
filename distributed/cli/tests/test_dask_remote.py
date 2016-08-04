from click.testing import CliRunner
from distributed.cli.dask_remote import main


def test_dask_remote():
    runner = CliRunner()
    result = runner.invoke(main, ['--help'])
    assert '--host TEXT     IP or hostname of this server' in result.output
    assert result.exit_code == 0
