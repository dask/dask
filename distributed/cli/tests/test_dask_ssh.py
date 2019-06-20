from click.testing import CliRunner
from distributed.cli.dask_ssh import main


def test_version_option():
    runner = CliRunner()
    result = runner.invoke(main, ["--version"])
    assert result.exit_code == 0
