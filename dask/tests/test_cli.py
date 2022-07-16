from click.testing import CliRunner

from dask import __version__
from dask.cli import cli


def test_version():
    runner = CliRunner()
    result = runner.invoke(cli, ["--version"])
    assert result.exit_code == 0
    assert result.output == f"cli, version {__version__}\n"
