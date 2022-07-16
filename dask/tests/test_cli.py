import pytest

click = pytest.importorskip("click")

from click.testing import CliRunner

import dask
from dask.cli import cli


def test_version():
    runner = CliRunner()
    result = runner.invoke(cli, ["--version"])
    assert result.exit_code == 0
    assert result.output == f"cli, version {dask.__version__}\n"
