import pytest

click = pytest.importorskip("click")

import importlib.metadata
import json
import platform
import sys

from click.testing import CliRunner

import dask
import dask.cli


def test_version():
    runner = CliRunner()
    result = runner.invoke(dask.cli.cli, ["--version"])
    assert result.exit_code == 0
    assert result.output == f"cli, version {dask.__version__}\n"


def test_info_versions():
    runner = CliRunner()
    result = runner.invoke(dask.cli.versions)
    assert result.exit_code == 0

    # $ dask info versions
    # will print to stdout a json like struct, so result.output can be
    # loaded with json.
    table = json.loads(result.output)

    assert table["Python"] == ".".join(str(x) for x in sys.version_info[:3])
    assert table["dask"] == dask.__version__
    assert table["Platform"] == platform.uname().system

    try:
        distributed_version = importlib.metadata.version("distributed")
    except importlib.metadata.PackageNotFoundError:
        distributed_version = None

    assert table["distributed"] == distributed_version
