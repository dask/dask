import json
import platform
import sys

import click
import importlib_metadata
import pytest
from click.testing import CliRunner

import dask
import dask.cli


def test_config_get():
    runner = CliRunner()
    result = runner.invoke(dask.cli.config_get)
    assert result.exit_code == 1
    assert result.output.startswith("Config key not specified")


def test_config_get_value():
    runner = CliRunner()
    result = runner.invoke(dask.cli.config_get, ["array"])
    assert result.exit_code == 0
    assert result.output.startswith("backend:")


def test_config_get_bad_value():
    runner = CliRunner()
    result = runner.invoke(dask.cli.config_get, ["bad_key"])
    assert result.exit_code != 0
    assert result.output.startswith("Section not found")


def test_config_list():
    runner = CliRunner()
    result = runner.invoke(dask.cli.config_list)
    assert result.exit_code == 0
    assert result.output.startswith("array:")


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
        from distributed import __version__ as distributed_version
    except ImportError:
        distributed_version = None

    assert table["distributed"] == distributed_version


@click.group()
def dummy_cli():
    pass


def bad_command():
    pass


@click.command(name="good")
def good_command():
    pass


@click.command(name="good")
def good_command_2():
    pass


def test_register_command_ep():
    from dask.cli import _register_command_ep

    bad_ep = importlib_metadata.EntryPoint(
        name="bad",
        value="dask.tests.test_cli:bad_command",
        group="dask_cli",
    )

    good_ep = importlib_metadata.EntryPoint(
        name="good",
        value="dask.tests.test_cli:good_command",
        group="dask_cli",
    )

    with pytest.warns(UserWarning, match="must be instances of"):
        _register_command_ep(dummy_cli, bad_ep)

    _register_command_ep(dummy_cli, good_ep)
    assert "good" in dummy_cli.commands
    assert dummy_cli.commands["good"] is good_command


@click.group()
def dummy_cli_2():
    pass


def test_repeated_name_registration_warn():
    from dask.cli import _register_command_ep

    one = importlib_metadata.EntryPoint(
        name="one",
        value="dask.tests.test_cli:good_command",
        group="dask_cli",
    )

    two = importlib_metadata.EntryPoint(
        name="two",
        value="dask.tests.test_cli:good_command_2",
        group="dask_cli",
    )

    _register_command_ep(dummy_cli_2, one)
    with pytest.warns(UserWarning, match="While registering the command with name"):
        _register_command_ep(dummy_cli_2, two)
