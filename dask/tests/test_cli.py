from __future__ import annotations

import json
import pathlib
import platform
import sys

import click
import importlib_metadata
import pytest
import yaml
from click.testing import CliRunner

import dask
import dask.cli


def test_config_get_no_key():
    runner = CliRunner()
    result = runner.invoke(dask.cli.config_get)
    assert result.exit_code == 1
    assert result.output.startswith("Config key not specified")


def test_config_get_value():
    runner = CliRunner()
    result = runner.invoke(dask.cli.config_get, ["array"])
    assert result.exit_code == 0
    assert result.output.startswith("backend:")
    assert len(result.output.splitlines()) > 2


def test_config_get_bad_value():
    runner = CliRunner()
    result = runner.invoke(dask.cli.config_get, ["bad_key"])
    assert result.exit_code != 0
    assert result.output.startswith("Section not found")


def test_config_get_none():
    with dask.config.set({"foo.bar": None}):
        runner = CliRunner()
        result = runner.invoke(dask.cli.config_get, ["foo.bar"])
        assert result.exit_code == 0
        assert result.output == "None\n"


@pytest.fixture
def tmp_conf_dir(tmpdir):
    original = dask.config.PATH
    dask.config.PATH = str(tmpdir)
    try:
        yield tmpdir
    finally:
        dask.config.PATH = original


@pytest.mark.parametrize("empty_config", (True, False))
@pytest.mark.parametrize("value", ("333MiB", 2, [1, 2], {"foo": "bar"}))
def test_config_set_value(tmp_conf_dir, value, empty_config):
    config_file = pathlib.Path(tmp_conf_dir) / "dask.yaml"

    if not empty_config:
        expected_conf = {"dataframe": {"foo": "bar"}}
        config_file.write_text(yaml.dump(expected_conf))
    else:
        expected_conf = dict()
        assert not config_file.exists()

    runner = CliRunner()
    result = runner.invoke(
        dask.cli.config_set, ["array.chunk-size", str(value)], catch_exceptions=False
    )

    expected = (
        f"Updated [array.chunk-size] to [{value}], config saved to {config_file}\n"
    )
    assert expected == result.output

    actual_conf = yaml.safe_load(config_file.read_text())
    expected_conf.update({"array": {"chunk-size": value}})
    assert expected_conf == actual_conf


def test_config_list():
    runner = CliRunner()
    result = runner.invoke(dask.cli.config_list)
    assert result.exit_code == 0
    assert "array:" in result.output


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

    class ErrorEP:
        @property
        def name(self):
            return "foo"

        def load(self):
            raise ImportError("Entrypoint could not be imported")

    with pytest.warns(UserWarning, match="must be instances of"):
        _register_command_ep(dummy_cli, bad_ep)

    with pytest.warns(UserWarning, match="exception ocurred"):
        _register_command_ep(dummy_cli, ErrorEP())

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
