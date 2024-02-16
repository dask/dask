from __future__ import annotations

import pathlib
import warnings
from functools import reduce

import click
import importlib_metadata
import yaml

import dask
from dask import __version__

CONTEXT_SETTINGS = {
    "help_option_names": ["-h", "--help"],
    "max_content_width": 88,
}


@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option(__version__)
def cli():
    """Dask command line interface."""
    pass


@cli.command()
def docs():
    """Open Dask documentation (https://docs.dask.org/) in a web browser."""
    import webbrowser

    webbrowser.open("https://docs.dask.org")


@cli.group()
def info():
    """Information about your dask installation."""
    pass


@info.command()
def versions():
    """Print versions of Dask related projects."""
    from dask.utils import show_versions

    show_versions()


@cli.group()
def config():
    """Dask config settings"""
    pass


@config.command(name="get")
@click.argument("key", default=None, required=False)
def config_get(key=None):
    """Print config key, or the whole config."""
    if key in (None, ""):
        click.echo(
            click.style(
                """Config key not specified. Are you looking for "dask config list"?"""
            ),
            err=True,
        )
        exit(1)
    else:
        try:
            data = reduce(lambda d, k: d[k], key.split("."), dask.config.config)
            if isinstance(data, (list, dict)):
                click.echo_via_pager(yaml.dump(data))
            elif data is None:
                click.echo("None")
            else:
                click.echo(data)
        except KeyError:
            click.echo(click.style(f"Section not found: {key}", fg="red"), err=True)
            exit(1)


@config.command(name="set")
@click.argument("key", default=None, required=False)
@click.argument("value", default=None, required=False)
def config_set(key=None, value=None):
    """Set a Dask config key to a new value"""
    if key in (None, ""):
        click.echo(
            click.style(
                """Config key not specified. Are you looking for "dask config list"?"""
            ),
            err=True,
        )
        exit(1)
    else:
        value = dask.config.interpret_value(value)
        _, path = save_config({key: value})
        click.echo(f"Updated [{key}] to [{value}], config saved to {path}")


def save_config(new_config: dict) -> tuple[dict, pathlib.Path]:
    """
    Save new config values to dask config file,
    return new config and path to file.
    """
    config_file = pathlib.Path(dask.config.PATH).joinpath("dask.yaml")
    config_file.parent.mkdir(exist_ok=True, parents=True)
    if config_file.exists():
        with open(str(config_file)) as f:
            config = yaml.safe_load(f) or dict()
    else:
        config = dict()

    dask.config.set(new_config, config=config)

    try:
        config_file.write_text(yaml.dump(config))
    except OSError as e:
        raise RuntimeError(
            f"""

For some reason we couldn't write config to {config_file}.
Perhaps you don't have permissions here?

You can change the directory where Dask writes config files
to somewhere else using the DASK_CONFIG environment variable.

For example, you could set the following:

    export DASK_CONFIG=~/.dask
"""
        ) from e

    return config, config_file


@config.command(name="list")
def config_list():
    """Print the whole config."""
    click.echo_via_pager(yaml.dump(dask.config.config))


def _register_command_ep(interface, entry_point):
    """Add `entry_point` command to `interface`.

    Parameters
    ----------
    interface : click.Command or click.Group
        The click interface to augment with `entry_point`.
    entry_point : importlib.metadata.EntryPoint
        The entry point which loads to a ``click.Command`` or
        ``click.Group`` instance to be added as a sub-command or
        sub-group in `interface`.

    """
    try:
        command = entry_point.load()
    except Exception as e:
        warnings.warn(
            f"While registering the command with name '{entry_point.name}', an "
            f"exception ocurred; {e}."
        )
        return
    if not isinstance(command, (click.Command, click.Group)):
        warnings.warn(
            "entry points in 'dask_cli' must be instances of "
            f"click.Command or click.Group, not {type(command)}."
        )
        return

    if command.name in interface.commands:
        warnings.warn(
            f"While registering the command with name '{command.name}', an "
            "existing command or group; the original has been overwritten."
        )

    interface.add_command(command)


def run_cli():
    """Run the dask command line interface."""

    # discover "dask_cli" entry points and try to register them to the
    # top level `cli`.
    for ep in importlib_metadata.entry_points(group="dask_cli"):
        _register_command_ep(cli, ep)

    cli()
