import warnings

import click
import importlib_metadata

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


@config.command()
@click.argument("key", default=None, required=False)
def get(key=None):
    """Print config key, or the whole config."""
    from functools import reduce

    from yaml import dump

    from dask.config import config

    if key is None:
        click.echo_via_pager(dump(config))
    else:
        try:
            data = reduce(lambda d, k: d[k], key.split("."), config)
            if isinstance(data, (list, dict)):
                click.echo_via_pager(dump(data))
            else:
                click.echo(data)
        except KeyError:
            click.echo(click.style(f"Section not found: {key}", fg="red"), err=True)


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
    command = entry_point.load()
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
