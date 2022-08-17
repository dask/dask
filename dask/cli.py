from dask import __version__
from dask.compatibility import entry_points

try:
    import click
except ImportError as e:
    msg = (
        "The dask.cli module requires click to be installed.\n\n"
        "Install with conda or pip:\n\n"
        " conda install click\n"
        " pip install click\n"
    )
    raise ImportError(msg) from e


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
        raise TypeError(
            "entry points in 'dask_cli' must be instances of "
            "click.Command or click.Group"
        )
    interface.add_command(command)


def run_cli():
    """Run the dask command line interface."""

    # discover "dask_cli" entry points and try to register them to the
    # top level `cli`.
    for ep in entry_points(group="dask_cli"):
        _register_command_ep(cli, ep)

    cli()
