from dask import __version__

try:
    import click
except ImportError as e:
    msg = (
        "The Dask CLI requires click to be installed.\n\n"
        "Install with conda or pip:\n\n"
        " conda install click\n"
        " pip install click\n"
    )
    raise ImportError(msg) from e


@click.group
@click.version_option(__version__)
def cli() -> None:
    """Dask command line interface."""
    pass


@cli.command
def docs() -> None:
    """Open Dask documentation in a web browser."""
    import webbrowser

    webbrowser.open("https://docs.dask.org")


@cli.group("info")
def info() -> None:
    """Information about your dask installation."""
    pass


@info.command()
def versions() -> None:
    """Print versions of Dask related projects."""
    from dask.utils import show_versions

    show_versions()


def register_third_party(cli: click.Group) -> None:
    """Discover third party dask_cli entry points.

    If a package includes the "dask_cli" entry point category, this
    function discovers and loads the associated entry points with
    ``importlib.metadata``. We only consider ``click.Command`` and
    ``click.Group`` instances to be valid entry points for the
    `dask_cli` category.

    """
    import importlib.metadata

    eps = importlib.metadata.entry_points()

    for ep in eps.select(group="dask_cli"):
        command = ep.load()
        if not isinstance(command, (click.Command, click.Group)):
            raise TypeError(
                "entry points in 'dask_cli' must be instances of "
                "click.Command or click.Group"
            )
        cli.add_command(command)


def run_cli() -> None:
    register_third_party(cli)
    cli()
