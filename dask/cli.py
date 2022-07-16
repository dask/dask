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
def cli():
    """Dask command line interface."""
    pass


@cli.command
def docs():
    """Open Dask documentation in a web browser."""
    import webbrowser

    webbrowser.open("https://docs.dask.org")


@cli.command
def info():
    """Print versions of Dask related projects."""
    from dask.utils import show_versions

    show_versions()


def register_third_party(cli):
    import pkg_resources

    for ep in pkg_resources.iter_entry_points(group="dask_cli"):
        cli.add_command(ep.load())


def run_cli():
    register_third_party(cli)
    cli()
