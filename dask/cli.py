try:
    import click
except ImportError as e:
    msg = (
        "The Dask CLI requires click to be installed.\n\n"
        "Install with conda or pip:\n\n"
        " conda install click"
        " pip install click"
    )
    raise ImportError(msg) from e


@click.group
def cli():
    """Dask command line interface."""
    pass


@cli.command
def version():
    """Print installed Dask version."""
    from dask import __version__ as v

    print(v)


@cli.command
def versions():
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
