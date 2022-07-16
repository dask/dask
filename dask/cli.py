from __future__ import annotations

try:
    import click
except ImportError as e:
    msg = (
        "The Dask CLI requires click to be installed.\n\n"
        "Install with conda or pip:\n\n"
        " conda install click"
        " pip install click"
    )
    raise ImportError


@click.group()
def cli():
    """Top Level CLI function."""
    pass


def register_third_party(cli):
    import pkg_resources

    for ep in pkg_resources.iter_entry_points(group="dask_cli"):
        cli.add_command(ep.load())


def run_cli():
    register_third_party(cli)
    cli()
