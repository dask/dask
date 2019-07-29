import click
from distributed.cli.utils import check_python_3, install_signal_handlers
from distributed.submit import _remote


@click.command()
@click.option("--host", type=str, default=None, help="IP or hostname of this server")
@click.option(
    "--port", type=int, default=8788, show_default=True, help="Remote Client Port"
)
@click.version_option()
def main(host, port):
    _remote(host, port)


def go():
    install_signal_handlers()
    check_python_3()
    main()


if __name__ == "__main__":
    go()
