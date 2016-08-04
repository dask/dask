import sys
import click
from tornado import gen
from tornado.ioloop import IOLoop
from distributed.cli.utils import check_python_3, install_signal_handlers
from distributed.submit import _submit


@click.command()
@click.argument('remote_client_address', type=str, required=True)
@click.argument('filepath', type=str, required=True)
def main(remote_client_address, filepath):
    @gen.coroutine
    def f():
        stdout, stderr = yield _submit(remote_client_address, filepath)
        if stdout:
            sys.stdout.write(str(stdout))
        if stderr:
            sys.stderr.write(str(stderr))

    IOLoop.instance().run_sync(f)


def go():
    install_signal_handlers()
    check_python_3()
    main()


if __name__ == '__main__':
    go()
