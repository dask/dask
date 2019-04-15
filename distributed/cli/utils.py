from __future__ import print_function, division, absolute_import

from tornado import gen
from tornado.ioloop import IOLoop

from distributed.comm import (
    parse_address,
    unparse_address,
    parse_host_port,
    unparse_host_port,
)


py3_err_msg = """
Warning: Your terminal does not set locales.

If you use unicode text inputs for command line options then this may cause
undesired behavior.  This is rare.

If you don't use unicode characters in command line options then you can safely
ignore this message.  This is the common case.

You can support unicode inputs by specifying encoding environment variables,
though exact solutions may depend on your system:

    $ export LC_ALL=C.UTF-8
    $ export LANG=C.UTF-8

For more information see: http://click.pocoo.org/5/python3/
""".lstrip()


def check_python_3():
    """Ensures that the environment is good for unicode on Python 3."""
    # https://github.com/pallets/click/issues/448#issuecomment-246029304
    import click.core

    click.core._verify_python3_env = lambda: None

    try:
        from click import _unicodefun

        _unicodefun._verify_python3_env()
    except (TypeError, RuntimeError) as e:
        import click

        click.echo(py3_err_msg, err=True)


def install_signal_handlers(loop=None, cleanup=None):
    """
    Install global signal handlers to halt the Tornado IOLoop in case of
    a SIGINT or SIGTERM.  *cleanup* is an optional callback called,
    before the loop stops, with a single signal number argument.
    """
    import signal

    loop = loop or IOLoop.current()

    old_handlers = {}

    def handle_signal(sig, frame):
        @gen.coroutine
        def cleanup_and_stop():
            try:
                if cleanup is not None:
                    yield cleanup(sig)
            finally:
                loop.stop()

        loop.add_callback_from_signal(cleanup_and_stop)
        # Restore old signal handler to allow for a quicker exit
        # if the user sends the signal again.
        signal.signal(sig, old_handlers[sig])

    for sig in [signal.SIGINT, signal.SIGTERM]:
        old_handlers[sig] = signal.signal(sig, handle_signal)


def uri_from_host_port(host_arg, port_arg, default_port):
    """
    Process the *host* and *port* CLI options.
    Return a URI.
    """
    # Much of distributed depends on a well-known IP being assigned to
    # each entity (Worker, Scheduler, etc.), so avoid "universal" addresses
    # like '' which would listen on all registered IPs and interfaces.
    scheme, loc = parse_address(host_arg or "")

    host, port = parse_host_port(
        loc, port_arg if port_arg is not None else default_port
    )

    if port is None and port_arg is None:
        port_arg = default_port

    if port and port_arg and port != port_arg:
        raise ValueError(
            "port number given twice in options: "
            "host %r and port %r" % (host_arg, port_arg)
        )
    if port is None and port_arg is not None:
        port = port_arg
    # Note `port = 0` means "choose a random port"
    if port is None:
        port = default_port
    loc = unparse_host_port(host, port)
    addr = unparse_address(scheme, loc)

    return addr
