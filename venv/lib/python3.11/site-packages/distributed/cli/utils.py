from __future__ import annotations

import warnings

from tornado.ioloop import IOLoop

warnings.warn(
    "the distributed.cli.utils module is deprecated", DeprecationWarning, stacklevel=2
)


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
        async def cleanup_and_stop():
            try:
                if cleanup is not None:
                    await cleanup(sig)
            finally:
                loop.stop()

        loop.add_callback_from_signal(cleanup_and_stop)
        # Restore old signal handler to allow for a quicker exit
        # if the user sends the signal again.
        signal.signal(sig, old_handlers[sig])

    for sig in [signal.SIGINT, signal.SIGTERM]:
        old_handlers[sig] = signal.signal(sig, handle_signal)
