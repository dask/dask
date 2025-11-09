from __future__ import annotations

import asyncio
import logging
import signal
from typing import Any

logger = logging.getLogger(__name__)


async def wait_for_signals() -> int:
    """Wait for sigint or sigterm by setting global signal handlers"""
    signals = (signal.SIGINT, signal.SIGTERM)
    loop = asyncio.get_running_loop()
    event = asyncio.Event()

    old_handlers: dict[int, Any] = {}
    caught_signal: int | None = None

    def handle_signal(signum, frame):
        # *** Do not log or print anything in here
        # https://stackoverflow.com/questions/45680378/how-to-explain-the-reentrant-runtimeerror-caused-by-printing-in-signal-handlers
        nonlocal caught_signal
        caught_signal = signum
        # Restore old signal handler to allow for quicker exit
        # if the user sends the signal again.
        signal.signal(signum, old_handlers[signum])
        loop.call_soon_threadsafe(event.set)

    for sig in signals:
        old_handlers[sig] = signal.signal(sig, handle_signal)

    try:
        await event.wait()
        assert caught_signal
        logger.info(
            "Received signal %s (%d)", signal.Signals(caught_signal).name, caught_signal
        )
        return caught_signal
    finally:
        for sig in signals:
            signal.signal(sig, old_handlers[sig])
