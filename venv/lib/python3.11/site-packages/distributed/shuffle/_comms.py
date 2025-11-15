from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from distributed.core import ErrorMessage, OKMessage, clean_exception
from distributed.metrics import context_meter
from distributed.shuffle._disk import ShardsBuffer
from distributed.shuffle._limiter import ResourceLimiter


class CommShardsBuffer(ShardsBuffer):
    """Accept, buffer, and send many small messages to many workers

    This takes in lots of small messages destined for remote workers, buffers
    those messages in memory, and then sends out batches of them when possible
    to different workers.  This tries to send larger messages when possible,
    while also respecting a memory bound

    **State**

    -   shards: dict[str, list[ShardType]]

        This is our in-memory buffer of data waiting to be sent to other workers.

    -   sizes: dict[str, int]

        The size of each list of shards.  We find the largest and send data from that buffer

    State
    -----
    max_message_size: int
        The maximum size in bytes of a single message that we want to send

    Parameters
    ----------
    send : callable
        How to send a list of shards to a worker
        Expects an address of the target worker (string)
        and a payload of shards (list of bytes) to send to that worker
    memory_limiter : ResourceLimiter
        Limiter for memory usage (in bytes). If the incoming data that
        has yet to be processed exceeds this limit, then the buffer will
        block until below the threshold. See :meth:`.write` for the
        implementation of this scheme.
    concurrency_limit : int
        Number of background tasks to run.
    """

    def __init__(
        self,
        send: Callable[
            [str, list[tuple[Any, Any]]], Awaitable[OKMessage | ErrorMessage]
        ],
        memory_limiter: ResourceLimiter,
        max_message_size: int,
        concurrency_limit: int,
    ):
        super().__init__(
            memory_limiter=memory_limiter,
            concurrency_limit=concurrency_limit,
            max_message_size=max_message_size,
        )
        self.send = send

    async def _process(self, address: str, shards: list[tuple[Any, Any]]) -> None:
        """Send one message off to a neighboring worker"""
        # Consider boosting total_size a bit here to account for duplication
        with context_meter.meter("send"):
            response = await self.send(address, shards)
            status = response["status"]
            if status == "error":
                _, exc, tb = clean_exception(**response)
                assert exc
                raise exc.with_traceback(tb)
            assert status == "OK"
