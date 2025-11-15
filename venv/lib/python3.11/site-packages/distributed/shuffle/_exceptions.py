from __future__ import annotations


class P2PIllegalStateError(RuntimeError):
    pass


class P2PConsistencyError(RuntimeError):
    pass


class ShuffleClosedError(P2PConsistencyError):
    pass


class DataUnavailable(Exception):
    """Raised when data is not available in the buffer"""


class P2POutOfDiskError(OSError):
    def __str__(self) -> str:
        return (
            "P2P ran out of available disk space while temporarily storing transferred data. "
            "Please make sure that P2P has enough disk space available by increasing the number of "
            "workers or the size of the attached disk."
        )
