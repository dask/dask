from __future__ import annotations

from collections.abc import Collection, Iterable
from typing import TYPE_CHECKING

from dask.typing import Key

if TYPE_CHECKING:
    # Circular import
    from distributed.scheduler import Transition


def scheduler_story(
    keys_or_stimuli: set[Key | str], transition_log: Iterable[Transition]
) -> list[Transition]:
    """Creates a story from the scheduler transition log given a set of keys
    describing tasks or stimuli.

    Parameters
    ----------
    keys_or_stimuli : set[str]
        Task keys or stimulus_id's
    log : iterable
        The scheduler transition log

    Returns
    -------
    story : list[tuple]
    """
    return [
        t
        for t in transition_log
        if t[0] in keys_or_stimuli or keys_or_stimuli.intersection(t[3])
    ]


def worker_story(keys_or_stimuli: Collection[Key | str], log: Iterable[tuple]) -> list:
    """Creates a story from the worker log given a set of keys
    describing tasks or stimuli.

    Parameters
    ----------
    keys_or_stimuli : set[str]
        Task keys or stimulus_id's
    log : iterable
        The worker log

    Returns
    -------
    story : list[str]
    """
    return [
        msg
        for msg in log
        if any(key in msg for key in keys_or_stimuli)
        or any(
            key in c
            for key in keys_or_stimuli
            for c in msg
            if isinstance(c, (tuple, list, set))
        )
    ]
