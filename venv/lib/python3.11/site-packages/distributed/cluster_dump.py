"Utilities for generating and analyzing cluster dumps"

from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Awaitable, Callable, Collection, Mapping
from pathlib import Path
from typing import IO, Any, Literal

import msgpack

from dask.typing import Key

from distributed._stories import scheduler_story as _scheduler_story
from distributed._stories import worker_story as _worker_story

DEFAULT_CLUSTER_DUMP_FORMAT: Literal["msgpack" | "yaml"] = "msgpack"
DEFAULT_CLUSTER_DUMP_EXCLUDE: Collection[str] = ("run_spec",)


def _tuple_to_list(node):
    if isinstance(node, (list, tuple)):
        return [_tuple_to_list(el) for el in node]
    elif isinstance(node, dict):
        return {k: _tuple_to_list(v) for k, v in node.items()}
    else:
        return node


async def write_state(
    get_state: Callable[[], Awaitable[Any]],
    url: str,
    format: Literal["msgpack", "yaml"] = DEFAULT_CLUSTER_DUMP_FORMAT,
    **storage_options: dict[str, Any],
) -> None:
    "Await a cluster dump, then serialize and write it to a path"
    if format == "msgpack":
        mode = "wb"
        suffix = ".msgpack.gz"
        if not url.endswith(suffix):
            url += suffix
        writer = msgpack.pack
    elif format == "yaml":
        import yaml

        mode = "w"
        suffix = ".yaml"
        if not url.endswith(suffix):
            url += suffix

        def writer(state: dict, f: IO) -> None:
            # YAML adds unnecessary `!!python/tuple` tags; convert tuples to lists to avoid them.
            # Unnecessary for msgpack, since tuples and lists are encoded the same.
            yaml.dump(_tuple_to_list(state), f)

    else:
        raise ValueError(
            f"Unsupported format {format!r}. Possible values are 'msgpack' or 'yaml'."
        )

    # Eagerly open the file to catch any errors before doing the full dump
    # NOTE: `compression="infer"` will automatically use gzip via the `.gz` suffix
    # This module is the only place where fsspec is used and it is a relatively
    # heavy import. Do lazy import to reduce import time
    import fsspec

    with fsspec.open(url, mode, compression="infer", **storage_options) as f:
        state = await get_state()
        # Write from a thread so we don't block the event loop quite as badly
        # (the writer will still hold the GIL a lot though).
        await asyncio.to_thread(writer, state, f)


def load_cluster_dump(url: str, **kwargs: Any) -> dict:
    """Loads a cluster dump from a disk artefact

    Parameters
    ----------
    url : str
        Name of the disk artefact. This should have either a
        ``.msgpack.gz`` or ``yaml`` suffix, depending on the dump format.
    **kwargs :
        Extra arguments passed to :func:`fsspec.open`.

    Returns
    -------
    state : dict
        The cluster state at the time of the dump.
    """
    if url.endswith(".msgpack.gz"):
        mode = "rb"
        reader = msgpack.unpack
    elif url.endswith(".yaml"):
        import yaml

        mode = "r"
        reader = yaml.safe_load
    else:
        raise ValueError(f"url ({url}) must have a .msgpack.gz or .yaml suffix")

    kwargs.setdefault("compression", "infer")
    # This module is the only place where fsspec is used and it is a relatively
    # heavy import. Do lazy import to reduce import time
    import fsspec

    with fsspec.open(url, mode, **kwargs) as f:
        return reader(f)


class DumpArtefact(Mapping):
    """
    Utility class for inspecting the state of a cluster dump

    .. code-block:: python

        dump = DumpArtefact.from_url("dump.msgpack.gz")
        memory_tasks = dump.scheduler_tasks("memory")
        executing_tasks = dump.worker_tasks("executing")
    """

    def __init__(self, state: dict):
        self.dump = state

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> DumpArtefact:
        """Loads a cluster dump from a disk artefact

        Parameters
        ----------
        url : str
            Name of the disk artefact. This should have either a
            ``.msgpack.gz`` or ``yaml`` suffix, depending on the dump format.
        **kwargs :
            Extra arguments passed to :func:`fsspec.open`.

        Returns
        -------
        state : dict
            The cluster state at the time of the dump.
        """
        return DumpArtefact(load_cluster_dump(url, **kwargs))

    def __getitem__(self, key):
        return self.dump[key]

    def __iter__(self):
        return iter(self.dump)

    def __len__(self):
        return len(self.dump)

    def _extract_tasks(self, state: str | None, context: dict[str, dict]) -> list[dict]:
        if state:
            return [v for v in context.values() if v["state"] == state]
        else:
            return list(context.values())

    def scheduler_tasks_in_state(self, state: str | None = None) -> list:
        """
        Parameters
        ----------
        state : optional, str
            If provided, only tasks in the given state are returned.
            Otherwise, all tasks are returned.

        Returns
        -------
        tasks : list
            The list of scheduler tasks in ``state``.
        """
        return self._extract_tasks(state, self.dump["scheduler"]["tasks"])

    def worker_tasks_in_state(self, state: str | None = None) -> list:
        """
        Parameters
        ----------
        state : optional, str
            If provided, only tasks in the given state are returned.
            Otherwise, all tasks are returned.

        Returns
        -------
        tasks : list
            The list of worker tasks in ``state``
        """
        tasks = []

        for worker_dump in self.dump["workers"].values():
            if isinstance(worker_dump, dict) and "tasks" in worker_dump:
                tasks.extend(self._extract_tasks(state, worker_dump["tasks"]))

        return tasks

    def scheduler_story(self, *key_or_stimulus_id: Key | str) -> dict:
        """
        Returns
        -------
        stories : dict
            A list of stories for the keys/stimulus ID's in ``*key_or_stimulus_id``.
        """
        stories = defaultdict(list)

        log = self.dump["scheduler"]["transition_log"]
        keys = set(key_or_stimulus_id)

        for story in _scheduler_story(keys, log):
            stories[story[0]].append(tuple(story))

        return dict(stories)

    def worker_story(self, *key_or_stimulus_id: str) -> dict:
        """
        Returns
        -------
        stories : dict
            A dict of stories for the keys/stimulus ID's in ``*key_or_stimulus_id`.`
        """
        keys = set(key_or_stimulus_id)
        stories = defaultdict(list)

        for worker_dump in self.dump["workers"].values():
            if isinstance(worker_dump, dict) and "log" in worker_dump:
                for story in _worker_story(keys, worker_dump["log"]):
                    stories[story[0]].append(tuple(story))

        return dict(stories)

    def missing_workers(self) -> list:
        """
        Returns
        -------
        missing : list
            A list of workers connected to the scheduler, but which
            did not respond to requests for a state dump.
        """
        scheduler_workers = self.dump["scheduler"]["workers"]
        responsive_workers = self.dump["workers"]
        return [
            w
            for w in scheduler_workers
            if w not in responsive_workers
            or not isinstance(responsive_workers[w], dict)
        ]

    def _compact_state(self, state: dict, expand_keys: set[str]) -> dict[str, dict]:
        """Compacts ``state`` keys into a general key,
        unless the key is in ``expand_keys``"""
        assert "general" not in state
        result = {}
        general = {}

        for k, v in state.items():
            if k in expand_keys:
                result[k] = v
            else:
                general[k] = v

        result["general"] = general
        return result

    def to_yamls(
        self,
        root_dir: str | Path | None = None,
        worker_expand_keys: Collection[str] = ("config", "log", "logs", "tasks"),
        scheduler_expand_keys: Collection[str] = (
            "events",
            "extensions",
            "log",
            "task_groups",
            "tasks",
            "transition_log",
            "workers",
        ),
    ) -> None:
        """
        Splits the Dump Artefact into a tree of yaml files with
        ``root_dir`` as it's base.

        The root level of the tree contains a directory for the scheduler
        and directories for each individual worker.
        Each directory contains yaml files describing the state of the scheduler
        or worker when the artefact was created.

        In general, keys associated with the state are compacted into a ``general.yaml``
        file, unless they are in ``scheduler_expand_keys`` and ``worker_expand_keys``.

        Parameters
        ----------
        root_dir : str or Path
            The root directory into which the tree is written.
            Defaults to the current working directory if ``None``.
        worker_expand_keys : iterable of str
            An iterable of artefact worker keys that will be expanded
            into separate yaml files.
            Keys that are not in this iterable are compacted into a
            `general.yaml` file.
        scheduler_expand_keys : iterable of str
            An iterable of artefact scheduler keys that will be expanded
            into separate yaml files.
            Keys that are not in this iterable are compacted into a
            ``general.yaml`` file.
        """
        import yaml

        root_dir = Path(root_dir) if root_dir else Path.cwd()
        dumper = yaml.CSafeDumper
        scheduler_expand_keys = set(scheduler_expand_keys)
        worker_expand_keys = set(worker_expand_keys)

        workers = self.dump["workers"]
        for info in workers.values():
            try:
                worker_id = info["id"]
            except KeyError:
                continue

            worker_state = self._compact_state(info, worker_expand_keys)

            log_dir = root_dir / worker_id
            log_dir.mkdir(parents=True, exist_ok=True)

            for name, _logs in worker_state.items():
                filename = str(log_dir / f"{name}.yaml")
                with open(filename, "w") as fd:
                    yaml.dump(_logs, fd, Dumper=dumper)

        context = "scheduler"
        scheduler_state = self._compact_state(self.dump[context], scheduler_expand_keys)

        log_dir = root_dir / context
        log_dir.mkdir(parents=True, exist_ok=True)
        # Compact smaller keys into a general dict

        for name, _logs in scheduler_state.items():
            filename = str(log_dir / f"{name}.yaml")

            with open(filename, "w") as fd:
                yaml.dump(_logs, fd, Dumper=dumper)
