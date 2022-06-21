from __future__ import annotations

import operator
from dataclasses import dataclass
from functools import cached_property
from typing import Any

from dask.base import tokenize

# from dask.core import keys_in_tasks
from dask.operation.dataframe.core import (
    PartitionKey,
    _FrameOperation,
    _SimpleFrameOperation,
)

no_default = "__no_default__"


@dataclass(frozen=True)
class ShuffleOnColumns(_SimpleFrameOperation):

    source: _FrameOperation
    on: str | list
    _npartitions: int | None = None
    max_branch: int = 32
    ignore_index: bool = False
    _meta: Any = no_default
    _divisions: tuple | None = None

    @cached_property
    def token(self) -> str:
        return tokenize(
            self.meta,
            self.divisions,
            self.dependencies,
            self.on,
            self._npartitions,
            self.max_branch,
            self.ignore_index,
        )

    @cached_property
    def name(self) -> str:
        return f"shuffle-{self.token}"

    @cached_property
    def default_divisions(self) -> tuple:
        npartitions = self._npartitions or self.source.npartitions
        return (None,) * (npartitions + 1)

    def subgraph(self, keys: list[PartitionKey]) -> tuple[dict, dict]:

        parts_out = [k[1] for k in keys if k[0] == self.name]
        if not parts_out:
            raise ValueError

        if self.npartitions <= self.max_branch:
            dsk = simple_shuffle_graph(
                parts_out,
                self.name,
                self.on,
                self.npartitions,
                self.source.npartitions,
                self.ignore_index,
                self.source.name,
                self.source.meta,
            )
            return dsk, {self.source: self.source.collection_keys}

        raise NotImplementedError("Full shuffle not implemented yet.")

    def __hash__(self):
        return hash(self.name)


def simple_shuffle_graph(
    parts_out,
    name,
    columns,
    npartitions,
    npartitions_input,
    ignore_index,
    name_input,
    meta_input,
):
    from dask.dataframe.shuffle import shuffle_group as shuffle_group_func
    from dask.operation.dataframe.collection import _concat as concat_func

    dsk = {}
    split_name = "split-" + name
    shuffle_group_name = "group-" + name
    for part_out in parts_out:
        _concat_list = [
            (split_name, part_out, part_in) for part_in in range(npartitions_input)
        ]
        dsk[(name, part_out)] = (
            concat_func,
            _concat_list,
            ignore_index,
        )
        for _, _part_out, _part_in in _concat_list:
            dsk[(split_name, _part_out, _part_in)] = (
                operator.getitem,
                (shuffle_group_name, _part_in),
                _part_out,
            )
            if (shuffle_group_name, _part_in) not in dsk:
                dsk[(shuffle_group_name, _part_in)] = (
                    shuffle_group_func,
                    (name_input, _part_in),
                    columns,
                    0,
                    npartitions,
                    npartitions,
                    ignore_index,
                    npartitions,
                )
    return dsk
