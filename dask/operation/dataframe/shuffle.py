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


# def full_shuffle_graph(
#     parts_out,
#     name,
#     columns,
#     npartitions,
#     npartitions_input,
#     ignore_index,
#     name_input,
#     meta_input,
#     max_branch,
# ):

#     max_branch = max_branch or 32
#     n = df.npartitions

#     stages = int(math.ceil(math.log(n) / math.log(max_branch)))
#     if stages > 1:
#         k = int(math.ceil(n ** (1 / stages)))
#     else:
#         k = n

#     groups = []
#     splits = []
#     joins = []

#     inputs = [tuple(digit(i, j, k) for j in range(stages)) for i in range(k ** stages)]

#     token = name.split("-")[-1]
#     shuffle_join_name = "shuffle-join-" + token
#     shuffle_group_name = "shuffle-group-" + token
#     shuffle_split_name = "shuffle-split-" + token
#     shuffle_token = "shuffle-" + token

#     start = {}
#     end = {}

#     for idx, inp in enumerate(inputs):
#         group = {}
#         split = {}
#         join = {}
#         start[(shuffle_join_name, 0, inp)] = (
#             (df._name, idx) if idx < df.npartitions else df._meta
#         )
#         for stage in range(1, stages + 1):
#             # Convert partition into dict of dataframe pieces
#             group[(shuffle_group_name, stage, inp)] = (
#                 shuffle_group,
#                 (shuffle_join_name, stage - 1, inp),
#                 column,
#                 stage - 1,
#                 k,
#                 n,
#                 ignore_index,
#                 npartitions,
#             )

#             _concat_list = []
#             for i in range(k):
#                 # Get out each individual dataframe piece from the dicts
#                 split[(shuffle_split_name, stage, i, inp)] = (
#                     getitem,
#                     (shuffle_group_name, stage, inp),
#                     i,
#                 )

#                 _concat_list.append(
#                     (
#                         shuffle_split_name,
#                         stage,
#                         inp[stage - 1],
#                         insert(inp, stage - 1, i),
#                     )
#                 )

#             # concatenate those pieces together, with their friends
#             join[(shuffle_join_name, stage, inp)] = (
#                 _concat,
#                 _concat_list,
#                 ignore_index,
#             )

#         groups.append(group)
#         splits.append(split)
#         joins.append(join)

#         end[(shuffle_token, idx)] = (shuffle_join_name, stages, inp)

#     groups.extend(splits)
#     groups.extend(joins)

#     dsk = toolz.merge(start, end, *(groups))
#     graph = HighLevelGraph.from_collections(shuffle_token, dsk, dependencies=[df])

#     df2 = new_dd_object(graph, shuffle_token, df._meta, df.divisions)

#     if npartitions is not None and npartitions != df.npartitions:
#         token = tokenize(df2, npartitions)
#         repartition_group_token = "repartition-group-" + token

#         dsk = {
#             (repartition_group_token, i): (
#                 shuffle_group_2,
#                 k,
#                 column,
#                 ignore_index,
#                 npartitions,
#             )
#             for i, k in enumerate(df2.__dask_keys__())
#         }

#         repartition_get_name = "repartition-get-" + token

#         for p in range(npartitions):
#             dsk[(repartition_get_name, p)] = (
#                 shuffle_group_get,
#                 (repartition_group_token, p % df.npartitions),
#                 p,
#             )

#         graph2 = HighLevelGraph.from_collections(
#             repartition_get_name, dsk, dependencies=[df2]
#         )
#         df3 = new_dd_object(
#             graph2, repartition_get_name, df2._meta, [None] * (npartitions + 1)
#         )
#     else:
#         df3 = df2
#         df3.divisions = (None,) * (df.npartitions + 1)

#     return df3
