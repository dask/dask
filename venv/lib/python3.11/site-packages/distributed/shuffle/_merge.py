# mypy: ignore-errors
from __future__ import annotations

from typing import TYPE_CHECKING

from dask._task_spec import GraphNode
from dask.typing import Key

from distributed.shuffle._core import ShuffleId, get_worker_plugin
from distributed.shuffle._shuffle import shuffle_transfer

if TYPE_CHECKING:
    import pandas as pd
    from pandas._typing import IndexLabel, MergeHow, Suffixes

    # TODO import from typing (requires Python >=3.10)
    from typing_extensions import TypeAlias

_T_LowLevelGraph: TypeAlias = dict[Key, GraphNode]


def merge_transfer(
    input: pd.DataFrame,
    id: ShuffleId,
    input_partition: int,
):
    return shuffle_transfer(
        input=input,
        id=id,
        input_partition=input_partition,
    )


def merge_unpack(
    shuffle_id_left: ShuffleId,
    shuffle_id_right: ShuffleId,
    output_partition: int,
    barrier_left: int,
    barrier_right: int,
    how: MergeHow,
    left_on: IndexLabel,
    right_on: IndexLabel,
    result_meta: pd.DataFrame,
    suffixes: Suffixes,
    left_index: bool,
    right_index: bool,
    indicator: bool = False,
):
    from dask.dataframe.multi import merge_chunk

    ext = get_worker_plugin()
    # If the partition is empty, it doesn't contain the hash column name
    left = ext.get_output_partition(shuffle_id_left, barrier_left, output_partition)
    right = ext.get_output_partition(shuffle_id_right, barrier_right, output_partition)
    return merge_chunk(
        left,
        right,
        how=how,
        result_meta=result_meta,
        left_on=left_on,
        right_on=right_on,
        suffixes=suffixes,
        left_index=left_index,
        right_index=right_index,
        indicator=indicator,
    )
