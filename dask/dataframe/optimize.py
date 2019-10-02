""" Dataframe optimizations """
import operator

from ..optimization import cull, fuse_getitem, fuse
from .. import config, core
from ..highlevelgraph import HighLevelGraph
from ..utils import ensure_dict
from ..blockwise import optimize_blockwise, Blockwise


def optimize(dsk, keys, **kwargs):

    if isinstance(dsk, HighLevelGraph):
        # Think about an API for this.
        dsk = optimize_read_parquet_getitem(dsk)
        dsk = optimize_blockwise(dsk, keys=list(core.flatten(keys)))

    dsk = ensure_dict(dsk)
    from .io import dataframe_from_ctable

    if isinstance(keys, list):
        dsk, dependencies = cull(dsk, list(core.flatten(keys)))
    else:
        dsk, dependencies = cull(dsk, [keys])
    dsk = fuse_getitem(dsk, dataframe_from_ctable, 3)

    dsk, dependencies = fuse(
        dsk,
        keys,
        dependencies=dependencies,
        fuse_subgraphs=config.get("fuse_subgraphs", True),
    )
    dsk, _ = cull(dsk, keys)
    return dsk


def optimize_read_parquet_getitem(dsk):
    # find the keys to optimze
    from .io.parquet.core import ParquetSubgraph

    read_parquets = [k for k, v in dsk.layers.items() if isinstance(v, ParquetSubgraph)]

    layers_dict = dsk.layers.copy()

    for k in read_parquets:
        columns = set()

        for dep in dsk.dependents[k]:
            block = dsk.layers[dep]
            if not isinstance(block, Blockwise):
                return dsk

            if len(block.dsk) != 1:
                # TODO
                return dsk

            if list(block.dsk.values())[0][0] != operator.getitem:
                return dsk

            block_columns = block.indices[1][0]  # like ('B', None)
            if isinstance(block_columns, str):
                block_columns = [block_columns]

            block_columns = set(block_columns)
            columns |= block_columns

        old = layers_dict[k]
        new = ParquetSubgraph(
            old.name,
            old.engine,
            old.fs,
            old.meta[list(columns)],
            list(columns),
            old.index,
            old.parts,
            old.kwargs,
        )
        layers_dict[k] = new

    return HighLevelGraph(layers_dict, dsk.dependencies)
