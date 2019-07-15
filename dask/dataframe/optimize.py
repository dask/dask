""" Dataframe optimizations """
from __future__ import absolute_import, division, print_function

from ..optimization import cull, fuse_getitem, fuse
from .. import config, core
from ..highlevelgraph import HighLevelGraph
from ..utils import ensure_dict
from ..blockwise import optimize_blockwise


def optimize(dsk, keys, **kwargs):

    if isinstance(dsk, HighLevelGraph):
        dsk = optimize_blockwise(dsk, keys=list(core.flatten(keys)))

    dsk = ensure_dict(dsk)
    from .io import dataframe_from_ctable

    if isinstance(keys, list):
        dsk, dependencies = cull(dsk, list(core.flatten(keys)))
    else:
        dsk, dependencies = cull(dsk, [keys])
    dsk = fuse_getitem(dsk, dataframe_from_ctable, 3)

    from .io.parquet import read_parquet_part

    dsk = fuse_getitem(dsk, read_parquet_part, 4)

    dsk, dependencies = fuse(
        dsk,
        keys,
        dependencies=dependencies,
        fuse_subgraphs=config.get("fuse_subgraphs", True),
    )
    dsk, _ = cull(dsk, keys)
    return dsk
