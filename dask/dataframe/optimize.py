""" Dataframe optimizations """
from __future__ import absolute_import, division, print_function

from ..optimization import cull, fuse_getitem, fuse
from .. import config, core
from ..highlevelgraph import HighLevelGraph
from ..utils import ensure_dict
from ..blockwise import optimize_blockwise

try:
    import fastparquet  # noqa: F401
except ImportError:
    fastparquet = False


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
    if fastparquet:
        from .io.parquet import _read_parquet_row_group

        dsk = fuse_getitem(dsk, _read_parquet_row_group, 4)

    dsk, dependencies = fuse(
        dsk,
        keys,
        dependencies=dependencies,
        fuse_subgraphs=config.get("fuse_subgraphs", True),
    )
    dsk, _ = cull(dsk, keys)
    return dsk
