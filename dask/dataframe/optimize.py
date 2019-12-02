""" Dataframe optimizations """
import operator

from ..optimization import cull, fuse, SubgraphCallable
from .. import config, core
from ..highlevelgraph import HighLevelGraph
from ..utils import ensure_dict, M
from ..blockwise import optimize_blockwise, fuse_roots, Blockwise
from ..core import get_deps


def optimize(dsk, keys, **kwargs):

    if isinstance(dsk, HighLevelGraph):
        # Think about an API for this.
        dsk = optimize_read_parquet_getitem(dsk)
        dsk = optimize_blockwise(dsk, keys=list(core.flatten(keys)))
        dsk = fuse_roots(dsk, keys=list(core.flatten(keys)))

    dsk = ensure_dict(dsk)

    if isinstance(keys, list):
        dsk, dependencies = cull(dsk, list(core.flatten(keys)))
    else:
        dsk, dependencies = cull(dsk, [keys])

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

    layers = dsk.layers.copy()

    for k in read_parquets:
        columns = set()

        for dep in dsk.dependents[k]:
            block = dsk.layers[dep]

            # Check if we're a read_parquet followed by a getitem
            if not isinstance(block, Blockwise):
                # getitem are Blockwise...
                return dsk

            if len(block.dsk) != 1:
                # ... with a single item...
                return dsk

            if list(block.dsk.values())[0][0] != operator.getitem:
                # ... where this value is __getitem__
                return dsk

            block_columns = block.indices[1][0]
            if isinstance(block_columns, str):
                block_columns = [block_columns]

            columns |= set(block_columns)

        old = layers[k]

        if columns and columns < set(old.meta.columns):
            columns = list(columns)
            meta = old.meta[columns]
        else:
            # Things like df[df.A == 'a'], where the argument to
            # getitem is not a column name
            meta = old.meta
            columns = list(meta.columns)

        new = ParquetSubgraph(
            old.name,
            old.engine,
            old.fs,
            meta,
            columns,
            old.index,
            old.parts,
            old.kwargs,
        )
        layers[k] = new

    return HighLevelGraph(layers, dsk.dependencies)


def optimize_drop(dsk):

    layers = dsk.layers.copy()

    for k, v in dsk.layers.items():

        if not isinstance(v, Blockwise):
            continue

        for value in v.values():
            sgc = value[0]
            if not isinstance(sgc, SubgraphCallable):
                continue

            sub_graph = sgc.dsk
            deps = get_deps(sub_graph)
            for dk, dv in sub_graph.items():
                if dv[0] == M.drop and deps[0][dk]:
                    import pdb

                    pdb.set_trace()
                    pass

                    # We need to add the `inplace` kwarg to the drop call here

                    # We also need to make sure that only one sub-task task
                    # depends on the output of the drop operation..

    return HighLevelGraph(layers, dsk.dependencies)
