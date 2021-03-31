""" Dataframe optimizations """
import operator

from dask.base import tokenize

from .. import config, core
from ..blockwise import Blockwise, fuse_roots, optimize_blockwise
from ..highlevelgraph import HighLevelGraph
from ..optimization import cull, fuse
from ..utils import ensure_dict


def optimize(dsk, keys, **kwargs):
    if not isinstance(keys, (list, set)):
        keys = [keys]
    keys = list(core.flatten(keys))

    if not isinstance(dsk, HighLevelGraph):
        dsk = HighLevelGraph.from_collections(id(dsk), dsk, dependencies=())

    dsk = optimize_read_parquet_getitem(dsk, keys=keys)
    dsk = optimize_blockwise(dsk, keys=keys)
    dsk = fuse_roots(dsk, keys=keys)
    dsk = dsk.cull(set(keys))

    if not config.get("optimization.fuse.active"):
        return dsk

    dependencies = dsk.get_all_dependencies()
    dsk = ensure_dict(dsk)

    fuse_subgraphs = config.get("optimization.fuse.subgraphs")
    if fuse_subgraphs is None:
        fuse_subgraphs = True
    dsk, _ = fuse(
        dsk,
        keys,
        dependencies=dependencies,
        fuse_subgraphs=fuse_subgraphs,
    )
    dsk, _ = cull(dsk, keys)
    return dsk


def optimize_read_parquet_getitem(dsk, keys):
    # find the keys to optimize
    from .io.parquet.core import ParquetSubgraph

    read_parquets = [k for k, v in dsk.layers.items() if isinstance(v, ParquetSubgraph)]

    layers = dsk.layers.copy()
    dependencies = dsk.dependencies.copy()

    for k in read_parquets:
        columns = set()
        update_blocks = {}

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
                # ... where this value is __getitem__...
                return dsk

            if any(layers[k].name == x[0] for x in keys if isinstance(x, tuple)):
                # ... but bail on the optimization if the read_parquet layer is in
                # the requested keys, because we cannot change the name anymore.
                # These keys are structured like [('getitem-<token>', 0), ...]
                # so we check for the first item of the tuple.
                # See https://github.com/dask/dask/issues/5893
                return dsk

            block_columns = block.indices[1][0]
            if isinstance(block_columns, str):
                block_columns = [block_columns]

            columns |= set(block_columns)
            update_blocks[dep] = block

        old = layers[k]

        if columns and columns < set(old.meta.columns):
            columns = list(columns)
            meta = old.meta[columns]
            name = "read-parquet-" + tokenize(old.name, columns)
            assert len(update_blocks)

            for block_key, block in update_blocks.items():
                # (('read-parquet-old', (.,)), ( ... )) ->
                # (('read-parquet-new', (.,)), ( ... ))
                new_indices = ((name, block.indices[0][1]), block.indices[1])
                numblocks = {name: block.numblocks[old.name]}
                new_block = Blockwise(
                    block.output,
                    block.output_indices,
                    block.dsk,
                    new_indices,
                    numblocks,
                    block.concatenate,
                    block.new_axes,
                )
                layers[block_key] = new_block
                dependencies[block_key] = {name}
            dependencies[name] = dependencies.pop(k)

        else:
            # Things like df[df.A == 'a'], where the argument to
            # getitem is not a column name
            name = old.name
            meta = old.meta
            columns = list(meta.columns)

        new = ParquetSubgraph(
            name,
            old.engine,
            old.fs,
            meta,
            columns,
            old.index,
            old.parts,
            old.kwargs,
            common_kwargs=old.common_kwargs,
        )
        layers[name] = new
        if name != old.name:
            del layers[old.name]

    new_hlg = HighLevelGraph(layers, dependencies)
    return new_hlg
