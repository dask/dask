import pandas as pd
from pandas.core.internals import create_block_manager_from_blocks, make_block
from pandas.core.index import _ensure_index


def to_blocks(df):
    blocks = [block.values for block in df._data.blocks]
    index = df.index.values
    return {'blocks': blocks,
            'index': index,
            'columns': df.columns,
            'placement': [ b.mgr_locs.as_array for b in df._data.blocks ]}


def from_blocks(blocks, index, columns, placement):
    blocks = [ make_block(b, placement=placement[i]) for i, b in enumerate(blocks) ]
    axes = [_ensure_index(columns), _ensure_index(index) ]
    df = pd.DataFrame(create_block_manager_from_blocks(blocks, axes))
    return df
