import bcolz
import os
import numpy as np
import pandas as pd
from .convert import from_blocks
import tempfile
from .utils import ignoring


class cframe(object):
    """ BColz on-disk DataFrame, stored as raw as possible

    Stores the dataframe columns directly in bcolz arrays.  Supports efficient
    append of new DataFrames and the extraction of all data to a new DataFrame.

    Stores the following

    blocks: dict {column-name: bcolz.carray}
        Holds a carray for each column
    index: carray
        Holds carray for the index
    rootdir: str
        Directory holding all of the block carrays
    columns: list
        Column identifiers (usually strings)

    TODO
    ----

    *  Partial reads by row slicing
    """
    def __init__(self, df, rootdir=None, chunklen=2**16, **kwargs):
        if rootdir is None:
            rootdir = tempfile.mkdtemp('.cframe')
            self._explicitly_given_path = False
        else:
            os.mkdir(rootdir)
            self._explicitly_given_path = True

        self.blocks = dict((col,
                       bcolz.zeros(rootdir=os.path.join(rootdir, '%s.bcolz' % col),
                                   shape=(0,),
                                   dtype=df.dtypes[col], safe=False,
                                   chunklen=chunklen, **kwargs))
                        for col in df.columns)
        self.columns = df.columns
        self.index = bcolz.zeros(shape=(0,), dtype=df.index.values.dtype,
                                 safe=False, chunklen=chunklen, **kwargs)
        self.rootdir = rootdir

    def append(self, df):
        for block in df._data.blocks:
            for i, loc in enumerate(block.mgr_locs.as_array):
                self.blocks[self.columns[loc]].append(block.values[i])
        self.index.append(df.index.values)

    def to_dataframe(self, columns=None):
        columns = self.columns if columns is None else columns
        if not isinstance(columns, (list, tuple, pd.Index)):
            return pd.Series(self.blocks[columns][:],
                             index=self.index[:],
                             name=columns)
        else:
            return pd.DataFrame(dict((col, self.blocks[col][:]) for col in
                columns), index=self.index[:], columns=columns)

    @property
    def nbytes(self):
        return self.index.nbytes + sum(block.nbytes for block
                                        in self.blocks.values())

    @property
    def cbytes(self):
        return self.index.cbytes + sum(block.cbytes for block
                                        in self.blocks.values())

    def flush(self):
        for block in self.blocks.values():
            block.flush()
        self.index.flush()

    def __del__(self):
        if self._explicitly_given_path:
            try:
                self.flush()
            except IOError:
                pass
        else:
            self.drop()

    def drop(self):
        shutil.rmtree(self.rootdir)

    def head(self, n=10):
        return self.to_dataframe().head(n)

    def __len__(self):
        return len(self.blocks[self.columns[0]])
