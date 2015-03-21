import bcolz
import os
import numpy as np
from convert import from_blocks
import tempfile
from .utils import ignoring


class cframe(object):
    """ BColz on-disk DataFrame, stored as raw as possible

    Stores the dataframe blocks directly in bcolz arrays.  Supports efficient
    append of new DataFrames and the extraction of all data in to a new
    DataFrame.

    TODO
    ----

    *  Partial reads by row slicing
    *  Partial column reads
    """
    def __init__(self, df, rootdir=None, **kwargs):
        if rootdir is None:
            rootdir = tempfile.mkdtemp('.cframe')
            self._explicitly_given_path = False
        else:
            os.mkdir(rootdir)
            self._explicitly_given_path = True

        self.blocks = [bcolz.zeros(rootdir=os.path.join(rootdir, '%d.bcolz' % i),
                                   shape=(0, blk.values.shape[0]),
                                   dtype=blk.values.dtype, safe=False, **kwargs)
                        for i, blk in enumerate(df._data.blocks)]
        self.columns = df.columns
        self.index = bcolz.zeros(shape=(0,), dtype=df.index.values.dtype,
                                 safe=False, **kwargs)
        self.placement = [ b.mgr_locs.as_array for b in df._data.blocks ]
        self.rootdir = rootdir

    def append(self, df):
        for a, b in zip(self.blocks, df._data.blocks):
            # TODO: is it better to store many columns or have fewer writes?
            a.append(flip(b.values))
        self.index.append(df.index.values)

    def to_dataframe(self):
        blocks = [flip(block[:]) for block in self.blocks]
        return from_blocks(blocks, self.index[:], self.columns, self.placement)

    @property
    def nbytes(self):
        return self.index.nbytes + sum(block.nbytes for block in self.blocks)

    @property
    def cbytes(self):
        return self.index.cbytes + sum(block.cbytes for block in self.blocks)

    def flush(self):
        for block in self.blocks:
            block.flush()
        self.index.flush()

    def __del__(self):
        if self._explicitly_given_path:
            with ignoring(IOError):
                self.flush()
        else:
            self.drop()

    def drop(self):
        shutil.rmtree(self.rootdir)

    def head(self, n=10):
        return self.to_dataframe().head(n)


def flip(x):
    """ Change striding and data ordering while leaving semantics intact """
    y = np.empty(shape=x.T.shape, dtype=x.T.dtype)
    y[:] = x.T
    return y
