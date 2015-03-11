import numpy as np
from cytoolz import merge_sorted, concat, map, partition_all
from toolz import merge_sorted

def shard(n, x):
    """

    >>> list(shard(3, list(range(10))))
    [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]
    """
    for i in range(0, len(x), n):
        yield x[i: i + n]


def emerge(seqs, out=None, out_chunksize=2**14, key=None, dtype=None):
    """ External sort

    Merged sorted sequences of numpy arrays in to out result

    Parameters
    ----------

    seqs: iterable of iterators of numpy arrays
        The sorted arrays to be merged together
    out: array supporting setitem syntax
        The output storage array
    out_chunksize: int (default 2**14)
        The size of data to keep in memory before writing out
    """
    assert out is not None

    seqs2 = [concat(x.tolist() for x in seq) for seq in seqs]

    seq = merge_sorted(*seqs2, key=key)
    chunks = (np.array(list(chunk), dtype=dtype)
                for chunk in partition_all(out_chunksize, seq))

    for i, chunk in enumerate(chunks):
        out[i*out_chunksize: min(len(out), (i+1)*out_chunksize)] = chunk

    return out
