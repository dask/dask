""" This file is experimental and may disappear without warning """
from __future__ import print_function, division, absolute_import

import dask.dataframe as dd
from dask.base import tokenize
from tornado import gen

from .utils import sync, ignoring


@gen.coroutine
def _futures_to_dask_dataframe(executor, futures, divisions=None):
    columns = executor.submit(lambda df: df.columns, futures[0])
    if divisions is True:
        divisions = executor.map(lambda df: df.index.min(), futures)
        divisions.append(executor.submit(lambda df: df.index.max(), futures[-1]))
        divisions = yield executor._gather(divisions)
        if sorted(divisions) != divisions:
            divisions = [None] * (len(futures) + 1)
    elif divisions in (None, False):
        divisions = [None] * (len(futures) + 1)
    else:
        raise NotImplementedError()
    columns = yield columns._result()

    name = 'distributed-pandas-to-dask-' + tokenize(*futures)
    dsk = {(name, i): f for i, f in enumerate(futures)}

    raise gen.Return(dd.DataFrame(dsk, name, columns, divisions))


def futures_to_dask_dataframe(executor, futures, divisions=None):
    """ Convert a list of futures into a dask.dataframe

    Parameters
    ----------
    executor: Executor
        Executor through which we access the remote dataframes
    futures: iterable of Futures
        Futures that create dataframes to form the divisions
    divisions: bool
        Set to True if the data is cleanly partitioned along the index
    """
    return sync(executor.loop, _futures_to_dask_dataframe, executor, futures,
                divisions=divisions)
