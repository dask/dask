from .core import repartition, tokens, DataFrame
from bisect import bisect_left, bisect_right
from toolz import merge_sorted, unique, merge
import pandas as pd


def bound(seq, left, right):
    """ Bound sorted list by left and right values

    >>> bound([1, 3, 4, 5, 8, 10, 12], 4, 10)
    [4, 5, 8, 10]
    """
    return seq[bisect_left(seq, left): bisect_right(seq, right)]


def align(*dfs):
    """ Mutually partition and align DataFrame blocks

    This serves as precursor to multi-dataframe operations like join, concat,
    or merge.

    Parameters
    ----------
    dfs: sequence of dd.DataFrames
        Sequence of dataframes to be aligned on their index

    Returns
    -------
    dfs: sequence of dd.DataFrames
        These DataFrames have consistent divisions with each other
    divisions: tuple
        Full divisions sequence of the entire result
    result: list
        A list of lists of keys that show which dataframes exist on which
        divisions
    """
    divisions = list(unique(merge_sorted(*[df.divisions for df in dfs])))
    divisionss = [bound(divisions, df.divisions[0], df.divisions[-1])
                  for df in dfs]
    dfs2 = list(map(repartition, dfs, divisionss))

    result = list()
    inds = [0 for df in dfs]
    for d in divisions[:-1]:
        L = list()
        for i in range(len(dfs)):
            j = inds[i]
            divs = dfs2[i].divisions
            if j < len(divs) - 1 and divs[j] == d:
                L.append((dfs2[i]._name, inds[i]))
                inds[i] += 1
            else:
                L.append(None)
        result.append(L)
    return dfs2, tuple(divisions), result
