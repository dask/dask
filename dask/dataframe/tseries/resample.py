from __future__ import absolute_import, division, print_function

import pandas as pd

from ..core import DataFrame, Series
from ...base import tokenize


def getnanos(rule):
    try:
        return getattr(rule, 'nanos', None)
    except ValueError:
        return None


def _resample(series, rule, how='mean', axis=0, fill_method=None, closed=None,
              label=None, convention='start', kind=None, loffset=None,
              limit=None, base=0):

    # Validate Inputs
    rule = pd.datetools.to_offset(rule)
    day_nanos = pd.datetools.Day().nanos

    if getnanos(rule) and day_nanos % rule.nanos:
        raise NotImplementedError('Resampling frequency %s that does'
                                  ' not evenly divide a day is not '
                                  'implemented' % rule)

    kwargs = {'fill_method': fill_method, 'limit': limit,
              'loffset': loffset, 'base': base,
              'convention': convention != 'start', 'kind': kind}
    err = ', '.join('`{0}`'.format(k) for (k, v) in kwargs.items() if v)
    if err:
        raise NotImplementedError('Keywords: ' + err)

    kwargs = {'how': how, 'closed': closed, 'label': label}

    # Create a grouper to determine closed and label conventions
    newdivs, outdivs = _resample_bin_and_out_divs(series.divisions, rule,
                                                  closed, label)

    # Repartition divs into bins. These won't match labels after mapping
    partitioned = series.repartition(newdivs, force=True)

    name = tokenize(series, rule, kwargs)
    dsk = partitioned.dask

    keys = partitioned._keys()
    args = zip(keys, outdivs, outdivs[1:], ['left']*(len(keys)-1) + [None])
    for i, (k, s, e, c) in enumerate(args):
        dsk[(name, i)] = (_resample_series, k, s, e, c, rule,
                          how, label, closed)

    if how == 'ohlc':
        return DataFrame(dsk, name, ['open', 'high', 'low', 'close'], outdivs)
    return Series(dsk, name, series.name, outdivs)


def _resample_series(series, start, end, reindex_closed,
                     rule, how, label, resample_closed):
    out = series.resample(rule, how=how, closed=resample_closed, label=label)
    return out.reindex(pd.date_range(start, end, freq=rule,
                                     closed=reindex_closed))


def _resample_bin_and_out_divs(divisions, rule, closed, label):
    rule = pd.datetools.to_offset(rule)
    g = pd.TimeGrouper(rule, how='count', closed=closed, label=label)

    # Determine bins to apply `how` to. Disregard labeling scheme.
    divs = pd.Series(range(len(divisions)), index=divisions)
    temp = divs.resample(rule, how='count', closed=closed, label='left')
    tempdivs = temp.loc[temp > 0].index

    # Cleanup closed == 'right' and label == 'right'
    res = pd.offsets.Nano() if hasattr(rule, 'delta') else pd.offsets.Day()
    if g.closed == 'right':
        newdivs = tempdivs + res
    else:
        newdivs = tempdivs
    if g.label == 'right':
        outdivs = tempdivs + rule
    else:
        outdivs = tempdivs

    newdivs = newdivs.tolist()
    outdivs = outdivs.tolist()

    # Adjust ends
    if newdivs[0] < divisions[0]:
        newdivs[0] = divisions[0]
    if newdivs[-1] < divisions[-1]:
        if len(newdivs) < len(divs):
            setter = lambda a, val: a.append(val)
        else:
            setter = lambda a, val: a.__setitem__(-1, val)
        setter(newdivs, divisions[-1])
        if outdivs[-1] > divisions[-1]:
            setter(outdivs, outdivs[-1])
        elif outdivs[-1] < divisions[-1]:
            setter(outdivs, temp.index[-1])

    return tuple(map(pd.Timestamp, newdivs)), tuple(map(pd.Timestamp, outdivs))
