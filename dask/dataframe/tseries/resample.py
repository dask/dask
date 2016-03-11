from __future__ import absolute_import, division, print_function

from distutils.version import LooseVersion

import pandas as pd
import numpy as np

from ..core import DataFrame, Series
from ...base import tokenize


def getnanos(rule):
    try:
        return getattr(rule, 'nanos', None)
    except ValueError:
        return None


if LooseVersion(pd.__version__) >= '0.18.0':
    def _resample_apply(s, rule, how, resample_kwargs):
        return getattr(s.resample(rule, **resample_kwargs), how)()

    def _resample(obj, rule, how, **kwargs):
        resampler = Resampler(obj, rule, **kwargs)
        if how is not None:
            raise FutureWarning(("how in .resample() is deprecated "
                                 "the new syntax is .resample(...)"
                                 ".{0}()").format(how))
            return getattr(resampler, how)()
        return resampler
else:
    def _resample_apply(s, rule, how, resample_kwargs):
        return s.resample(rule, how=how, **resample_kwargs)

    def _resample(obj, rule, how, **kwargs):
        how = how or 'mean'
        return getattr(Resampler(obj, rule, **kwargs), how)()


def _resample_series(series, start, end, reindex_closed, rule,
                     resample_kwargs, how, fill_value):
    out = _resample_apply(series, rule, how, resample_kwargs)
    return out.reindex(pd.date_range(start, end, freq=rule,
                                     closed=reindex_closed),
                       fill_value=fill_value)


def _resample_bin_and_out_divs(divisions, rule, closed='left', label='left'):
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


class Resampler(object):
    def __init__(self, obj, rule, **kwargs):
        self.obj = obj
        rule = pd.datetools.to_offset(rule)
        day_nanos = pd.datetools.Day().nanos

        if getnanos(rule) and day_nanos % rule.nanos:
            raise NotImplementedError('Resampling frequency %s that does'
                                      ' not evenly divide a day is not '
                                      'implemented' % rule)
        self._rule = rule
        self._kwargs = kwargs

    def _agg(self, how, columns=None, fill_value=np.nan):
        rule = self._rule
        kwargs = self._kwargs
        name = tokenize(self.obj, rule, kwargs, how)

        # Create a grouper to determine closed and label conventions
        newdivs, outdivs = _resample_bin_and_out_divs(self.obj.divisions, rule,
                                                      **kwargs)

        # Repartition divs into bins. These won't match labels after mapping
        partitioned = self.obj.repartition(newdivs, force=True)

        keys = partitioned._keys()
        dsk = partitioned.dask

        args = zip(keys, outdivs, outdivs[1:], ['left']*(len(keys)-1) + [None])
        for i, (k, s, e, c) in enumerate(args):
            dsk[(name, i)] = (_resample_series, k, s, e, c,
                              rule, kwargs, how, fill_value)
        if columns:
            return DataFrame(dsk, name, columns, outdivs)
        return Series(dsk, name, self.obj.name, outdivs)

    def count(self):
        return self._agg('count', fill_value=0)

    def first(self):
        return self._agg('first')

    def last(self):
        return self._agg('last')

    def mean(self):
        return self._agg('mean')

    def min(self):
        return self._agg('min')

    def median(self):
        return self._agg('median')

    def max(self):
        return self._agg('max')

    def ohlc(self):
        return self._agg('ohlc', columns=['open', 'high', 'low', 'close'])

    def prod(self):
        return self._agg('prod')

    def sem(self):
        return self._agg('sem')

    def std(self):
        return self._agg('std')

    def sum(self):
        return self._agg('sum')

    def var(self):
        return self._agg('var')
