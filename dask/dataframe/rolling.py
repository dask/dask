from __future__ import absolute_import, division, print_function

from functools import partial, wraps

from toolz import merge
import pandas as pd

from ..base import tokenize


def rolling_chunk(func, part1, part2, window, *args):
    if part1.shape[0] < window-1:
        raise NotImplementedError("Window larger than partition size")
    if window > 1:
        extra = window - 1
        combined = pd.concat([part1.iloc[-extra:], part2])
        applied = func(combined, window, *args)
        return applied.iloc[extra:]
    else:
        return func(part2, window, *args)


def wrap_rolling(func):
    """Create a chunked version of a pandas.rolling_* function"""
    @wraps(func)
    def rolling(arg, window, *args, **kwargs):
        if not isinstance(window, int):
            raise TypeError('Window must be an integer')
        if window < 0:
            raise ValueError('Window must be a positive integer')
        if 'freq' in kwargs or 'how' in kwargs:
            raise NotImplementedError('Resampling before rolling computations '
                                      'not supported')
        old_name = arg._name
        token = tokenize(func, arg, window, args, kwargs)
        new_name = 'rolling-' + token
        f = partial(func, **kwargs)
        dsk = {(new_name, 0): (f, (old_name, 0), window) + args}
        for i in range(1, arg.npartitions + 1):
            dsk[(new_name, i)] = (rolling_chunk, f, (old_name, i - 1),
                                  (old_name, i), window) + args
        return arg._constructor(merge(arg.dask, dsk), new_name,
                                arg, arg.divisions)
    return rolling


rolling_count = wrap_rolling(pd.rolling_count)
rolling_sum = wrap_rolling(pd.rolling_sum)
rolling_mean = wrap_rolling(pd.rolling_mean)
rolling_median = wrap_rolling(pd.rolling_median)
rolling_min = wrap_rolling(pd.rolling_min)
rolling_max = wrap_rolling(pd.rolling_max)
rolling_std = wrap_rolling(pd.rolling_std)
rolling_var = wrap_rolling(pd.rolling_var)
rolling_skew = wrap_rolling(pd.rolling_skew)
rolling_kurt = wrap_rolling(pd.rolling_kurt)
rolling_quantile = wrap_rolling(pd.rolling_quantile)
rolling_apply = wrap_rolling(pd.rolling_apply)
rolling_window = wrap_rolling(pd.rolling_window)


def call_pandas_rolling_method_single(this_partition, rolling_kwargs,
        method_name, method_args, method_kwargs):
    # used for the start of the df/series (or for rolling through columns)
    method = getattr(this_partition.rolling(**rolling_kwargs), method_name)
    return method(*method_args, **method_kwargs)

def call_pandas_rolling_method_with_neighbors(
        prev_partition, this_partition, next_partition, before, after,
        rolling_kwargs, method_name, method_args, method_kwargs):
    if prev_partition.shape[0] != before or next_partition.shape[0] != after:
        raise NotImplementedError("Window requires larger inter-partition view than partition size")

    combined = pd.concat([prev_partition, this_partition, next_partition])
    method = getattr(combined.rolling(**rolling_kwargs), method_name)
    applied = method(*method_args, **method_kwargs)
    if after:
        return applied.iloc[before:-after]
    else:
        return applied.iloc[before:]

def tail(obj, n):
    return obj.tail(n)

def head(obj, n):
    return obj.head(n)

class Rolling(object):
    # What you get when you do ddf.rolling(...) or similar
    """Provides rolling window calculations.

    """

    def __init__(self, obj, window=None, min_periods=None, freq=None,
                 center=False, win_type=None, axis=0):
        if freq is not None:
            raise NotImplementedError(
                'The deprecated freq argument is not supported.')

        self.obj = obj # dataframe or series

        self.window = window
        self.min_periods = min_periods
        self.center = center
        self.win_type = win_type
        self.axis = axis

        # Allow pandas to raise if appropriate
        obj._pd.rolling(**self._rolling_kwargs())

    def _rolling_kwargs(self):
        return {
            'window': self.window,
            'min_periods': self.min_periods,
            'center': self.center,
            'win_type': self.win_type,
            'axis': self.axis}

    def _call_method(self, method_name, *args, **kwargs):
        args = list(args) # make sure dask does not mistake this for a task

        old_name = self.obj._name
        new_name = 'rolling-' + tokenize(
            self.obj, self._rolling_kwargs(), method_name, args, kwargs)

        dsk = {}
        if self.axis in [1, 'columns'] or self.window <= 1 or self.obj.npartitions == 1:
            # This is the easy scenario, we're rolling over columns (or not 
            # really rolling at all, so each chunk is independent.
            for i in range(self.obj.npartitions):
                dsk[new_name, i] = (
                    call_pandas_rolling_method_single, (old_name, i),
                    self._rolling_kwargs(), method_name, args, kwargs)
        else:
            # This is a bit trickier, we need to feed in information from the
            # neighbors to roll along rows.

            # Figure out how many we need to look at before and after.
            if self.center:
                before = self.window // 2
                after = self.window - before - 1
            else:
                before = self.window - 1
                after = 0

            head_name = 'head-{}-{}'.format(after, old_name)
            tail_name = 'tail-{}-{}'.format(before, old_name)

            # First chunk, only look after (if necessary)
            if after > 0:
                next_partition = (head_name, 1)
                dsk[next_partition] = (head, (old_name, 1), after)
            else:
                # Either we are only looking backward or this was the
                # only chunk.
                next_partition = self.obj._pd
            dsk[new_name, 0] = (call_pandas_rolling_method_with_neighbors,
                self.obj._pd, (old_name, 0), next_partition, 0, after,
                self._rolling_kwargs(), method_name, args, kwargs)

            # All the middle chunks
            for i in range(1, self.obj.npartitions-1):
                # Get just the needed values from the previous partition
                dsk[tail_name, i-1] = (tail, (old_name, i-1), before)
                if after:
                    next_partition = (head_name, i+1)
                    dsk[next_partition] = (head, (old_name, i+1), after)

                dsk[new_name, i] = (
                    call_pandas_rolling_method_with_neighbors,
                    (tail_name, i-1), (old_name, i), next_partition, before, after,
                    self._rolling_kwargs(), method_name, args, kwargs)

            # The last chunk
            if self.obj.npartitions > 1: # if the first wasn't the only partition
                end = self.obj.npartitions - 1
                dsk[tail_name, end-1] = (tail, (old_name, end-1), before)

                dsk[new_name, end] = (
                    call_pandas_rolling_method_with_neighbors,
                    (tail_name, end-1), (old_name, end), self.obj._pd, before, 0,
                    self._rolling_kwargs(), method_name, args, kwargs)

        # Do the pandas operation to get the appropriate thing for metadata
        pd_rolling = self.obj._pd.rolling(**self._rolling_kwargs())
        metadata = getattr(pd_rolling, method_name)(*args, **kwargs)

        return self.obj._constructor(
            merge(self.obj.dask, dsk),
            new_name,
            metadata,
            self.obj.divisions)

    def count(self, *args, **kwargs):
        return self._call_method('count', *args, **kwargs)

    def sum(self, *args, **kwargs):
        return self._call_method('sum', *args, **kwargs)

    def mean(self, *args, **kwargs):
        return self._call_method('mean', *args, **kwargs)

    def median(self, *args, **kwargs):
        return self._call_method('median', *args, **kwargs)

    def min(self, *args, **kwargs):
        return self._call_method('min', *args, **kwargs)

    def max(self, *args, **kwargs):
        return self._call_method('max', *args, **kwargs)

    def std(self, *args, **kwargs):
        return self._call_method('std', *args, **kwargs)

    def var(self, *args, **kwargs):
        return self._call_method('var', *args, **kwargs)

    def skew(self, *args, **kwargs):
        return self._call_method('skew', *args, **kwargs)

    def kurt(self, *args, **kwargs):
        return self._call_method('kurt', *args, **kwargs)

    def quantile(self, *args, **kwargs):
        return self._call_method('quantile', *args, **kwargs)

    def apply(self, *args, **kwargs):
        return self._call_method('apply', *args, **kwargs)

    def __repr__(self):
        return 'Rolling [{}]'.format(','.join(
            '{}={}'.format(k, v)
            for k, v in self._rolling_kwargs().items() if v is not None))
