from ..base import tokenize
from ..utils import funcname
from ..highlevelgraph import HighLevelGraph
from .core import _emulate
from .utils import make_meta


def map_ewm_merge(func, merge_func, df, *args, **kwargs):
    """Apply an ewm-type function to the dataframe, merging the results of applying
       the function to an increasing prefix of partitions.

    Parameters
    ----------
    func : function
        Function applied to the dataframe.
    df : dd.DataFrame, dd.Series
    merge_func: function
        Function that merges the partial result of applying func to the current partition
        with results from previous partitions.
    args, kwargs :
        Arguments and keywords to pass to the function. The partition will
        be the first argument, and these will be passed *after*.
    """

    merge_func_name = funcname(merge_func)
    if "token" in kwargs:
        func_name = kwargs.pop("token")
        token = tokenize(df, *args, **kwargs)
    else:
        func_name = "ewm-" + funcname(func)
        token = tokenize(func, merge_func, df, *args, **kwargs)

    if "meta" in kwargs:
        meta = kwargs.pop("meta")
    else:
        meta = _emulate(func, df, *args, **kwargs)
    meta = make_meta(meta, index=df._meta.index)

    name = "{0}-{1}".format(func_name, token)
    name_partial = "partial-" + func_name
    name_merge = "merge-" + merge_func_name

    dsk = {}

    for i, input_key in enumerate(df.__dask_keys__()):
        dsk.update({(name_partial, i): (func, input_key, args, kwargs)})
        kwargs["min_periods"] = max(
            0, kwargs["min_periods"] - df.partitions[i].shape[kwargs["axis"]]
        )

    partial_keys = list(dsk)
    dsk.update(
        {
            (name_merge, i): (
                merge_func,
                input_key,
                partial_keys[:i],
                df.__dask_keys__()[:i],
            )
            for i, input_key in enumerate(partial_keys[1:], 1)
        }
    )

    graph = HighLevelGraph.from_collections(name, dsk, dependencies=[df])
    return df._constructor(graph, name, meta, df.divisions)


def pandas_ewm_method(df, ewm_kwargs, name, *args, **kwargs):
    ewm = df.ewm(**ewm_kwargs)
    return getattr(ewm, name)(*args, **kwargs)


class EWM(object):
    """Provides exponentially weighted calculations."""

    def __init__(
        self,
        obj,
        com=None,
        span=None,
        halflife=None,
        alpha=None,
        min_periods=0,
        adjust=True,
        ignore_na=False,
        axis=0,
    ):
        if adjust:
            raise NotImplementedError("Only unadjusted implementation available")
        self.obj = obj  # dataframe or series
        self.com = com
        self.span = span
        self.halflife = halflife
        self.alpha = alpha
        self.min_periods = min_periods
        self.adjust = adjust
        self.ignore_na = ignore_na
        self.axis = axis
        # Allow pandas to raise if appropriate
        obj._meta.ewm(**self._ewm_kwargs())

    def _ewm_kwargs(self):
        return {
            "com": self.com,
            "span": self.span,
            "halflife": self.halflife,
            "alpha": self.alpha,
            "min_periods": self.min_periods,
            "adjust": self.adjust,
            "ignore_na": self.ignore_na,
            "axis": self.axis,
        }
