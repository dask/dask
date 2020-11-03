import numpy as np
from pandas.core.window.ewm import EWM as pd_EWM

from ..base import tokenize
from ..utils import funcname, derived_from
from ..highlevelgraph import HighLevelGraph
from .core import _emulate
from .utils import make_meta


def map_ewm_adjust(func, adj, df, ewm_kwargs, *args, **kwargs):
    """Apply an ewm-type function to the dataframe, by applying it to every partition
        and adjusting the partial results.

    Parameters
    ----------
    func : function
        Function to be applied to the dataframe.
    df : dd.DataFrame, dd.Series
    adj: function
        Function that returns a single number based on parition's values, used in adjustment.
    args, kwargs :
        Arguments and keywords to pass to the function. The partition will
        be the first argument, and these will be passed *after*.
    """
    alpha = get_alpha(ewm_kwargs)
    axis = ewm_kwargs["axis"]

    adj_name = funcname(adj)
    func_name = "ewm-" + funcname(func)
    token = tokenize(func, adj, df, ewm_kwargs, *args, **kwargs)
    meta = _emulate(func, df, args, kwargs)
    meta = make_meta(meta, index=df._meta.index)
    name = "{0}-{1}".format(func_name, token)
    name_partial = "partial-" + func_name
    name_adj_rem = "adj-rem-" + adj_name
    name_adj_last = "adj-last-" + adj_name

    dsk = {}

    dsk.update(
        {
            (name_partial, i): (func, df.partitions[i], args, kwargs)
            for i in range(df.npartitions)
        }
    )

    def get_last_elements(partition):
        index = partition.shape[axis] - 1
        if axis == 0:
            last_elements = partition.loc[index, :]
        else:
            last_elements = partition.loc[:, index]
        return last_elements

    def get_adjustment(adj, partition, prev_partition):
        last_elements = get_last_elements(prev_partition)
        return (1 - alpha) ** partition.shape[axis] * (last_elements - adj(partition))

    def adjust_last_element(adj, partition, prev_partition):
        last_elements = get_last_elements(partition)
        last_elements += get_adjustment(adj, partition, prev_partition)

    partial_keys = list(dsk)

    dsk.update(
        {
            (name_adj_last, i): (
                adjust_last_element,
                adj,
                partial_keys[i],
                partial_keys[i - 1],
            )
            for i in range(1, len(partial_keys))
        }
    )

    partial_last_adjusted_keys = [(partial_keys[0], 0)] + [
        (name_adj_last, i) for i in range(1, len(partial_keys))
    ]

    def adjust_remaining_elements(adj, partition, prev_partition):
        index = partition.shape[axis] - 1
        remaining_elements = (
            partition.loc[:index, :] if axis == 0 else partition.iloc[:, :index]
        )
        remaining_elements += get_adjustment(adj, partition, prev_partition)

    dsk.update({(name, 0): ()})
    dsk.update(
        {
            (name, i): (
                adjust_remaining_elements,
                adj,
                partial_last_adjusted_keys[i],
                partial_last_adjusted_keys[i - 1],
            )
            for i in range(1, len(partial_last_adjusted_keys))
        }
    )
    # print(dsk)
    graph = HighLevelGraph.from_collections(name, dsk, dependencies=[df])
    return df._constructor(graph, name, meta, df.divisions)


def pandas_ewm_method(df, args, kwargs):
    ewm_kwargs = args[0]
    name = args[1]
    args = args[2:]
    ewm = df.ewm(**ewm_kwargs)
    return getattr(ewm, name)(*args, **kwargs)


def get_alpha(ewm_kwargs):
    arg_options = ["com", "span", "halflife"]
    alpha_comp = {
        "com": lambda x: 1 / (1 + x),
        "span": lambda x: 2 / (x + 1),
        "halflife": lambda x: 1 - np.exp(np.log(0.5) / x),
    }

    for option in arg_options:
        if option in ewm_kwargs and ewm_kwargs[option] is not None:
            return alpha_comp[option](ewm_kwargs[option])


class EWM:
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

    def _call_method(self, method_name, adj, *args, **kwargs):
        ewm_kwargs = self._ewm_kwargs()
        args = (ewm_kwargs, method_name) + args
        return map_ewm_adjust(
            pandas_ewm_method, adj, self.obj, ewm_kwargs, *args, **kwargs,
        )

    @derived_from(pd_EWM)
    def mean(self):
        return self._call_method("mean", lambda x: 0)

    @derived_from(pd_EWM)
    def var(self, bias=True):
        if not bias:
            raise ValueError("bias=false is not yet implemented.")

        df_sq_mean = self.obj.mul(self.obj).ewm(**self._ewm_kwargs()).mean()
        df_mean = self.obj.ewm(**self._ewm_kwargs()).mean()
        df_mean_sq = df_mean.mul(df_mean)
        return df_sq_mean - df_mean_sq

    @derived_from(pd_EWM)
    def std(self, bias=True):
        return self.var(bias).pow(0.5)
