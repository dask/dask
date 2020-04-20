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
        min_periods=None,
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
