from __future__ import annotations

import pandas as pd

from dask.dataframe import methods


def _resample_series(
    series,
    start,
    end,
    reindex_closed,
    rule,
    resample_kwargs,
    how,
    fill_value,
    how_args,
    how_kwargs,
):
    out = getattr(series.resample(rule, **resample_kwargs), how)(
        *how_args, **how_kwargs
    )
    if reindex_closed is None:
        inclusive = "both"
    else:
        inclusive = reindex_closed
    closed_kwargs = {"inclusive": inclusive}

    new_index = pd.date_range(
        start.tz_localize(None),
        end.tz_localize(None),
        freq=rule,
        **closed_kwargs,
        name=out.index.name,
        unit=out.index.unit,
    ).tz_localize(start.tz, nonexistent="shift_forward")

    if not out.index.isin(new_index).all():
        raise ValueError(
            "Index is not contained within new index. This can often be "
            "resolved by using larger partitions, or unambiguous "
            "frequencies: 'Q', 'A'..."
        )

    return out.reindex(new_index, fill_value=fill_value)


def _resample_bin_and_out_divs(divisions, rule, closed="left", label="left"):
    rule = pd.tseries.frequencies.to_offset(rule)
    g = pd.Grouper(freq=rule, how="count", closed=closed, label=label)

    # Determine bins to apply `how` to. Disregard labeling scheme.
    divs = pd.Series(range(len(divisions)), index=divisions)
    temp = divs.resample(rule, closed=closed, label="left").count()
    tempdivs = temp.loc[temp > 0].index

    # Cleanup closed == 'right' and label == 'right'
    res = pd.offsets.Nano() if isinstance(rule, pd.offsets.Tick) else pd.offsets.Day()
    if g.closed == "right":
        newdivs = tempdivs + res
    else:
        newdivs = tempdivs
    if g.label == "right":
        outdivs = tempdivs + rule
    else:
        outdivs = tempdivs

    newdivs = methods.tolist(newdivs)
    outdivs = methods.tolist(outdivs)

    # Adjust ends
    if newdivs[0] < divisions[0]:
        newdivs[0] = divisions[0]
    if newdivs[-1] < divisions[-1]:
        if len(newdivs) < len(divs):
            setter = lambda a, val: a.append(val)
        else:
            setter = lambda a, val: a.__setitem__(-1, val)
        setter(newdivs, divisions[-1] + res)
        if outdivs[-1] > divisions[-1]:
            setter(outdivs, outdivs[-1])
        elif outdivs[-1] < divisions[-1]:
            setter(outdivs, temp.index[-1])

    return tuple(map(pd.Timestamp, newdivs)), tuple(map(pd.Timestamp, outdivs))
