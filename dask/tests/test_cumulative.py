"""
Tests cumulative operations on dask dataframes
"""

import pytest

from dask.utils import ignoring
from dask import compute


def import_or_none(path):
    with ignoring():
        return pytest.importorskip(path)


dd = import_or_none('dask.dataframe')
pd = import_or_none('pandas')


@pytest.mark.skipif('not dd')
def test_groupby_cum_caching():
    """Test caching behavior of cumulative operations on grouped dataframes.

    Relates to #3756.
    """
    df = pd.DataFrame(dict(a=list('aabbcc')),
                      index=pd.date_range(start='20100101', periods=6))
    df['ones'] = 1
    df['twos'] = 2

    ddf = dd.from_pandas(df, npartitions=3)

    ops = ['cumsum', 'cumprod']

    for op in ops:
        ddf0 = getattr(ddf.groupby(['a']), op)()
        ddf1 = ddf.rename(columns={'ones': 'foo', 'twos': 'bar'})
        ddf1 = getattr(ddf1.groupby(['a']), op)()

        # _a and _b dataframe should be equal
        res0_a, res1_a = compute(ddf0, ddf1, scheduler='sync')
        res0_b, res1_b = ddf0.compute(), ddf1.compute()

        assert res0_a.equals(res0_b)
        assert res1_a.equals(res1_b)


@pytest.mark.skipif('not dd')
def test_cum_caching():
    """Test caching behavior of cumulative operations on grouped Series

    Relates to #3755
    """
    df = pd.DataFrame(dict(a=list('aabbcc')),
                      index=pd.date_range(start='20100101', periods=6))
    df['ones'] = 1
    df['twos'] = 2

    ops = ['cumsum', 'cumprod']
    for op in ops:
        ddf = dd.from_pandas(df, npartitions=3)
        dcum = ddf.groupby(['a'])
        res0_a, res1_a = compute(getattr(dcum['ones'], op)(),
                                 getattr(dcum['twos'], op)())
        cum = df.groupby(['a'])
        res0_b, res1_b = getattr(cum['ones'], op)(), \
                         getattr(cum['twos'], op)()

        assert res0_a.equals(res0_b)
        assert res1_a.equals(res1_b)
