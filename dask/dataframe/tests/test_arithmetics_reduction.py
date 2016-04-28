from datetime import datetime

import numpy as np
import pandas as pd

from dask.utils import raises
import dask.dataframe as dd
from dask.dataframe.utils import eq, assert_dask_graph


def test_arithmetics():
    dsk = {('x', 0): pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]},
                                  index=[0, 1, 3]),
           ('x', 1): pd.DataFrame({'a': [4, 5, 6], 'b': [3, 2, 1]},
                                  index=[5, 6, 8]),
           ('x', 2): pd.DataFrame({'a': [7, 8, 9], 'b': [0, 0, 0]},
                                  index=[9, 9, 9])}
    ddf1 = dd.DataFrame(dsk, 'x', ['a', 'b'], [0, 4, 9, 9])
    pdf1 = ddf1.compute()

    pdf2 = pd.DataFrame({'a': [1, 2, 3, 4, 5, 6, 7, 8],
                         'b': [5, 6, 7, 8, 1, 2, 3, 4]})
    pdf3 = pd.DataFrame({'a': [5, 6, 7, 8, 4, 3, 2, 1],
                         'b': [2, 4, 5, 3, 4, 2, 1, 0]})
    ddf2 = dd.from_pandas(pdf2, 3)
    ddf3 = dd.from_pandas(pdf3, 2)

    dsk4 = {('y', 0): pd.DataFrame({'a': [3, 2, 1], 'b': [7, 8, 9]},
                                   index=[0, 1, 3]),
            ('y', 1): pd.DataFrame({'a': [5, 2, 8], 'b': [4, 2, 3]},
                                   index=[5, 6, 8]),
            ('y', 2): pd.DataFrame({'a': [1, 4, 10], 'b': [1, 0, 5]},
                                   index=[9, 9, 9])}
    ddf4 = dd.DataFrame(dsk4, 'y', ['a', 'b'], [0, 4, 9, 9])
    pdf4 = ddf4.compute()

    # Arithmetics
    cases = [(ddf1, ddf1, pdf1, pdf1),
             (ddf1, ddf1.repartition([0, 1, 3, 6, 9]), pdf1, pdf1),
             (ddf2, ddf3, pdf2, pdf3),
             (ddf2.repartition([0, 3, 6, 7]), ddf3.repartition([0, 7]),
              pdf2, pdf3),
             (ddf2.repartition([0, 7]), ddf3.repartition([0, 2, 4, 5, 7]),
              pdf2, pdf3),
             (ddf1, ddf4, pdf1, pdf4),
             (ddf1, ddf4.repartition([0, 9]), pdf1, pdf4),
             (ddf1.repartition([0, 3, 9]), ddf4.repartition([0, 5, 9]),
              pdf1, pdf4),
             # dask + pandas
             (ddf1, pdf4, pdf1, pdf4), (ddf2, pdf3, pdf2, pdf3)]

    for (l, r, el, er) in cases:
        check_series_arithmetics(l.a, r.b, el.a, er.b)
        check_frame_arithmetics(l, r, el, er)

    # different index, pandas raises ValueError in comparison ops

    pdf5 = pd.DataFrame({'a': [3, 2, 1, 5, 2, 8, 1, 4, 10],
                         'b': [7, 8, 9, 4, 2, 3, 1, 0, 5]},
                        index=[0, 1, 3, 5, 6, 8, 9, 9, 9])
    ddf5 = dd.from_pandas(pdf5, 2)

    pdf6 = pd.DataFrame({'a': [3, 2, 1, 5, 2, 8, 1, 4, 10],
                         'b': [7, 8, 9, 5, 7, 8, 4, 2, 5]},
                        index=[0, 1, 2, 3, 4, 5, 6, 7, 9])
    ddf6 = dd.from_pandas(pdf6, 4)

    pdf7 = pd.DataFrame({'a': [1, 2, 3, 4, 5, 6, 7, 8],
                         'b': [5, 6, 7, 8, 1, 2, 3, 4]},
                        index=list('aaabcdeh'))
    pdf8 = pd.DataFrame({'a': [5, 6, 7, 8, 4, 3, 2, 1],
                         'b': [2, 4, 5, 3, 4, 2, 1, 0]},
                        index=list('abcdefgh'))
    ddf7 = dd.from_pandas(pdf7, 3)
    ddf8 = dd.from_pandas(pdf8, 4)

    pdf9 = pd.DataFrame({'a': [1, 2, 3, 4, 5, 6, 7, 8],
                         'b': [5, 6, 7, 8, 1, 2, 3, 4],
                         'c': [5, 6, 7, 8, 1, 2, 3, 4]},
                        index=list('aaabcdeh'))
    pdf10 = pd.DataFrame({'b': [5, 6, 7, 8, 4, 3, 2, 1],
                          'c': [2, 4, 5, 3, 4, 2, 1, 0],
                          'd': [2, 4, 5, 3, 4, 2, 1, 0]},
                         index=list('abcdefgh'))
    ddf9 = dd.from_pandas(pdf9, 3)
    ddf10 = dd.from_pandas(pdf10, 4)

    # Arithmetics with different index
    cases = [(ddf5, ddf6, pdf5, pdf6),
             (ddf5.repartition([0, 9]), ddf6, pdf5, pdf6),
             (ddf5.repartition([0, 5, 9]), ddf6.repartition([0, 7, 9]),
              pdf5, pdf6),
             (ddf7, ddf8, pdf7, pdf8),
             (ddf7.repartition(['a', 'c', 'h']), ddf8.repartition(['a', 'h']),
              pdf7, pdf8),
             (ddf7.repartition(['a', 'b', 'e', 'h']),
              ddf8.repartition(['a', 'e', 'h']), pdf7, pdf8),
             (ddf9, ddf10, pdf9, pdf10),
             (ddf9.repartition(['a', 'c', 'h']), ddf10.repartition(['a', 'h']),
              pdf9, pdf10),
             # dask + pandas
             (ddf5, pdf6, pdf5, pdf6), (ddf7, pdf8, pdf7, pdf8),
             (ddf9, pdf10, pdf9, pdf10)]

    for (l, r, el, er) in cases:
        check_series_arithmetics(l.a, r.b, el.a, er.b,
                                 allow_comparison_ops=False)
        check_frame_arithmetics(l, r, el, er,
                                allow_comparison_ops=False)


def test_arithmetics_different_index():
    # index are different, but overwraps
    pdf1 = pd.DataFrame({'a': [1, 2, 3, 4, 5], 'b': [3, 5, 2, 5, 7]},
                        index=[1, 2, 3, 4, 5])
    ddf1 = dd.from_pandas(pdf1, 2)
    pdf2 = pd.DataFrame({'a': [3, 2, 6, 7, 8], 'b': [9, 4, 2, 6, 2]},
                        index=[3, 4, 5, 6, 7])
    ddf2 = dd.from_pandas(pdf2, 2)

    # index are not overwrapped
    pdf3 = pd.DataFrame({'a': [1, 2, 3, 4, 5], 'b': [3, 5, 2, 5, 7]},
                        index=[1, 2, 3, 4, 5])
    ddf3 = dd.from_pandas(pdf3, 2)
    pdf4 = pd.DataFrame({'a': [3, 2, 6, 7, 8], 'b': [9, 4, 2, 6, 2]},
                        index=[10, 11, 12, 13, 14])
    ddf4 = dd.from_pandas(pdf4, 2)

    # index is included in another
    pdf5 = pd.DataFrame({'a': [1, 2, 3, 4, 5], 'b': [3, 5, 2, 5, 7]},
                        index=[1, 3, 5, 7, 9])
    ddf5 = dd.from_pandas(pdf5, 2)
    pdf6 = pd.DataFrame({'a': [3, 2, 6, 7, 8], 'b': [9, 4, 2, 6, 2]},
                        index=[2, 3, 4, 5, 6])
    ddf6 = dd.from_pandas(pdf6, 2)

    cases = [(ddf1, ddf2, pdf1, pdf2),
             (ddf2, ddf1, pdf2, pdf1),
             (ddf1.repartition([1, 3, 5]), ddf2.repartition([3, 4, 7]),
              pdf1, pdf2),
             (ddf2.repartition([3, 4, 5, 7]), ddf1.repartition([1, 2, 4, 5]),
              pdf2, pdf1),
             (ddf3, ddf4, pdf3, pdf4),
             (ddf4, ddf3, pdf4, pdf3),
             (ddf3.repartition([1, 2, 3, 4, 5]),
              ddf4.repartition([10, 11, 12, 13, 14]), pdf3, pdf4),
             (ddf4.repartition([10, 14]), ddf3.repartition([1, 3, 4, 5]),
              pdf4, pdf3),
             (ddf5, ddf6, pdf5, pdf6),
             (ddf6, ddf5, pdf6, pdf5),
             (ddf5.repartition([1, 7, 8, 9]), ddf6.repartition([2, 3, 4, 6]),
              pdf5, pdf6),
             (ddf6.repartition([2, 6]), ddf5.repartition([1, 3, 7, 9]),
              pdf6, pdf5),
             # dask + pandas
             (ddf1, pdf2, pdf1, pdf2), (ddf2, pdf1, pdf2, pdf1),
             (ddf3, pdf4, pdf3, pdf4), (ddf4, pdf3, pdf4, pdf3),
             (ddf5, pdf6, pdf5, pdf6), (ddf6, pdf5, pdf6, pdf5)]

    for (l, r, el, er) in cases:
        check_series_arithmetics(l.a, r.b, el.a, er.b,
                                 allow_comparison_ops=False)
        check_frame_arithmetics(l, r, el, er,
                                allow_comparison_ops=False)

    pdf7 = pd.DataFrame({'a': [1, 2, 3, 4, 5, 6, 7, 8],
                         'b': [5, 6, 7, 8, 1, 2, 3, 4]},
                        index=[0, 2, 4, 8, 9, 10, 11, 13])
    pdf8 = pd.DataFrame({'a': [5, 6, 7, 8, 4, 3, 2, 1],
                         'b': [2, 4, 5, 3, 4, 2, 1, 0]},
                        index=[1, 3, 4, 8, 9, 11, 12, 13])
    ddf7 = dd.from_pandas(pdf7, 3)
    ddf8 = dd.from_pandas(pdf8, 2)

    pdf9 = pd.DataFrame({'a': [1, 2, 3, 4, 5, 6, 7, 8],
                         'b': [5, 6, 7, 8, 1, 2, 3, 4]},
                        index=[0, 2, 4, 8, 9, 10, 11, 13])
    pdf10 = pd.DataFrame({'a': [5, 6, 7, 8, 4, 3, 2, 1],
                          'b': [2, 4, 5, 3, 4, 2, 1, 0]},
                         index=[0, 3, 4, 8, 9, 11, 12, 13])
    ddf9 = dd.from_pandas(pdf9, 3)
    ddf10 = dd.from_pandas(pdf10, 2)

    cases = [(ddf7, ddf8, pdf7, pdf8),
             (ddf8, ddf7, pdf8, pdf7),
             # (ddf7.repartition([0, 13]),
             #  ddf8.repartition([0, 4, 11, 14], force=True),
             #  pdf7, pdf8),
             (ddf8.repartition([-5, 10, 15], force=True),
              ddf7.repartition([-1, 4, 11, 14], force=True), pdf8, pdf7),
             (ddf7.repartition([0, 8, 12, 13]),
              ddf8.repartition([0, 2, 8, 12, 13], force=True), pdf7, pdf8),
             (ddf8.repartition([-5, 0, 10, 20], force=True),
              ddf7.repartition([-1, 4, 11, 13], force=True), pdf8, pdf7),
             (ddf9, ddf10, pdf9, pdf10),
             (ddf10, ddf9, pdf10, pdf9),
             # dask + pandas
             (ddf7, pdf8, pdf7, pdf8), (ddf8, pdf7, pdf8, pdf7),
             (ddf9, pdf10, pdf9, pdf10), (ddf10, pdf9, pdf10, pdf9)]

    for (l, r, el, er) in cases:
        check_series_arithmetics(l.a, r.b, el.a, er.b,
                                 allow_comparison_ops=False)
        check_frame_arithmetics(l, r, el, er,
                                allow_comparison_ops=False)


def check_series_arithmetics(l, r, el, er, allow_comparison_ops=True):
    assert isinstance(l, dd.Series)
    assert isinstance(r, (dd.Series, pd.Series))
    assert isinstance(el, pd.Series)
    assert isinstance(er, pd.Series)

    # l, r may be repartitioned, test whether repartition keeps original data
    assert eq(l, el)
    assert eq(r, er)

    assert eq(l + r, el + er)
    assert eq(l * r, el * er)
    assert eq(l - r, el - er)
    assert eq(l / r, el / er)
    assert eq(l // r, el // er)
    assert eq(l ** r, el ** er)
    assert eq(l % r, el % er)

    if allow_comparison_ops:
        # comparison is allowed if data have same index
        assert eq(l & r, el & er)
        assert eq(l | r, el | er)
        assert eq(l ^ r, el ^ er)
        assert eq(l > r, el > er)
        assert eq(l < r, el < er)
        assert eq(l >= r, el >= er)
        assert eq(l <= r, el <= er)
        assert eq(l == r, el == er)
        assert eq(l != r, el != er)

    assert eq(l + 2, el + 2)
    assert eq(l * 2, el * 2)
    assert eq(l - 2, el - 2)
    assert eq(l / 2, el / 2)
    assert eq(l & True, el & True)
    assert eq(l | True, el | True)
    assert eq(l ^ True, el ^ True)
    assert eq(l // 2, el // 2)
    assert eq(l ** 2, el ** 2)
    assert eq(l % 2, el % 2)
    assert eq(l > 2, el > 2)
    assert eq(l < 2, el < 2)
    assert eq(l >= 2, el >= 2)
    assert eq(l <= 2, el <= 2)
    assert eq(l == 2, el == 2)
    assert eq(l != 2, el != 2)

    assert eq(2 + r, 2 + er)
    assert eq(2 * r, 2 * er)
    assert eq(2 - r, 2 - er)
    assert eq(2 / r, 2 / er)
    assert eq(True & r, True & er)
    assert eq(True | r, True | er)
    assert eq(True ^ r, True ^ er)
    assert eq(2 // r, 2 // er)
    assert eq(2 ** r, 2 ** er)
    assert eq(2 % r, 2 % er)
    assert eq(2 > r, 2 > er)
    assert eq(2 < r, 2 < er)
    assert eq(2 >= r, 2 >= er)
    assert eq(2 <= r, 2 <= er)
    assert eq(2 == r, 2 == er)
    assert eq(2 != r, 2 != er)

    assert eq(-l, -el)
    assert eq(abs(l), abs(el))

    if allow_comparison_ops:
        # comparison is allowed if data have same index
        assert eq(~(l == r), ~(el == er))


def check_frame_arithmetics(l, r, el, er, allow_comparison_ops=True):
    assert isinstance(l, dd.DataFrame)
    assert isinstance(r, (dd.DataFrame, pd.DataFrame))
    assert isinstance(el, pd.DataFrame)
    assert isinstance(er, pd.DataFrame)
    # l, r may be repartitioned, test whether repartition keeps original data
    assert eq(l, el)
    assert eq(r, er)

    assert eq(l + r, el + er)
    assert eq(l * r, el * er)
    assert eq(l - r, el - er)
    assert eq(l / r, el / er)
    assert eq(l // r, el // er)
    assert eq(l ** r, el ** er)
    assert eq(l % r, el % er)

    if allow_comparison_ops:
        # comparison is allowed if data have same index
        assert eq(l & r, el & er)
        assert eq(l | r, el | er)
        assert eq(l ^ r, el ^ er)
        assert eq(l > r, el > er)
        assert eq(l < r, el < er)
        assert eq(l >= r, el >= er)
        assert eq(l <= r, el <= er)
        assert eq(l == r, el == er)
        assert eq(l != r, el != er)

    assert eq(l + 2, el + 2)
    assert eq(l * 2, el * 2)
    assert eq(l - 2, el - 2)
    assert eq(l / 2, el / 2)
    assert eq(l & True, el & True)
    assert eq(l | True, el | True)
    assert eq(l ^ True, el ^ True)
    assert eq(l // 2, el // 2)
    assert eq(l ** 2, el ** 2)
    assert eq(l % 2, el % 2)
    assert eq(l > 2, el > 2)
    assert eq(l < 2, el < 2)
    assert eq(l >= 2, el >= 2)
    assert eq(l <= 2, el <= 2)
    assert eq(l == 2, el == 2)
    assert eq(l != 2, el != 2)

    assert eq(2 + l, 2 + el)
    assert eq(2 * l, 2 * el)
    assert eq(2 - l, 2 - el)
    assert eq(2 / l, 2 / el)
    assert eq(True & l, True & el)
    assert eq(True | l, True | el)
    assert eq(True ^ l, True ^ el)
    assert eq(2 // l, 2 // el)
    assert eq(2 ** l, 2 ** el)
    assert eq(2 % l, 2 % el)
    assert eq(2 > l, 2 > el)
    assert eq(2 < l, 2 < el)
    assert eq(2 >= l, 2 >= el)
    assert eq(2 <= l, 2 <= el)
    assert eq(2 == l, 2 == el)
    assert eq(2 != l, 2 != el)

    assert eq(-l, -el)
    assert eq(abs(l), abs(el))

    if allow_comparison_ops:
        # comparison is allowed if data have same index
        assert eq(~(l == r), ~(el == er))


def test_scalar_arithmetics():
    l = dd.core.Scalar({('l', 0): 10}, 'l')
    r = dd.core.Scalar({('r', 0): 4}, 'r')
    el = 10
    er = 4

    assert isinstance(l, dd.core.Scalar)
    assert isinstance(r, dd.core.Scalar)

    # l, r may be repartitioned, test whether repartition keeps original data
    assert eq(l, el)
    assert eq(r, er)

    assert eq(l + r, el + er)
    assert eq(l * r, el * er)
    assert eq(l - r, el - er)
    assert eq(l / r, el / er)
    assert eq(l // r, el // er)
    assert eq(l ** r, el ** er)
    assert eq(l % r, el % er)

    assert eq(l & r, el & er)
    assert eq(l | r, el | er)
    assert eq(l ^ r, el ^ er)
    assert eq(l > r, el > er)
    assert eq(l < r, el < er)
    assert eq(l >= r, el >= er)
    assert eq(l <= r, el <= er)
    assert eq(l == r, el == er)
    assert eq(l != r, el != er)

    assert eq(l + 2, el + 2)
    assert eq(l * 2, el * 2)
    assert eq(l - 2, el - 2)
    assert eq(l / 2, el / 2)
    assert eq(l & True, el & True)
    assert eq(l | True, el | True)
    assert eq(l ^ True, el ^ True)
    assert eq(l // 2, el // 2)
    assert eq(l ** 2, el ** 2)
    assert eq(l % 2, el % 2)
    assert eq(l > 2, el > 2)
    assert eq(l < 2, el < 2)
    assert eq(l >= 2, el >= 2)
    assert eq(l <= 2, el <= 2)
    assert eq(l == 2, el == 2)
    assert eq(l != 2, el != 2)

    assert eq(2 + r, 2 + er)
    assert eq(2 * r, 2 * er)
    assert eq(2 - r, 2 - er)
    assert eq(2 / r, 2 / er)
    assert eq(True & r, True & er)
    assert eq(True | r, True | er)
    assert eq(True ^ r, True ^ er)
    assert eq(2 // r, 2 // er)
    assert eq(2 ** r, 2 ** er)
    assert eq(2 % r, 2 % er)
    assert eq(2 > r, 2 > er)
    assert eq(2 < r, 2 < er)
    assert eq(2 >= r, 2 >= er)
    assert eq(2 <= r, 2 <= er)
    assert eq(2 == r, 2 == er)
    assert eq(2 != r, 2 != er)

    assert eq(-l, -el)
    assert eq(abs(l), abs(el))

    assert eq(~(l == r), ~(el == er))


def test_scalar_arithmetics_with_dask_instances():
    s = dd.core.Scalar({('s', 0): 10}, 's')
    e = 10

    pds = pd.Series([1, 2, 3, 4, 5, 6, 7])
    dds = dd.from_pandas(pds, 2)

    pdf = pd.DataFrame({'a': [1, 2, 3, 4, 5, 6, 7],
                        'b': [7, 6, 5, 4, 3, 2, 1]})
    ddf = dd.from_pandas(pdf, 2)

    # pandas Series
    result = pds + s   # this result pd.Series (automatically computed)
    assert isinstance(result, pd.Series)
    assert eq(result, pds + e)

    result = s + pds   # this result dd.Series
    assert isinstance(result, dd.Series)
    assert eq(result, pds + e)

    # dask Series
    result = dds + s   # this result dd.Series
    assert isinstance(result, dd.Series)
    assert eq(result, pds + e)

    result = s + dds   # this result dd.Series
    assert isinstance(result, dd.Series)
    assert eq(result, pds + e)


    # pandas DataFrame
    result = pdf + s   # this result pd.DataFrame (automatically computed)
    assert isinstance(result, pd.DataFrame)
    assert eq(result, pdf + e)

    result = s + pdf   # this result dd.DataFrame
    assert isinstance(result, dd.DataFrame)
    assert eq(result, pdf + e)

    # dask DataFrame
    result = ddf + s   # this result dd.DataFrame
    assert isinstance(result, dd.DataFrame)
    assert eq(result, pdf + e)

    result = s + ddf   # this result dd.DataFrame
    assert isinstance(result, dd.DataFrame)
    assert eq(result, pdf + e)


def test_frame_series_arithmetic_methods():
    pdf1 = pd.DataFrame({'A': np.arange(10),
                         'B': [np.nan, 1, 2, 3, 4] * 2,
                         'C': [np.nan] * 10,
                         'D': np.arange(10)},
                        index=list('abcdefghij'), columns=list('ABCD'))
    pdf2 = pd.DataFrame(np.random.randn(10, 4),
                        index=list('abcdefghjk'), columns=list('ABCX'))
    ps1 = pdf1.A
    ps2 = pdf2.A

    ddf1 = dd.from_pandas(pdf1, 2)
    ddf2 = dd.from_pandas(pdf2, 2)
    ds1 = ddf1.A
    ds2 = ddf2.A

    s = dd.core.Scalar({('s', 0): 4}, 's')

    for l, r, el, er in [(ddf1, ddf2, pdf1, pdf2), (ds1, ds2, ps1, ps2),
                         (ddf1.repartition(['a', 'f', 'j']), ddf2, pdf1, pdf2),
                         (ds1.repartition(['a', 'b', 'f', 'j']), ds2, ps1, ps2),
                         (ddf1, ddf2.repartition(['a', 'k']), pdf1, pdf2),
                         (ds1, ds2.repartition(['a', 'b', 'd', 'h', 'k']), ps1, ps2),
                         (ddf1, 3, pdf1, 3), (ds1, 3, ps1, 3),
                         (ddf1, s, pdf1, 4), (ds1, s, ps1, 4)]:
        # l, r may be repartitioned, test whether repartition keeps original data
        assert eq(l, el)
        assert eq(r, er)

        assert eq(l.add(r, fill_value=0), el.add(er, fill_value=0))
        assert eq(l.sub(r, fill_value=0), el.sub(er, fill_value=0))
        assert eq(l.mul(r, fill_value=0), el.mul(er, fill_value=0))
        assert eq(l.div(r, fill_value=0), el.div(er, fill_value=0))
        assert eq(l.truediv(r, fill_value=0), el.truediv(er, fill_value=0))
        assert eq(l.floordiv(r, fill_value=1), el.floordiv(er, fill_value=1))
        assert eq(l.mod(r, fill_value=0), el.mod(er, fill_value=0))
        assert eq(l.pow(r, fill_value=0), el.pow(er, fill_value=0))

        assert eq(l.radd(r, fill_value=0), el.radd(er, fill_value=0))
        assert eq(l.rsub(r, fill_value=0), el.rsub(er, fill_value=0))
        assert eq(l.rmul(r, fill_value=0), el.rmul(er, fill_value=0))
        assert eq(l.rdiv(r, fill_value=0), el.rdiv(er, fill_value=0))
        assert eq(l.rtruediv(r, fill_value=0), el.rtruediv(er, fill_value=0))
        assert eq(l.rfloordiv(r, fill_value=1), el.rfloordiv(er, fill_value=1))
        assert eq(l.rmod(r, fill_value=0), el.rmod(er, fill_value=0))
        assert eq(l.rpow(r, fill_value=0), el.rpow(er, fill_value=0))

    for l, r, el, er in [(ddf1, ds2, pdf1, ps2), (ddf1, ddf2.X, pdf1, pdf2.X)]:
        assert eq(l, el)
        assert eq(r, er)

        # must specify axis=0 to add Series to each column
        # axis=1 is not supported (add to each row)
        assert eq(l.add(r, axis=0), el.add(er, axis=0))
        assert eq(l.sub(r, axis=0), el.sub(er, axis=0))
        assert eq(l.mul(r, axis=0), el.mul(er, axis=0))
        assert eq(l.div(r, axis=0), el.div(er, axis=0))
        assert eq(l.truediv(r, axis=0), el.truediv(er, axis=0))
        assert eq(l.floordiv(r, axis=0), el.floordiv(er, axis=0))
        assert eq(l.mod(r, axis=0), el.mod(er, axis=0))
        assert eq(l.pow(r, axis=0), el.pow(er, axis=0))

        assert eq(l.radd(r, axis=0), el.radd(er, axis=0))
        assert eq(l.rsub(r, axis=0), el.rsub(er, axis=0))
        assert eq(l.rmul(r, axis=0), el.rmul(er, axis=0))
        assert eq(l.rdiv(r, axis=0), el.rdiv(er, axis=0))
        assert eq(l.rtruediv(r, axis=0), el.rtruediv(er, axis=0))
        assert eq(l.rfloordiv(r, axis=0), el.rfloordiv(er, axis=0))
        assert eq(l.rmod(r, axis=0), el.rmod(er, axis=0))
        assert eq(l.rpow(r, axis=0), el.rpow(er, axis=0))

        assert raises(ValueError, lambda: l.add(r, axis=1))

    for l, r, el, er in [(ddf1, pdf2, pdf1, pdf2), (ddf1, ps2, pdf1, ps2)]:
        assert eq(l, el)
        assert eq(r, er)

        for axis in [0, 1, 'index', 'columns']:
            assert eq(l.add(r, axis=axis), el.add(er, axis=axis))
            assert eq(l.sub(r, axis=axis), el.sub(er, axis=axis))
            assert eq(l.mul(r, axis=axis), el.mul(er, axis=axis))
            assert eq(l.div(r, axis=axis), el.div(er, axis=axis))
            assert eq(l.truediv(r, axis=axis), el.truediv(er, axis=axis))
            assert eq(l.floordiv(r, axis=axis), el.floordiv(er, axis=axis))
            assert eq(l.mod(r, axis=axis), el.mod(er, axis=axis))
            assert eq(l.pow(r, axis=axis), el.pow(er, axis=axis))

            assert eq(l.radd(r, axis=axis), el.radd(er, axis=axis))
            assert eq(l.rsub(r, axis=axis), el.rsub(er, axis=axis))
            assert eq(l.rmul(r, axis=axis), el.rmul(er, axis=axis))
            assert eq(l.rdiv(r, axis=axis), el.rdiv(er, axis=axis))
            assert eq(l.rtruediv(r, axis=axis), el.rtruediv(er, axis=axis))
            assert eq(l.rfloordiv(r, axis=axis), el.rfloordiv(er, axis=axis))
            assert eq(l.rmod(r, axis=axis), el.rmod(er, axis=axis))
            assert eq(l.rpow(r, axis=axis), el.rpow(er, axis=axis))


def test_reductions():
    dsk = {('x', 0): pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]},
                                  index=[0, 1, 3]),
           ('x', 1): pd.DataFrame({'a': [4, 5, 6], 'b': [3, 2, 1]},
                                  index=[5, 6, 8]),
           ('x', 2): pd.DataFrame({'a': [7, 8, 9], 'b': [0, 0, 0]},
                                  index=[9, 9, 9])}
    ddf1 = dd.DataFrame(dsk, 'x', ['a', 'b'], [0, 4, 9, 9])
    pdf1 = ddf1.compute()

    nans1 = pd.Series([1] + [np.nan] * 4 + [2] + [np.nan] * 3)
    nands1 = dd.from_pandas(nans1, 2)
    nans2 = pd.Series([1] + [np.nan] * 8)
    nands2 = dd.from_pandas(nans2, 2)
    nans3 = pd.Series([np.nan] * 9)
    nands3 = dd.from_pandas(nans3, 2)

    bools = pd.Series([True, False, True, False, True], dtype=bool)
    boolds = dd.from_pandas(bools, 2)

    for dds, pds in [(ddf1.b, pdf1.b), (ddf1.a, pdf1.a),
                     (ddf1['a'], pdf1['a']), (ddf1['b'], pdf1['b']),
                     (nands1, nans1), (nands2, nans2), (nands3, nans3),
                     (boolds, bools)]:
        assert isinstance(dds, dd.Series)
        assert isinstance(pds, pd.Series)
        assert eq(dds.sum(), pds.sum())
        assert eq(dds.min(), pds.min())
        assert eq(dds.max(), pds.max())
        assert eq(dds.count(), pds.count())
        assert eq(dds.std(), pds.std())
        assert eq(dds.var(), pds.var())
        assert eq(dds.std(ddof=0), pds.std(ddof=0))
        assert eq(dds.var(ddof=0), pds.var(ddof=0))
        assert eq(dds.mean(), pds.mean())
        assert eq(dds.nunique(), pds.nunique())
        assert eq(dds.nbytes, pds.nbytes)

        assert eq(dds.sum(skipna=False), pds.sum(skipna=False))
        assert eq(dds.min(skipna=False), pds.min(skipna=False))
        assert eq(dds.max(skipna=False), pds.max(skipna=False))
        assert eq(dds.std(skipna=False), pds.std(skipna=False))
        assert eq(dds.var(skipna=False), pds.var(skipna=False))
        assert eq(dds.std(skipna=False, ddof=0), pds.std(skipna=False, ddof=0))
        assert eq(dds.var(skipna=False, ddof=0), pds.var(skipna=False, ddof=0))
        assert eq(dds.mean(skipna=False), pds.mean(skipna=False))

    assert_dask_graph(ddf1.b.sum(), 'series-sum')
    assert_dask_graph(ddf1.b.min(), 'series-min')
    assert_dask_graph(ddf1.b.max(), 'series-max')
    assert_dask_graph(ddf1.b.count(), 'series-count')
    assert_dask_graph(ddf1.b.std(), 'series-std(ddof=1)')
    assert_dask_graph(ddf1.b.var(), 'series-var(ddof=1)')
    assert_dask_graph(ddf1.b.std(ddof=0), 'series-std(ddof=0)')
    assert_dask_graph(ddf1.b.var(ddof=0), 'series-var(ddof=0)')
    assert_dask_graph(ddf1.b.mean(), 'series-mean')
    # nunique is performed using drop-duplicates
    assert_dask_graph(ddf1.b.nunique(), 'drop-duplicates')


def test_reduction_series_invalid_axis():
    dsk = {('x', 0): pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]},
                                  index=[0, 1, 3]),
           ('x', 1): pd.DataFrame({'a': [4, 5, 6], 'b': [3, 2, 1]},
                                  index=[5, 6, 8]),
           ('x', 2): pd.DataFrame({'a': [7, 8, 9], 'b': [0, 0, 0]},
                                  index=[9, 9, 9])}
    ddf1 = dd.DataFrame(dsk, 'x', ['a', 'b'], [0, 4, 9, 9])
    pdf1 = ddf1.compute()

    for axis in [1, 'columns']:
        for s in [ddf1.a, pdf1.a]: # both must behave the same
            assert raises(ValueError, lambda: s.sum(axis=axis))
            assert raises(ValueError, lambda: s.min(axis=axis))
            assert raises(ValueError, lambda: s.max(axis=axis))
            # only count doesn't have axis keyword
            assert raises(TypeError, lambda: s.count(axis=axis))
            assert raises(ValueError, lambda: s.std(axis=axis))
            assert raises(ValueError, lambda: s.var(axis=axis))
            assert raises(ValueError, lambda: s.mean(axis=axis))


def test_reductions_non_numeric_dtypes():
    # test non-numric blocks

    def check_raises(d, p, func):
        assert raises((TypeError, ValueError),
                      lambda: getattr(d, func)().compute())
        assert raises((TypeError, ValueError),
                      lambda: getattr(p, func)())

    pds = pd.Series(['a', 'b', 'c', 'd', 'e'])
    dds = dd.from_pandas(pds, 2)
    assert eq(dds.sum(), pds.sum())
    assert eq(dds.min(), pds.min())
    assert eq(dds.max(), pds.max())
    assert eq(dds.count(), pds.count())
    check_raises(dds, pds, 'std')
    check_raises(dds, pds, 'var')
    check_raises(dds, pds, 'mean')
    assert eq(dds.nunique(), pds.nunique())

    for pds in [pd.Series(pd.Categorical([1, 2, 3, 4, 5], ordered=True)),
                pd.Series(pd.Categorical(list('abcde'), ordered=True)),
                pd.Series(pd.date_range('2011-01-01', freq='D', periods=5))]:
        dds = dd.from_pandas(pds, 2)

        check_raises(dds, pds, 'sum')
        assert eq(dds.min(), pds.min())
        assert eq(dds.max(), pds.max())
        assert eq(dds.count(), pds.count())
        check_raises(dds, pds, 'std')
        check_raises(dds, pds, 'var')
        check_raises(dds, pds, 'mean')
        assert eq(dds.nunique(), pds.nunique())

    pds = pd.Series(pd.timedelta_range('1 days', freq='D', periods=5))
    dds = dd.from_pandas(pds, 2)
    assert eq(dds.sum(), pds.sum())
    assert eq(dds.min(), pds.min())
    assert eq(dds.max(), pds.max())
    assert eq(dds.count(), pds.count())

    # ToDo: pandas supports timedelta std, otherwise dask raises:
    # incompatible type for a datetime/timedelta operation [__pow__]
    # assert eq(dds.std(), pds.std())
    # assert eq(dds.var(), pds.var())

    # ToDo: pandas supports timedelta std, otherwise dask raises:
    # TypeError: unsupported operand type(s) for *: 'float' and 'Timedelta'
    # assert eq(dds.mean(), pds.mean())

    assert eq(dds.nunique(), pds.nunique())


def test_reductions_frame():
    dsk = {('x', 0): pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]},
                                  index=[0, 1, 3]),
           ('x', 1): pd.DataFrame({'a': [4, 5, 6], 'b': [3, 2, 1]},
                                  index=[5, 6, 8]),
           ('x', 2): pd.DataFrame({'a': [7, 8, 9], 'b': [0, 0, 0]},
                                  index=[9, 9, 9])}
    ddf1 = dd.DataFrame(dsk, 'x', ['a', 'b'], [0, 4, 9, 9])
    pdf1 = ddf1.compute()

    assert eq(ddf1.sum(), pdf1.sum())
    assert eq(ddf1.min(), pdf1.min())
    assert eq(ddf1.max(), pdf1.max())
    assert eq(ddf1.count(), pdf1.count())
    assert eq(ddf1.std(), pdf1.std())
    assert eq(ddf1.var(), pdf1.var())
    assert eq(ddf1.std(ddof=0), pdf1.std(ddof=0))
    assert eq(ddf1.var(ddof=0), pdf1.var(ddof=0))
    assert eq(ddf1.mean(), pdf1.mean())

    for axis in [0, 1, 'index', 'columns']:
        assert eq(ddf1.sum(axis=axis), pdf1.sum(axis=axis))
        assert eq(ddf1.min(axis=axis), pdf1.min(axis=axis))
        assert eq(ddf1.max(axis=axis), pdf1.max(axis=axis))
        assert eq(ddf1.count(axis=axis), pdf1.count(axis=axis))
        assert eq(ddf1.std(axis=axis), pdf1.std(axis=axis))
        assert eq(ddf1.var(axis=axis), pdf1.var(axis=axis))
        assert eq(ddf1.std(axis=axis, ddof=0), pdf1.std(axis=axis, ddof=0))
        assert eq(ddf1.var(axis=axis, ddof=0), pdf1.var(axis=axis, ddof=0))
        assert eq(ddf1.mean(axis=axis), pdf1.mean(axis=axis))

    assert raises(ValueError, lambda: ddf1.sum(axis='incorrect').compute())

    # axis=0
    assert_dask_graph(ddf1.sum(), 'dataframe-sum')
    assert_dask_graph(ddf1.min(), 'dataframe-min')
    assert_dask_graph(ddf1.max(), 'dataframe-max')
    assert_dask_graph(ddf1.count(), 'dataframe-count')
    # std, var, mean consists from sum and count operations
    assert_dask_graph(ddf1.std(), 'dataframe-sum')
    assert_dask_graph(ddf1.std(), 'dataframe-count')
    assert_dask_graph(ddf1.var(), 'dataframe-sum')
    assert_dask_graph(ddf1.var(), 'dataframe-count')
    assert_dask_graph(ddf1.mean(), 'dataframe-sum')
    assert_dask_graph(ddf1.mean(), 'dataframe-count')

    # axis=1
    assert_dask_graph(ddf1.sum(axis=1), 'dataframe-sum(axis=1)')
    assert_dask_graph(ddf1.min(axis=1), 'dataframe-min(axis=1)')
    assert_dask_graph(ddf1.max(axis=1), 'dataframe-max(axis=1)')
    assert_dask_graph(ddf1.count(axis=1), 'dataframe-count(axis=1)')
    assert_dask_graph(ddf1.std(axis=1), 'dataframe-std(axis=1, ddof=1)')
    assert_dask_graph(ddf1.var(axis=1), 'dataframe-var(axis=1, ddof=1)')
    assert_dask_graph(ddf1.mean(axis=1), 'dataframe-mean(axis=1)')


def test_reductions_frame_dtypes():
    df = pd.DataFrame({'int': [1, 2, 3, 4, 5, 6, 7, 8],
                       'float': [1., 2., 3., 4., np.nan, 6., 7., 8.],
                       'dt': [pd.NaT] + [datetime(2011, i, 1) for i in range(1, 8)],
                       'str': list('abcdefgh')})
    ddf = dd.from_pandas(df, 3)
    assert eq(df.sum(), ddf.sum())
    assert eq(df.min(), ddf.min())
    assert eq(df.max(), ddf.max())
    assert eq(df.count(), ddf.count())
    assert eq(df.std(), ddf.std())
    assert eq(df.var(), ddf.var())
    assert eq(df.std(ddof=0), ddf.std(ddof=0))
    assert eq(df.var(ddof=0), ddf.var(ddof=0))
    assert eq(df.mean(), ddf.mean())

    assert eq(df._get_numeric_data(), ddf._get_numeric_data())

    numerics = ddf[['int', 'float']]
    assert numerics._get_numeric_data().dask == numerics.dask


def test_get_numeric_data_unknown_part():
    df = pd.DataFrame({'a': range(5), 'b': range(5), 'c': list('abcde')})
    ddf = dd.from_pandas(df, 3)
    # Drop dtype information
    ddf = dd.DataFrame(ddf.dask, ddf._name, ['a', 'b', 'c'], ddf.divisions)
    assert eq(ddf._get_numeric_data(), df._get_numeric_data())


def test_reductions_frame_nan():
    df = pd.DataFrame({'a': [1, 2, np.nan, 4, 5, 6, 7, 8],
                       'b': [1, 2, np.nan, np.nan, np.nan, 5, np.nan, np.nan],
                       'c': [np.nan] * 8})
    ddf = dd.from_pandas(df, 3)
    assert eq(df.sum(), ddf.sum())
    assert eq(df.min(), ddf.min())
    assert eq(df.max(), ddf.max())
    assert eq(df.count(), ddf.count())
    assert eq(df.std(), ddf.std())
    assert eq(df.var(), ddf.var())
    assert eq(df.std(ddof=0), ddf.std(ddof=0))
    assert eq(df.var(ddof=0), ddf.var(ddof=0))
    assert eq(df.mean(), ddf.mean())

    assert eq(df.sum(skipna=False), ddf.sum(skipna=False))
    assert eq(df.min(skipna=False), ddf.min(skipna=False))
    assert eq(df.max(skipna=False), ddf.max(skipna=False))
    assert eq(df.std(skipna=False), ddf.std(skipna=False))
    assert eq(df.var(skipna=False), ddf.var(skipna=False))
    assert eq(df.std(skipna=False, ddof=0), ddf.std(skipna=False, ddof=0))
    assert eq(df.var(skipna=False, ddof=0), ddf.var(skipna=False, ddof=0))
    assert eq(df.mean(skipna=False), ddf.mean(skipna=False))

    assert eq(df.sum(axis=1, skipna=False), ddf.sum(axis=1, skipna=False))
    assert eq(df.min(axis=1, skipna=False), ddf.min(axis=1, skipna=False))
    assert eq(df.max(axis=1, skipna=False), ddf.max(axis=1, skipna=False))
    assert eq(df.std(axis=1, skipna=False), ddf.std(axis=1, skipna=False))
    assert eq(df.var(axis=1, skipna=False), ddf.var(axis=1, skipna=False))
    assert eq(df.std(axis=1, skipna=False, ddof=0),
              ddf.std(axis=1, skipna=False, ddof=0))
    assert eq(df.var(axis=1, skipna=False, ddof=0),
              ddf.var(axis=1, skipna=False, ddof=0))
    assert eq(df.mean(axis=1, skipna=False), ddf.mean(axis=1, skipna=False))

    assert eq(df.cumsum(), ddf.cumsum())
    assert eq(df.cummin(), ddf.cummin())
    assert eq(df.cummax(), ddf.cummax())
    assert eq(df.cumprod(), ddf.cumprod())

    assert eq(df.cumsum(skipna=False), ddf.cumsum(skipna=False))
    assert eq(df.cummin(skipna=False), ddf.cummin(skipna=False))
    assert eq(df.cummax(skipna=False), ddf.cummax(skipna=False))
    assert eq(df.cumprod(skipna=False), ddf.cumprod(skipna=False))

    assert eq(df.cumsum(axis=1), ddf.cumsum(axis=1))
    assert eq(df.cummin(axis=1), ddf.cummin(axis=1))
    assert eq(df.cummax(axis=1), ddf.cummax(axis=1))
    assert eq(df.cumprod(axis=1), ddf.cumprod(axis=1))

    assert eq(df.cumsum(axis=1, skipna=False), ddf.cumsum(axis=1, skipna=False))
    assert eq(df.cummin(axis=1, skipna=False), ddf.cummin(axis=1, skipna=False))
    assert eq(df.cummax(axis=1, skipna=False), ddf.cummax(axis=1, skipna=False))
    assert eq(df.cumprod(axis=1, skipna=False), ddf.cumprod(axis=1, skipna=False))
