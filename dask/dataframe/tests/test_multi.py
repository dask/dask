import dask.dataframe as dd
import numpy as np
import pandas as pd
from dask.dataframe.multi import (align_partitions, join_indexed_dataframes,
        hash_join, concat_indexed_dataframes)
import pandas.util.testing as tm
from dask.async import get_sync

import pytest
from test_dataframe import eq


def test_align_partitions():
    A = pd.DataFrame({'x': [1, 2, 3, 4, 5, 6], 'y': list('abdabd')},
                     index=[10, 20, 30, 40, 50, 60])
    a = dd.repartition(A, [10, 40, 60])

    B = pd.DataFrame({'x': [1, 2, 3, 4], 'y': list('abda')},
                     index=[30, 70, 80, 100])
    b = dd.repartition(B, [30, 80, 100])

    s = dd.core.Scalar({('s', 0): 10}, 's')

    (aa, bb), divisions, L = align_partitions(a, b)

    def _check(a, b, aa, bb):
        assert isinstance(a, dd.DataFrame)
        assert isinstance(b, dd.DataFrame)
        assert isinstance(aa, dd.DataFrame)
        assert isinstance(bb, dd.DataFrame)
        assert eq(a, aa)
        assert eq(b, bb)
        assert divisions == (10, 30, 40, 60, 80, 100)
        assert isinstance(L, list)
        assert len(divisions) == 1 + len(L)

    _check(a, b, aa, bb)
    assert L == [[(aa._name, 0), (bb._name, 0)],
                 [(aa._name, 1), (bb._name, 1)],
                 [(aa._name, 2), (bb._name, 2)],
                 [(aa._name, 3), (bb._name, 3)],
                 [(aa._name, 4), (bb._name, 4)]]

    (aa, ss, bb), divisions, L = align_partitions(a, s, b)
    _check(a, b, aa, bb)
    assert L == [[(aa._name, 0), None, (bb._name, 0)],
                 [(aa._name, 1), None, (bb._name, 1)],
                 [(aa._name, 2), None, (bb._name, 2)],
                 [(aa._name, 3), None, (bb._name, 3)],
                 [(aa._name, 4), None, (bb._name, 4)]]
    assert eq(ss, 10)

    ldf = pd.DataFrame({'a': [1, 2, 3, 4, 5, 6, 7],
                        'b': [7, 6, 5, 4, 3, 2, 1]})
    rdf = pd.DataFrame({'c': [1, 2, 3, 4, 5, 6, 7],
                        'd': [7, 6, 5, 4, 3, 2, 1]})

    for lhs, rhs in [(dd.from_pandas(ldf, 1), dd.from_pandas(rdf, 1)),
                     (dd.from_pandas(ldf, 2), dd.from_pandas(rdf, 2)),
                     (dd.from_pandas(ldf, 2), dd.from_pandas(rdf, 3)),
                     (dd.from_pandas(ldf, 3), dd.from_pandas(rdf, 2))]:
        (lresult, rresult), div, parts = dd.multi.align_partitions(lhs, rhs)
        assert eq(lresult, ldf)
        assert eq(rresult, rdf)

    # different index
    ldf = pd.DataFrame({'a': [1, 2, 3, 4, 5, 6, 7],
                        'b': [7, 6, 5, 4, 3, 2, 1]},
                       index=list('abcdefg'))
    rdf = pd.DataFrame({'c': [1, 2, 3, 4, 5, 6, 7],
                        'd': [7, 6, 5, 4, 3, 2, 1]},
                       index=list('fghijkl'))

    for lhs, rhs in [(dd.from_pandas(ldf, 1), dd.from_pandas(rdf, 1)),
                     (dd.from_pandas(ldf, 2), dd.from_pandas(rdf, 2)),
                     (dd.from_pandas(ldf, 2), dd.from_pandas(rdf, 3)),
                     (dd.from_pandas(ldf, 3), dd.from_pandas(rdf, 2))]:
        (lresult, rresult), div, parts = dd.multi.align_partitions(lhs, rhs)
        assert eq(lresult, ldf)
        assert eq(rresult, rdf)


def test_join_indexed_dataframe_to_indexed_dataframe():
    A = pd.DataFrame({'x': [1, 2, 3, 4, 5, 6]},
                     index=[1, 2, 3, 4, 6, 7])
    a = dd.repartition(A, [1, 4, 7])

    B = pd.DataFrame({'y': list('abcdef')},
                     index=[1, 2, 4, 5, 6, 8])
    b = dd.repartition(B, [1, 2, 5, 8])

    c = join_indexed_dataframes(a, b, how='left')
    assert c.divisions[0] == a.divisions[0]
    assert c.divisions[-1] == max(a.divisions + b.divisions)
    assert eq(c, A.join(B))

    c = join_indexed_dataframes(a, b, how='right')
    assert c.divisions[0] == b.divisions[0]
    assert c.divisions[-1] == b.divisions[-1]
    assert eq(c, A.join(B, how='right'))

    c = join_indexed_dataframes(a, b, how='inner')
    assert c.divisions[0] == 1
    assert c.divisions[-1] == max(a.divisions + b.divisions)
    assert eq(c.compute(), A.join(B, how='inner'))

    c = join_indexed_dataframes(a, b, how='outer')
    assert c.divisions[0] == 1
    assert c.divisions[-1] == 8
    assert eq(c.compute(), A.join(B, how='outer'))

    assert sorted(join_indexed_dataframes(a, b, how='inner').dask) == \
           sorted(join_indexed_dataframes(a, b, how='inner').dask)
    assert sorted(join_indexed_dataframes(a, b, how='inner').dask) != \
           sorted(join_indexed_dataframes(a, b, how='outer').dask)


def list_eq(a, b):
    if isinstance(a, dd.DataFrame):
        a = a.compute(get=get_sync)
    if isinstance(b, dd.DataFrame):
        b = b.compute(get=get_sync)
    tm.assert_index_equal(a.columns, b.columns)

    # ToDo: As of pandas 0,17, tm.assert_numpy_array_equal can
    # compare arrays include NaN. This logic can be replaced
    for c in a.columns:
        if a[c].dtype in (int, float):
            a[c] = a[c].fillna(100)
            b[c] = b[c].fillna(100)
        else:
            a[c] = a[c].fillna('NAN')
            b[c] = b[c].fillna('NAN')
    av = sorted(a.values.tolist())
    bv = sorted(b.values.tolist())
    assert av == bv, (av, bv)


@pytest.mark.parametrize('how', ['inner', 'left', 'right', 'outer'])
def test_hash_join(how):
    A = pd.DataFrame({'x': [1, 2, 3, 4, 5, 6], 'y': [1, 1, 2, 2, 3, 4]})
    a = dd.repartition(A, [0, 4, 5])

    B = pd.DataFrame({'y': [1, 3, 4, 4, 5, 6], 'z': [6, 5, 4, 3, 2, 1]})
    b = dd.repartition(B, [0, 2, 5])

    c = hash_join(a, 'y', b, 'y', how)

    result = c.compute()
    expected = pd.merge(A, B, how, 'y')
    list_eq(result, expected)

    # Different columns and npartitions
    c = hash_join(a, 'x', b, 'z', 'outer', npartitions=3)
    assert c.npartitions == 3

    result = c.compute()
    expected = pd.merge(A, B, 'outer', None, 'x', 'z')

    list_eq(result, expected)

    assert hash_join(a, 'y', b, 'y', 'inner')._name == \
           hash_join(a, 'y', b, 'y', 'inner')._name
    assert hash_join(a, 'y', b, 'y', 'inner')._name != \
           hash_join(a, 'y', b, 'y', 'outer')._name


@pytest.mark.parametrize('join', ['inner', 'outer'])
def test_indexed_concat(join):
    A = pd.DataFrame({'x': [1, 2, 3, 4, 6, 7], 'y': list('abcdef')},
                     index=[1, 2, 3, 4, 6, 7])
    a = dd.repartition(A, [1, 4, 7])

    B = pd.DataFrame({'x': [10, 20, 40, 50, 60, 80]},
                     index=[1, 2, 4, 5, 6, 8])
    b = dd.repartition(B, [1, 2, 5, 8])

    result = concat_indexed_dataframes([a, b], join=join)
    expected = pd.concat([A, B], axis=0, join=join)
    assert eq(result, expected)

    assert sorted(concat_indexed_dataframes([a, b], join=join).dask) == \
           sorted(concat_indexed_dataframes([a, b], join=join).dask)
    assert sorted(concat_indexed_dataframes([a, b], join='inner').dask) != \
           sorted(concat_indexed_dataframes([a, b], join='outer').dask)


@pytest.mark.parametrize('join', ['inner', 'outer'])
def test_concat(join):
    pdf1 = pd.DataFrame({'x': [1, 2, 3, 4, 6, 7],
                         'y': list('abcdef')},
                        index=[1, 2, 3, 4, 6, 7])
    ddf1 = dd.from_pandas(pdf1, 2)
    pdf2 = pd.DataFrame({'x': [1, 2, 3, 4, 6, 7],
                         'y': list('abcdef')},
                        index=[8, 9, 10, 11, 12, 13])
    ddf2 = dd.from_pandas(pdf2, 2)

    # different columns
    pdf3 = pd.DataFrame({'x': [1, 2, 3, 4, 6, 7],
                         'z': list('abcdef')},
                        index=[8, 9, 10, 11, 12, 13])
    ddf3 = dd.from_pandas(pdf3, 2)

    for (dd1, dd2, pd1, pd2) in [(ddf1, ddf2, pdf1, pdf2),
                                 (ddf1, ddf3, pdf1, pdf3)]:
        result = dd.concat([dd1, dd2], join=join)
        expected = pd.concat([pd1, pd2], join=join)
        assert eq(result, expected)

    # test outer only, inner has a problem on pandas side
    for (dd1, dd2, pd1, pd2) in [(ddf1, ddf2, pdf1, pdf2),
                                 (ddf1, ddf3, pdf1, pdf3),
                                 (ddf1.x, ddf2.x, pdf1.x, pdf2.x),
                                 (ddf1.x, ddf3.z, pdf1.x, pdf3.z),
                                 (ddf1.x, ddf2.x, pdf1.x, pdf2.x),
                                 (ddf1.x, ddf3.z, pdf1.x, pdf3.z)]:
        result = dd.concat([dd1, dd2])
        expected = pd.concat([pd1, pd2])
        assert eq(result, expected)


@pytest.mark.parametrize('how', ['inner', 'outer', 'left', 'right'])
def test_merge(how):
    A = pd.DataFrame({'x': [1, 2, 3, 4, 5, 6], 'y': [1, 1, 2, 2, 3, 4]})
    a = dd.repartition(A, [0, 4, 5])

    B = pd.DataFrame({'y': [1, 3, 4, 4, 5, 6], 'z': [6, 5, 4, 3, 2, 1]})
    b = dd.repartition(B, [0, 2, 5])

    eq(dd.merge(a, b, left_index=True, right_index=True),
       pd.merge(A, B, left_index=True, right_index=True))

    result = dd.merge(a, b, on='y', how=how)
    list_eq(result, pd.merge(A, B, on='y', how=how))
    assert all(d is None for d in result.divisions)

    list_eq(dd.merge(a, b, left_on='x', right_on='z', how=how),
            pd.merge(A, B, left_on='x', right_on='z', how=how))
    list_eq(dd.merge(a, b, left_on='x', right_on='z', how=how,
                     suffixes=('1', '2')),
            pd.merge(A, B, left_on='x', right_on='z', how=how,
                     suffixes=('1', '2')))

    list_eq(dd.merge(a, b, how=how), pd.merge(A, B, how=how))
    list_eq(dd.merge(a, B, how=how), pd.merge(A, B, how=how))
    list_eq(dd.merge(A, b, how=how), pd.merge(A, B, how=how))
    list_eq(dd.merge(A, B, how=how), pd.merge(A, B, how=how))

    list_eq(dd.merge(a, b, left_index=True, right_index=True, how=how),
            pd.merge(A, B, left_index=True, right_index=True, how=how))
    list_eq(dd.merge(a, b, left_index=True, right_index=True, how=how,
                     suffixes=('1', '2')),
            pd.merge(A, B, left_index=True, right_index=True, how=how,
                     suffixes=('1', '2')))

    list_eq(dd.merge(a, b, left_on='x', right_index=True, how=how),
            pd.merge(A, B, left_on='x', right_index=True, how=how))
    list_eq(dd.merge(a, b, left_on='x', right_index=True, how=how,
                     suffixes=('1', '2')),
            pd.merge(A, B, left_on='x', right_index=True, how=how,
                     suffixes=('1', '2')))

    # pandas result looks buggy
    # list_eq(dd.merge(a, B, left_index=True, right_on='y'),
    #         pd.merge(A, B, left_index=True, right_on='y'))



@pytest.mark.parametrize('how', ['inner', 'outer', 'left', 'right'])
def test_merge_by_index_patterns(how):

    pdf1l = pd.DataFrame({'a': [1, 2, 3, 4, 5, 6, 7],
                          'b': [7, 6, 5, 4, 3, 2, 1]})
    pdf1r = pd.DataFrame({'c': [1, 2, 3, 4, 5, 6, 7],
                          'd': [7, 6, 5, 4, 3, 2, 1]})

    pdf2l = pd.DataFrame({'a': [1, 2, 3, 4, 5, 6, 7],
                          'b': [7, 6, 5, 4, 3, 2, 1]},
                          index=list('abcdefg'))
    pdf2r = pd.DataFrame({'c': [7, 6, 5, 4, 3, 2, 1],
                          'd': [7, 6, 5, 4, 3, 2, 1]},
                          index=list('abcdefg'))

    pdf3l = pdf2l
    pdf3r = pd.DataFrame({'c': [6, 7, 8, 9],
                          'd': [5, 4, 3, 2]},
                          index=list('abdg'))

    pdf4l = pdf2l
    pdf4r = pd.DataFrame({'c': [9, 10, 11, 12],
                          'd': [5, 4, 3, 2]},
                          index=list('abdg'))

    # completely different index
    pdf5l = pd.DataFrame({'a': [1, 1, 2, 2, 3, 3, 4],
                          'b': [7, 6, 5, 4, 3, 2, 1]},
                          index=list('lmnopqr'))
    pdf5r = pd.DataFrame({'c': [1, 1, 1, 1],
                          'd': [5, 4, 3, 2]},
                          index=list('abcd'))

    pdf6l = pd.DataFrame({'a': [1, 1, 2, 2, 3, 3, 4],
                          'b': [7, 6, 5, 4, 3, 2, 1]},
                          index=list('cdefghi'))
    pdf6r = pd.DataFrame({'c': [1, 2, 1, 2],
                          'd': [5, 4, 3, 2]},
                          index=list('abcd'))

    pdf7l = pd.DataFrame({'a': [1, 1, 2, 2, 3, 3, 4],
                          'b': [7, 6, 5, 4, 3, 2, 1]},
                          index=list('abcdefg'))
    pdf7r = pd.DataFrame({'c': [5, 6, 7, 8],
                          'd': [5, 4, 3, 2]},
                          index=list('fghi'))

    for pdl, pdr in [(pdf1l, pdf1r), (pdf2l, pdf2r), (pdf3l, pdf3r),
                     (pdf4l, pdf4r), (pdf5l, pdf5r), (pdf6l, pdf6r),
                     (pdf7l, pdf7r)]:

        for lpart, rpart in [(2, 2),  # same partition
                             (3, 2),  # left npartition > right npartition
                             (2, 3)]: # left npartition < right npartition

            ddl = dd.from_pandas(pdl, lpart)
            ddr = dd.from_pandas(pdr, rpart)

            eq(dd.merge(ddl, ddr, how=how, left_index=True, right_index=True),
               pd.merge(pdl, pdr, how=how, left_index=True, right_index=True))
            eq(dd.merge(ddr, ddl, how=how, left_index=True, right_index=True),
               pd.merge(pdr, pdl, how=how, left_index=True, right_index=True))

            eq(ddr.merge(ddl, how=how, left_index=True, right_index=True),
               pdr.merge(pdl, how=how, left_index=True, right_index=True))
            eq(ddl.merge(ddr, how=how, left_index=True, right_index=True),
               pdl.merge(pdr, how=how, left_index=True, right_index=True))

            # hash join
            list_eq(dd.merge(ddl, ddr, how=how, left_on='a', right_on='c'),
                    pd.merge(pdl, pdr, how=how, left_on='a', right_on='c'))
            list_eq(dd.merge(ddl, ddr, how=how, left_on='b', right_on='d'),
                    pd.merge(pdl, pdr, how=how, left_on='b', right_on='d'))

            list_eq(dd.merge(ddr, ddl, how=how, left_on='c', right_on='a'),
                    pd.merge(pdr, pdl, how=how, left_on='c', right_on='a'))
            list_eq(dd.merge(ddr, ddl, how=how, left_on='d', right_on='b'),
                    pd.merge(pdr, pdl, how=how, left_on='d', right_on='b'))

            list_eq(ddl.merge(ddr, how=how, left_on='a', right_on='c'),
                    pdl.merge(pdr, how=how, left_on='a', right_on='c'))
            list_eq(ddl.merge(ddr, how=how, left_on='b', right_on='d'),
                    pdl.merge(pdr, how=how, left_on='b', right_on='d'))

            list_eq(ddr.merge(ddl, how=how, left_on='c', right_on='a'),
                    pdr.merge(pdl, how=how, left_on='c', right_on='a'))
            list_eq(ddr.merge(ddl, how=how, left_on='d', right_on='b'),
                    pdr.merge(pdl, how=how, left_on='d', right_on='b'))


@pytest.mark.parametrize('how', ['inner', 'outer', 'left', 'right'])
def test_join_by_index_patterns(how):

    # Similar test cases as test_merge_by_index_patterns,
    # but columns / index for join have same dtype

    pdf1l = pd.DataFrame({'a': list('abcdefg'),
                          'b': [7, 6, 5, 4, 3, 2, 1]},
                         index=list('abcdefg'))
    pdf1r = pd.DataFrame({'c': list('abcdefg'),
                          'd': [7, 6, 5, 4, 3, 2, 1]},
                         index=list('abcdefg'))

    pdf2l = pdf1l
    pdf2r = pd.DataFrame({'c': list('gfedcba'),
                          'd': [7, 6, 5, 4, 3, 2, 1]},
                          index=list('abcdefg'))

    pdf3l = pdf1l
    pdf3r = pd.DataFrame({'c': list('abdg'),
                          'd': [5, 4, 3, 2]},
                          index=list('abdg'))

    pdf4l = pd.DataFrame({'a': list('abcabce'),
                          'b': [7, 6, 5, 4, 3, 2, 1]},
                          index=list('abcdefg'))
    pdf4r = pd.DataFrame({'c': list('abda'),
                          'd': [5, 4, 3, 2]},
                          index=list('abdg'))

    # completely different index
    pdf5l = pd.DataFrame({'a': list('lmnopqr'),
                          'b': [7, 6, 5, 4, 3, 2, 1]},
                          index=list('lmnopqr'))
    pdf5r = pd.DataFrame({'c': list('abcd'),
                          'd': [5, 4, 3, 2]},
                          index=list('abcd'))

    pdf6l = pd.DataFrame({'a': list('cdefghi'),
                          'b': [7, 6, 5, 4, 3, 2, 1]},
                          index=list('cdefghi'))
    pdf6r = pd.DataFrame({'c': list('abab'),
                          'd': [5, 4, 3, 2]},
                          index=list('abcd'))

    pdf7l = pd.DataFrame({'a': list('aabbccd'),
                          'b': [7, 6, 5, 4, 3, 2, 1]},
                          index=list('abcdefg'))
    pdf7r = pd.DataFrame({'c': list('aabb'),
                          'd': [5, 4, 3, 2]},
                          index=list('fghi'))

    for pdl, pdr in [(pdf1l, pdf1r), (pdf2l, pdf2r), (pdf3l, pdf3r),
                     (pdf4l, pdf4r), (pdf5l, pdf5r), (pdf6l, pdf6r),
                     (pdf7l, pdf7r)]:

        for lpart, rpart in [(2, 2), (3, 2), (2, 3)]:

            ddl = dd.from_pandas(pdl, lpart)
            ddr = dd.from_pandas(pdr, rpart)

            eq(ddl.join(ddr, how=how), pdl.join(pdr, how=how))
            eq(ddr.join(ddl, how=how), pdr.join(pdl, how=how))

            eq(ddl.join(ddr, how=how, lsuffix='l', rsuffix='r'),
               pdl.join(pdr, how=how, lsuffix='l', rsuffix='r'))
            eq(ddr.join(ddl, how=how, lsuffix='l', rsuffix='r'),
               pdr.join(pdl, how=how, lsuffix='l', rsuffix='r'))

            """
            # temporary disabled bacause pandas may incorrectly raise
            # IndexError for empty DataFrame
            # https://github.com/pydata/pandas/pull/10826

            list_eq(ddl.join(ddr, how=how, on='a', lsuffix='l', rsuffix='r'),
                    pdl.join(pdr, how=how, on='a', lsuffix='l', rsuffix='r'))

            list_eq(ddr.join(ddl, how=how, on='c', lsuffix='l', rsuffix='r'),
                    pdr.join(pdl, how=how, on='c', lsuffix='l', rsuffix='r'))

            # merge with index and columns
            list_eq(ddl.merge(ddr, how=how, left_on='a', right_index=True),
                    pdl.merge(pdr, how=how, left_on='a', right_index=True))
            list_eq(ddr.merge(ddl, how=how, left_on='c', right_index=True),
                    pdr.merge(pdl, how=how, left_on='c', right_index=True))
            list_eq(ddl.merge(ddr, how=how, left_index=True, right_on='c'),
                    pdl.merge(pdr, how=how, left_index=True, right_on='c'))
            list_eq(ddr.merge(ddl, how=how, left_index=True, right_on='a'),
                    pdr.merge(pdl, how=how, left_index=True, right_on='a'))
            """


@pytest.mark.parametrize('how', ['inner', 'outer', 'left', 'right'])
def test_merge_by_multiple_columns(how):

    pdf1l = pd.DataFrame({'a': list('abcdefghij'),
                          'b': list('abcdefghij'),
                          'c': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]},
                         index=list('abcdefghij'))
    pdf1r = pd.DataFrame({'d': list('abcdefghij'),
                          'e': list('abcdefghij'),
                          'f': [10, 9, 8, 7, 6, 5, 4, 3, 2, 1]},
                         index=list('abcdefghij'))

    pdf2l = pd.DataFrame({'a': list('abcdeabcde'),
                          'b': list('abcabcabca'),
                          'c': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]},
                         index=list('abcdefghij'))
    pdf2r = pd.DataFrame({'d': list('edcbaedcba'),
                          'e': list('aaabbbcccd'),
                          'f': [10, 9, 8, 7, 6, 5, 4, 3, 2, 1]},
                         index=list('fghijklmno'))

    pdf3l = pd.DataFrame({'a': list('aaaaaaaaaa'),
                          'b': list('aaaaaaaaaa'),
                          'c': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]},
                         index=list('abcdefghij'))
    pdf3r = pd.DataFrame({'d': list('aaabbbccaa'),
                          'e': list('abbbbbbbbb'),
                          'f': [10, 9, 8, 7, 6, 5, 4, 3, 2, 1]},
                         index=list('ABCDEFGHIJ'))

    for pdl, pdr in [(pdf1l, pdf1r), (pdf2l, pdf2r), (pdf3l, pdf3r)]:

        for lpart, rpart in [(2, 2), (3, 2), (2, 3)]:

            ddl = dd.from_pandas(pdl, lpart)
            ddr = dd.from_pandas(pdr, rpart)

            eq(ddl.join(ddr, how=how), pdl.join(pdr, how=how))
            eq(ddr.join(ddl, how=how), pdr.join(pdl, how=how))

            eq(dd.merge(ddl, ddr, how=how, left_index=True, right_index=True),
               pd.merge(pdl, pdr, how=how, left_index=True, right_index=True))
            eq(dd.merge(ddr, ddl, how=how, left_index=True, right_index=True),
               pd.merge(pdr, pdl, how=how, left_index=True, right_index=True))

            # hash join
            list_eq(dd.merge(ddl, ddr, how=how, left_on='a', right_on='d'),
                    pd.merge(pdl, pdr, how=how, left_on='a', right_on='d'))
            list_eq(dd.merge(ddl, ddr, how=how, left_on='b', right_on='e'),
                    pd.merge(pdl, pdr, how=how, left_on='b', right_on='e'))

            list_eq(dd.merge(ddr, ddl, how=how, left_on='d', right_on='a'),
                    pd.merge(pdr, pdl, how=how, left_on='d', right_on='a'))
            list_eq(dd.merge(ddr, ddl, how=how, left_on='e', right_on='b'),
                    pd.merge(pdr, pdl, how=how, left_on='e', right_on='b'))

            list_eq(dd.merge(ddl, ddr, how=how, left_on=['a', 'b'], right_on=['d', 'e']),
                    pd.merge(pdl, pdr, how=how, left_on=['a', 'b'], right_on=['d', 'e']))


def test_melt():
    pdf = pd.DataFrame({'A': list('abcd') * 5,
                        'B': list('XY') * 10,
                        'C': np.random.randn(20)})
    ddf = dd.from_pandas(pdf, 4)

    list_eq(dd.melt(ddf),
            pd.melt(pdf))

    list_eq(dd.melt(ddf, id_vars='C'),
            pd.melt(pdf, id_vars='C'))
    list_eq(dd.melt(ddf, value_vars='C'),
            pd.melt(pdf, value_vars='C'))
    list_eq(dd.melt(ddf, value_vars=['A', 'C'], var_name='myvar'),
            pd.melt(pdf, value_vars=['A', 'C'], var_name='myvar'))
    list_eq(dd.melt(ddf, id_vars='B', value_vars=['A', 'C'], value_name='myval'),
            pd.melt(pdf, id_vars='B', value_vars=['A', 'C'], value_name='myval'))
