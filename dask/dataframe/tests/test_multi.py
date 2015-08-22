import dask.dataframe as dd
import pandas as pd
from dask.dataframe.multi import (align_partitions, join_indexed_dataframes,
        hash_join, concat_indexed_dataframes)
import pandas.util.testing as tm
from dask.async import get_sync


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
    tm.assert_frame_equal(c.compute(), A.join(B))

    c = join_indexed_dataframes(a, b, how='right')
    assert c.divisions[0] == b.divisions[0]
    assert c.divisions[-1] == b.divisions[-1]
    tm.assert_frame_equal(c.compute(), A.join(B, how='right'))

    c = join_indexed_dataframes(a, b, how='inner')
    assert c.divisions[0] == 1
    assert c.divisions[-1] == max(a.divisions + b.divisions)
    tm.assert_frame_equal(c.compute(), A.join(B, how='inner'))

    c = join_indexed_dataframes(a, b, how='outer')
    assert c.divisions[0] == 1
    assert c.divisions[-1] == 8
    tm.assert_frame_equal(c.compute(), A.join(B, how='outer'))

    assert sorted(join_indexed_dataframes(a, b, how='inner').dask) == \
           sorted(join_indexed_dataframes(a, b, how='inner').dask)
    assert sorted(join_indexed_dataframes(a, b, how='inner').dask) != \
           sorted(join_indexed_dataframes(a, b, how='outer').dask)


def list_eq(a, b):
    if isinstance(a, dd.DataFrame):
        a = a.compute(get=get_sync)
    if isinstance(b, dd.DataFrame):
        b = b.compute(get=get_sync)
    assert list(a.columns) == list(b.columns)

    assert sorted(a.fillna(100).values.tolist()) == \
           sorted(b.fillna(100).values.tolist())


def test_hash_join():
    A = pd.DataFrame({'x': [1, 2, 3, 4, 5, 6], 'y': [1, 1, 2, 2, 3, 4]})
    a = dd.repartition(A, [0, 4, 5])

    B = pd.DataFrame({'y': [1, 3, 4, 4, 5, 6], 'z': [6, 5, 4, 3, 2, 1]})
    b = dd.repartition(B, [0, 2, 5])

    for how in ['inner', 'left', 'right', 'outer']:
        c = hash_join(a, 'y', b, 'y', how)

        result = c.compute()
        expected = pd.merge(A, B, how, 'y')

        assert list(result.columns) == list(expected.columns)
        assert sorted(result.fillna(100).values.tolist()) == \
               sorted(expected.fillna(100).values.tolist())


    # Different columns and npartitions
    c = hash_join(a, 'x', b, 'z', 'outer', npartitions=3)
    assert c.npartitions == 3

    result = c.compute()
    expected = pd.merge(A, B, 'outer', None, 'x', 'z')
    assert list(result.columns) == list(expected.columns)
    assert sorted(result.fillna(100).values.tolist()) == \
           sorted(expected.fillna(100).values.tolist())

    assert hash_join(a, 'y', b, 'y', 'inner')._name == \
           hash_join(a, 'y', b, 'y', 'inner')._name
    assert hash_join(a, 'y', b, 'y', 'inner')._name != \
           hash_join(a, 'y', b, 'y', 'outer')._name


def test_indexed_concat():
    A = pd.DataFrame({'x': [1, 2, 3, 4, 6, 7], 'y': list('abcdef')},
                     index=[1, 2, 3, 4, 6, 7])
    a = dd.repartition(A, [1, 4, 7])

    B = pd.DataFrame({'x': [10, 20, 40, 50, 60, 80]},
                     index=[1, 2, 4, 5, 6, 8])
    b = dd.repartition(B, [1, 2, 5, 8])

    for how in ['inner', 'outer']:
        c = concat_indexed_dataframes([a, b], join=how)

        result = c.compute()
        expected = pd.concat([A, B], 0, how)

        assert list(result.columns) == list(expected.columns)

        assert sorted(zip(result.values.tolist(), result.index.values.tolist())) == \
               sorted(zip(expected.values.tolist(), expected.index.values.tolist()))

    assert sorted(concat_indexed_dataframes([a, b], join='inner').dask) == \
           sorted(concat_indexed_dataframes([a, b], join='inner').dask)
    assert sorted(concat_indexed_dataframes([a, b], join='inner').dask) != \
           sorted(concat_indexed_dataframes([a, b], join='outer').dask)


def test_merge():
    A = pd.DataFrame({'x': [1, 2, 3, 4, 5, 6], 'y': [1, 1, 2, 2, 3, 4]})
    a = dd.repartition(A, [0, 4, 5])

    B = pd.DataFrame({'y': [1, 3, 4, 4, 5, 6], 'z': [6, 5, 4, 3, 2, 1]})
    b = dd.repartition(B, [0, 2, 5])

    list_eq(dd.merge(a, b, left_index=True, right_index=True),
            pd.merge(A, B, left_index=True, right_index=True))

    list_eq(dd.merge(a, b, on='y'),
            pd.merge(A, B, on='y'))

    list_eq(dd.merge(a, b, left_on='x', right_on='z'),
            pd.merge(A, B, left_on='x', right_on='z'))

    list_eq(dd.merge(a, b),
            pd.merge(A, B))

    list_eq(dd.merge(a, B),
            pd.merge(A, B))

    list_eq(dd.merge(A, b),
            pd.merge(A, B))

    list_eq(dd.merge(A, B),
            pd.merge(A, B))

    list_eq(dd.merge(a, b, left_index=True, right_index=True),
            pd.merge(A, B, left_index=True, right_index=True))

    # list_eq(dd.merge(a, b, left_on='x', right_index=True),
    #         pd.merge(A, B, left_on='x', right_index=True))

    # list_eq(dd.merge(a, B, left_index=True, right_on='y'),
    #         pd.merge(A, B, left_index=True, right_on='y'))


def test_merge_by_index_patterns():

    pdf1l = pd.DataFrame({'a': [1, 2, 3, 4, 5, 6, 7],
                          'b': [7, 6, 5, 4, 3, 2, 1]})
    pdf1r = pd.DataFrame({'c': [1, 2, 3, 4, 5, 6, 7],
                          'd': [7, 6, 5, 4, 3, 2, 1]})

    pdf2l = pd.DataFrame({'a': [1, 2, 3, 4, 5, 6, 7],
                          'b': [7, 6, 5, 4, 3, 2, 1]},
                          index=list('abcdefg'))
    pdf2r = pd.DataFrame({'c': [1, 2, 3, 4, 5, 6, 7],
                          'd': [7, 6, 5, 4, 3, 2, 1]},
                          index=list('abcdefg'))

    pdf3l = pd.DataFrame({'a': [1, 2, 3, 4, 5, 6, 7],
                          'b': [7, 6, 5, 4, 3, 2, 1]},
                          index=list('abcdefg'))
    pdf3r = pd.DataFrame({'c': [1, 2, 3, 4],
                          'd': [5, 4, 3, 2]},
                          index=list('abdg'))

    pdf4r = pd.DataFrame({'c': [1, 2, 3, 4],
                          'd': [5, 4, 3, 2]},
                          index=list('abdg'))
    pdf4l = pd.DataFrame({'a': [1, 2, 3, 4, 5, 6, 7],
                          'b': [7, 6, 5, 4, 3, 2, 1]},
                          index=list('abcdefg'))

    # completely different index
    pdf5r = pd.DataFrame({'c': [1, 2, 3, 4],
                          'd': [5, 4, 3, 2]},
                          index=list('abcd'))
    pdf5l = pd.DataFrame({'a': [1, 2, 3, 4, 5, 6, 7],
                          'b': [7, 6, 5, 4, 3, 2, 1]},
                          index=list('lmnopqr'))

    pdf6r = pd.DataFrame({'c': [1, 2, 3, 4],
                          'd': [5, 4, 3, 2]},
                          index=list('abcd'))
    pdf6l = pd.DataFrame({'a': [1, 2, 3, 4, 5, 6, 7],
                          'b': [7, 6, 5, 4, 3, 2, 1]},
                          index=list('cdefghi'))

    pdf7r = pd.DataFrame({'c': [1, 2, 3, 4],
                          'd': [5, 4, 3, 2]},
                          index=list('fghi'))
    pdf7l = pd.DataFrame({'a': [1, 2, 3, 4, 5, 6, 7],
                          'b': [7, 6, 5, 4, 3, 2, 1]},
                          index=list('abcdefg'))

    for pdl, pdr in [(pdf1l, pdf1r), (pdf2l, pdf2r), (pdf3l, pdf3r),
                     (pdf4l, pdf4r), (pdf5r, pdf5l), (pdf6r, pdf6l),
                     (pdf7r, pdf7l)]:
        # same partition
        ddl = dd.from_pandas(pdl, 2)
        ddr = dd.from_pandas(pdr, 2)

        for how in ['inner', 'outer', 'left', 'right']:
            eq(dd.merge(ddl, ddr, how=how, left_index=True, right_index=True),
               pd.merge(pdl, pdr, how=how, left_index=True, right_index=True))

        # different partition (left npartition > right npartition)
        ddl = dd.from_pandas(pdl, 3)
        ddr = dd.from_pandas(pdr, 2)

        for how in ['inner', 'outer', 'left', 'right']:
            eq(dd.merge(ddl, ddr, how=how, left_index=True, right_index=True),
               pd.merge(pdl, pdr, how=how, left_index=True, right_index=True))

        # different partition (left npartition < right npartition)
        ddl = dd.from_pandas(pdl, 2)
        ddr = dd.from_pandas(pdr, 3)

        for how in ['inner', 'outer', 'left', 'right']:
            eq(dd.merge(ddl, ddr, how=how, left_index=True, right_index=True),
               pd.merge(pdl, pdr, how=how, left_index=True, right_index=True))
