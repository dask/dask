import dask.dataframe as dd
import pandas.util.testing as tm
import pandas as pd
from dask.dataframe.shuffle import shuffle
import partd
from dask.async import get_sync

dsk = {('x', 0): pd.DataFrame({'a': [1, 2, 3], 'b': [1, 4, 7]},
                              index=[0, 1, 3]),
       ('x', 1): pd.DataFrame({'a': [4, 5, 6], 'b': [2, 5, 8]},
                              index=[5, 6, 8]),
       ('x', 2): pd.DataFrame({'a': [7, 8, 9], 'b': [3, 6, 9]},
                              index=[9, 9, 9])}
d = dd.DataFrame(dsk, 'x', ['a', 'b'], [0, 4, 9, 9])
full = d.compute()


def test_shuffle():
    s = shuffle(d, d.b, npartitions=2)
    assert isinstance(s, dd.DataFrame)
    assert s.npartitions == 2

    x = get_sync(s.dask, (s._name, 0))
    y = get_sync(s.dask, (s._name, 1))

    assert not (set(x.b) & set(y.b))  # disjoint

    assert shuffle(d, d.b, npartitions=2)._name == shuffle(d, d.b, npartitions=2)._name


def test_default_partitions():
    assert shuffle(d, d.b).npartitions == d.npartitions


def test_index_with_non_series():
    tm.assert_frame_equal(shuffle(d, d.b).compute(),
                          shuffle(d, 'b').compute())

def test_index_with_dataframe():
    assert sorted(shuffle(d, d[['b']]).compute().values.tolist()) ==\
           sorted(shuffle(d, ['b']).compute().values.tolist()) ==\
           sorted(shuffle(d, 'b').compute().values.tolist())


def test_shuffle_from_one_partition_to_one_other():
    df = pd.DataFrame({'x': [1, 2, 3]})
    a = dd.from_pandas(df, 1)

    for i in [1, 2]:
        b = shuffle(a, 'x', i)
        assert len(a.compute(get=get_sync)) == len(b.compute(get=get_sync))
