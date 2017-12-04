import dask.dataframe as dd
import numpy as np
import pandas as pd
import pytest

from dask.dataframe.utils import assert_eq, PANDAS_VERSION


# Fixtures
# ========
@pytest.fixture
def df_left():
    return pd.DataFrame(dict(
        idx=[1, 1, 1, 2, 2, 2, 2, 3, 3, 4, 4],
        k=[1, 2, 3, 1, 2, 3, 4, 1, 2, 1, 2],
        v1=np.linspace(0, 1, 11)
    )).set_index(['idx'])


@pytest.fixture
def df_right():
    return pd.DataFrame(dict(
        idx=[1, 1, 1, 1, 1, 1, 2, 2, 3, 3, 3, 3],
        k=[1, 2, 2, 3, 3, 4, 2, 3, 1, 1, 2, 3],
        v2=np.linspace(10, 11, 12)
    )).set_index(['idx'])


@pytest.fixture
def ddf_left(df_left):
    return dd.repartition(df_left, [1, 3, 4])


@pytest.fixture
def ddf_left_unknown(ddf_left):
    return ddf_left.clear_divisions()


@pytest.fixture
def ddf_left_single(df_left):
    return dd.from_pandas(df_left, npartitions=1, sort=False)


@pytest.fixture
def ddf_right(df_right):
    return dd.repartition(df_right, [1, 2, 4])


@pytest.fixture
def ddf_right_unknown(ddf_right):
    return ddf_right.clear_divisions()


@pytest.fixture
def ddf_right_single(df_right):
    return dd.from_pandas(df_right, npartitions=1, sort=False)


@pytest.fixture(params=['inner', 'left', 'right', 'outer'])
def how(request):
    return request.param


@pytest.fixture(params=[
    'idx',
    ['idx'],
    ['idx', 'k'],
    ['k', 'idx']])
def on(request):
    return request.param


# Tests
# =====
@pytest.mark.skipif(PANDAS_VERSION < '0.22.0',
                    reason="Need pandas col+index merge support (pandas-dev/pandas#14355)")
def test_merge_known_to_known(df_left, df_right, ddf_left, ddf_right, on, how):
    # Compute expected
    expected = df_left.merge(df_right, on=on, how=how)

    # Perform merge
    result = ddf_left.merge(ddf_right, on=on, how=how)

    # Assertions
    assert_eq(result, expected)
    assert_eq(result.divisions, (1, 2, 3, 4))


@pytest.mark.skipif(PANDAS_VERSION < '0.22.0',
                    reason="Need pandas col+index merge support (pandas-dev/pandas#14355)")
@pytest.mark.parametrize('how', ['inner', 'left'])
def test_merge_known_to_single(df_left, df_right, ddf_left, ddf_right_single, on, how):
    # Compute expected
    expected = df_left.merge(df_right, on=on, how=how)

    # Perform merge
    result = ddf_left.merge(ddf_right_single, on=on, how=how)

    # Assertions
    assert_eq(result, expected)
    assert_eq(result.divisions, ddf_left.divisions)


@pytest.mark.skipif(PANDAS_VERSION < '0.22.0',
                    reason="Need pandas col+index merge support (pandas-dev/pandas#14355)")
@pytest.mark.parametrize('how', ['inner', 'right'])
def test_merge_single_to_known(df_left, df_right, ddf_left_single, ddf_right, on, how):
    # Compute expected
    expected = df_left.merge(df_right, on=on, how=how)

    # Perform merge
    result = ddf_left_single.merge(ddf_right, on=on, how=how)

    # Assertions
    assert_eq(result, expected)
    assert_eq(result.divisions, ddf_right.divisions)


@pytest.mark.skipif(PANDAS_VERSION < '0.22.0',
                    reason="Need pandas col+index merge support (pandas-dev/pandas#14355)")
def test_merge_known_to_unknown(df_left, df_right, ddf_left, ddf_right_unknown, on, how):
    # Compute expected
    expected = df_left.merge(df_right, on=on, how=how)

    # Perform merge
    result = ddf_left.merge(ddf_right_unknown, on=on, how=how)
    assert_eq(result, expected)
    assert_eq(result.divisions, (None, None, None))


@pytest.mark.skipif(PANDAS_VERSION < '0.22.0',
                    reason="Need pandas col+index merge support (pandas-dev/pandas#14355)")
def test_merge_unknown_to_known(df_left, df_right, ddf_left_unknown, ddf_right, on, how):
    # Compute expected
    expected = df_left.merge(df_right, on=on, how=how)

    # Perform merge
    result = ddf_left_unknown.merge(ddf_right, on=on, how=how)

    # Assertions
    assert_eq(result, expected)
    assert_eq(result.divisions, (None, None, None))


@pytest.mark.skipif(PANDAS_VERSION < '0.22.0',
                    reason="Need pandas col+index merge support (pandas-dev/pandas#14355)")
def test_merge_unknown_to_unknown(df_left, df_right, ddf_left_unknown, ddf_right_unknown, on, how):
    # Compute expected
    expected = df_left.merge(df_right, on=on, how=how)

    # Merge unknown to unknown
    result = ddf_left_unknown.merge(ddf_right_unknown, on=on, how=how)
    assert_eq(result, expected)
    assert_eq(result.divisions, (None, None, None))
