from __future__ import print_function, division, absolute_import

import sys

import pytest

from distributed.sizeof import sizeof, getsizeof


def test_base():
    assert sizeof(1) == getsizeof(1)


def test_containers():
    assert sizeof([1, 2, [3]]) > (getsizeof(3) * 3 + getsizeof([]))


def test_numpy():
    np = pytest.importorskip('numpy')
    assert sizeof(np.empty(1000, dtype='f8')) == 8000
    dt = np.dtype('f8')
    assert sizeof(dt) == sys.getsizeof(dt)


def test_pandas():
    pd = pytest.importorskip('pandas')
    df = pd.DataFrame({'x': [1, 2, 3], 'y': ['a'*100, 'b'*100, 'c'*100]},
                      index=[10, 20, 30])

    assert sizeof(df) >= sizeof(df.x) + sizeof(df.y) - sizeof(df.index)
    assert sizeof(df.x) >= sizeof(df.index)
    if pd.__version__ >= '0.17.1':
        assert sizeof(df.y) >= 100 * 3
    assert sizeof(df.index) >= 20

    assert isinstance(sizeof(df), int)
    assert isinstance(sizeof(df.x), int)
    assert isinstance(sizeof(df.index), int)
