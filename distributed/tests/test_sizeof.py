import sys

import pytest

from distributed.sizeof import sizeof


def test_base():
    assert sizeof(1) == sys.getsizeof(1)


def test_containers():
    assert sizeof([1, 2, [3]]) > (sys.getsizeof(3) * 3 + sys.getsizeof([]))


def test_numpy():
    np = pytest.importorskip('numpy')
    assert sizeof(np.empty(1000, dtype='f8')) >= 8000


def test_pandas():
    pd = pytest.importorskip('pandas')
    df = pd.DataFrame({'x': [1, 2, 3], 'y': ['a'*1000, 'b'*1000, 'c'*1000]},
                      index=[10, 20, 30])

    # TODO: change these once pandas memory_usage improves (in PR as of now)
    assert sizeof(df) >= sizeof(df.x) + sizeof(df.y) - sizeof(df.index)
    assert sizeof(df.x) >= sizeof(df.index)
    # assert sizeof(df.y) >= 1000 * 3
    assert sizeof(df.index) == 56
