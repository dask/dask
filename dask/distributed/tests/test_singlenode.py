from dask.distributed.singlenode import get
from operator import add

def test_get():
    assert get({'x': 1, 'y': (add, 'x', 1)}, 'y') == 2
