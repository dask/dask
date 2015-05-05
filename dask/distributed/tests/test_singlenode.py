from dask.distributed.single import get

def test_get():
    assert get({'x': 1, 'y': (lambda x: x + 1, 'x')}, 'y') == 2
