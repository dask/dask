from dask.async import get_sync
from dask.callbacks import Callback

def test_callback():
    flag = [False]

    class MyCallback(Callback):
        def _start(self, dsk):
            flag[0] = True

    with MyCallback():
        get_sync({'x': 1}, 'x')

    assert flag[0] is True
