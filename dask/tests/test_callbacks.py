from dask.async import get_sync
from dask.callbacks import Callback

def test_start_callback():
    flag = [False]

    class MyCallback(Callback):
        def _start(self, dsk):
            flag[0] = True

    with MyCallback():
        get_sync({'x': 1}, 'x')

    assert flag[0] is True

def test_start_state_callback():
    flag = [False]

    class MyCallback(Callback):
        def _start_state(self, dsk, state):
            flag[0] = True
            assert dsk['x'] == 1
            assert len(state['cache']) == 1

    with MyCallback():
        get_sync({'x': 1}, 'x')

    assert flag[0] is True
