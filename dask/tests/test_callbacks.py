from dask.async import get_sync
from dask.threaded import get as get_threaded
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


def test_finish_always_called():
    flag = [False]

    class MyCallback(Callback):
        def _finish(self, dsk, state, errored):
            flag[0] = True
            assert errored

    dsk = {'x': (lambda: 1 / 0,)}

    # `raise_on_exception=True`
    try:
        with MyCallback():
            get_sync(dsk, 'x')
    except Exception as e:
        assert isinstance(e, ZeroDivisionError)
    assert flag[0]

    # `raise_on_exception=False`
    flag[0] = False
    try:
        with MyCallback():
            get_threaded(dsk, 'x')
    except Exception as e:
        assert isinstance(e, ZeroDivisionError)
    assert flag[0]

    # KeyboardInterrupt
    def raise_keyboard():
        raise KeyboardInterrupt()

    dsk = {'x': (raise_keyboard,)}
    flag[0] = False
    try:
        with MyCallback():
            get_sync(dsk, 'x')
    except BaseException as e:
        assert isinstance(e, KeyboardInterrupt)
    assert flag[0]
