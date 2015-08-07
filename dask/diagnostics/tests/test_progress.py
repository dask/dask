from operator import add, mul
from dask.diagnostics import ProgressBar
from dask.diagnostics.progress import format_time
from dask.threaded import get
from dask.context import _globals


dsk = {'a': 1,
       'b': 2,
       'c': (add, 'a', 'b'),
       'd': (mul, 'a', 'b'),
       'e': (mul, 'c', 'd')}


def test_progressbar(capsys):
    with ProgressBar():
        out = get(dsk, 'e')
    assert out == 6
    out, err = capsys.readouterr()
    bar, percent, time = [i.strip() for i in out.split('\r')[-1].split('|')]
    assert bar == "[########################################]"
    assert percent == "100% Completed"
    with ProgressBar(width=20):
        out = get(dsk, 'e')
    out, err = capsys.readouterr()
    bar, percent, time = [i.strip() for i in out.split('\r')[-1].split('|')]
    assert bar == "[####################]"
    assert percent == "100% Completed"


def test_clean_exit():
    dsk = {'a': (lambda: 1/0,)}
    try:
        with ProgressBar() as pbar:
            get(dsk, 'a')
    except:
        pass
    assert not pbar._running
    assert not pbar._timer.is_alive()


def test_format_time():
    assert format_time(1.4) == ' 1.4s'
    assert format_time(10.4) == '10.4s'
    assert format_time(100.4) == ' 1min 40.4s'
    assert format_time(1000.4) == '16min 40.4s'
    assert format_time(10000.4) == ' 2hr 46min 40.4s'


def test_register(capsys):
    try:
        p = ProgressBar()
        p.register()

        assert _globals['callbacks']

        get(dsk, 'e')
        out, err = capsys.readouterr()
        bar, percent, time = [i.strip() for i in out.split('\r')[-1].split('|')]
        assert bar == "[########################################]"
        assert percent == "100% Completed"

        p.unregister()

        assert not _globals['callbacks']
    finally:
        _globals['callbacks'].clear()


def test_no_tasks(capsys):
    with ProgressBar():
        get({'x': 1}, 'x')

    out, err = capsys.readouterr()
    bar, percent, time = [i.strip() for i in out.split('\r')[-1].split('|')]
    assert bar == "[########################################]"
    assert percent == "100% Completed"
