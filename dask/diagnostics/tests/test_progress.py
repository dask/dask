from operator import add, mul
from dask.diagnostics import ProgressBar
from dask.diagnostics.progress import format_time
from dask.threaded import get


dsk = {'a': 1,
       'b': 2,
       'c': (add, 'a', 'b'),
       'd': (mul, 'a', 'b'),
       'e': (mul, 'c', 'd')}


def test_profiler(capsys):
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


def test_format_time():
    assert format_time(1.4) == ' 1.4s'
    assert format_time(10.4) == '10.4s'
    assert format_time(100.4) == ' 1min 40.4s'
    assert format_time(1000.4) == '16min 40.4s'
    assert format_time(10000.4) == ' 2hr 46min 40.4s'
