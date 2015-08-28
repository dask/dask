from operator import add, mul
import os

from dask.diagnostics import Profiler
from dask.threaded import get
from dask.utils import ignoring, tmpfile
import pytest

try:
    import bokeh
except:
    bokeh = None


prof = Profiler()


dsk = {'a': 1,
       'b': 2,
       'c': (add, 'a', 'b'),
       'd': (mul, 'a', 'b'),
       'e': (mul, 'c', 'd')}


def test_profiler():
    with prof:
        out = get(dsk, 'e')
    assert out == 6
    prof_data = sorted(prof.results(), key=lambda d: d.key)
    keys = [i.key for i in prof_data]
    assert keys == ['c', 'd', 'e']
    tasks = [i.task for i in prof_data]
    assert tasks == [(add, 'a', 'b'), (mul, 'a', 'b'), (mul, 'c', 'd')]
    prof.clear()
    assert prof.results() == []


def test_profiler_works_under_error():
    div = lambda x, y: x / y
    dsk = {'x': (div, 1, 1), 'y': (div, 'x', 2), 'z': (div, 'y', 0)}

    with ignoring(ZeroDivisionError):
        with prof:
            out = get(dsk, 'z')

    assert all(len(v) == 5 for v in prof.results())
    assert len(prof.results()) == 2


@pytest.mark.skipif("not bokeh")
def test_pprint_task():
    from dask.diagnostics.profile_visualize import pprint_task
    keys = set(['a', 'b', 'c', 'd', 'e'])
    assert pprint_task((add, 'a', 1), keys) == 'add(_, *)'
    assert pprint_task((add, (add, 'a', 1)), keys) == 'add(add(_, *))'
    res = 'sum([*, _, add(_, *)])'
    assert pprint_task((sum, [1, 'b', (add, 'a', 1)]), keys) == res
    assert pprint_task((sum, (1, 2, 3, 4, 5, 6, 7)), keys) == 'sum(*)'

    assert len(pprint_task((sum, list(keys) * 100), keys)) < 100
    assert pprint_task((sum, list(keys) * 100), keys) == 'sum([_, _, _, ...])'
    assert pprint_task((sum, [1, 2, (sum, ['a', 4]), 5, 6] * 100), keys) == \
            'sum([*, *, sum([_, *]), ...])'
    assert pprint_task((sum, [1, 2, (sum, ['a', (sum, [1, 2, 3])]), 5, 6])
                      , keys) == 'sum([*, *, sum([_, sum(...)]), ...])'


@pytest.mark.skipif("not bokeh")
def test_profiler_plot():
    with prof:
        get(dsk, 'e')
    # Run just to see that it doesn't error
    prof.visualize(show=False)
    p = prof.visualize(plot_width=500,
                       plot_height=300,
                       tools="hover",
                       title="Not the default",
                       show=False)
    assert p.plot_width == 500
    assert p.plot_height == 300
    assert len(p.tools) == 1
    assert isinstance(p.tools[0], bokeh.models.HoverTool)
    assert p.title == "Not the default"


@pytest.mark.skipif("not bokeh")
def test_saves_file():
    with tmpfile('html') as fn:
        with prof:
            get(dsk, 'e')
        # Run just to see that it doesn't error
        prof.visualize(show=False, file_path=fn)

        assert os.path.exists(fn)
        with open(fn) as f:
            assert 'HTML' in f.read()


@pytest.mark.skipif("not bokeh")
def test_get_colors():
    from dask.diagnostics.profile_visualize import get_colors
    from bokeh.palettes import Blues9, Blues5, BrBG3
    from itertools import cycle
    funcs = list(range(11))
    cmap = get_colors('Blues', funcs)
    lk = dict(zip(funcs, cycle(Blues9)))
    assert cmap == [lk[i] for i in funcs]
    funcs = list(range(5))
    cmap = get_colors('Blues', funcs)
    lk = dict(zip(funcs, Blues5))
    assert cmap == [lk[i] for i in funcs]
    funcs = [0, 1, 0, 1, 0, 1]
    cmap = get_colors('BrBG', funcs)
    lk = dict(zip([0, 1], BrBG3))
    assert cmap == [lk[i] for i in funcs]
