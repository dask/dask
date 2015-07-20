from operator import add, mul
from dask.diagnostics import thread_prof
import pytest
try:
    import bokeh
except:
    bokeh = None


dsk = {'a': 1,
       'b': 2,
       'c': (add, 'a', 'b'),
       'd': (mul, 'a', 'b'),
       'e': (mul, 'c', 'd')}


def test_profiler():
    out = thread_prof.get(dsk, 'e')
    assert out == 6
    prof_data = sorted(thread_prof.results(), key=lambda d: d.key)
    keys = [i.key for i in prof_data]
    assert keys == ['c', 'd', 'e']
    tasks = [i.task for i in prof_data]
    assert tasks == [(add, 'a', 'b'), (mul, 'a', 'b'), (mul, 'c', 'd')]
    thread_prof.clear()
    assert thread_prof.results() == []


@pytest.mark.skipif("not bokeh")
def test_pprint_task():
    from dask.diagnostics.profile_visualize import pprint_task
    keys = set(['a', 'b', 'c', 'd', 'e'])
    assert pprint_task((add, 'a', 1), keys) == 'add(_, 1)'
    assert pprint_task((add, (add, 'a', 1)), keys) == 'add(add(_, 1))'
    res = 'sum([1, _, add(_, 1)])'
    assert pprint_task((sum, [1, 'b', (add, 'a', 1)]), keys) == res
    assert pprint_task((sum, (1, 2, 3, 4, 5, 6, 7)), keys) == 'sum(<tuple>)'


@pytest.mark.skipif("not bokeh")
def test_profiler_plot():
    thread_prof.get(dsk, 'e')
    # Run just to see that it doesn't error
    thread_prof.visualize(show=False)
    p = thread_prof.visualize(plot_width=500,
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
