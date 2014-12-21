import dask

def inc(x):
    return x + 1

def add(x, y):
    return x + y


def test_comprehensive_user_experience():
    d = dict()
    dask.set(d, 'x', 1)
    dask.set(d, 'y', inc, args=['x'])
    dask.set(d, 'z', add, args=['x', 'y'])

    assert dask.get(d, 'z') == 1 + inc(1)


def test_istask():
    assert dask.istask((inc, 1))
    assert not dask.istask(1)
    assert not dask.istask((1, 2))


d = {':x': 1,
     ':y': (inc, ':x'),
     ':z': (add, ':x', ':y')}


def test_get():
    assert dask.get(d, ':x') == 1
    assert dask.get(d, ':y') == 2
    assert dask.get(d, ':z') == 3


def test_set():
    d2 = d.copy()
    dask.set(d2, ':w', add, args=[':x', ':x'])
    assert dask.get(d2, ':w') == 2


def test_memoized_get():
    try:
        import toolz
    except ImportError:
        return
    cache = dict()
    getm = toolz.memoize(dask.get, cache=cache, key=lambda args, kwargs: args[1:])

    result = getm(d, ':z', get=getm)
    assert result == 3

    assert cache == {(':x',): 1,
                     (':y',): 2,
                     (':z',): 3}

def test_data_not_in_dict_is_ok():
    d = {'x': 1}
    dask.set(d, 'y', add, args=['x', 10])

    assert dask.get(d, 'y') == 11

