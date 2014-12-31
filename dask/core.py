def ishashable(x):
    """ Is x hashable?

    Exmaple
    -------

    >>> ishashable(1)
    True
    >>> ishashable([1])
    False
    """
    try:
        hash(x)
        return True
    except TypeError:
        return False


def istask(x):
    """ Is x a runnable task?

    A task is a tuple with a callable first argument

    Exmaple
    -------

    >>> inc = lambda x: x + 1
    >>> istask((inc, 1))
    True
    >>> istask(1)
    False
    """
    return isinstance(x, tuple) and x and callable(x[0])


def get(d, key, get=None, concrete=True, **kwargs):
    """ Get value from Dask

    Exmaple
    -------

    >>> inc = lambda x: x + 1
    >>> d = {'x': 1, 'y': (inc, 'x')}

    >>> get(d, 'x')
    1
    >>> get(d, 'y')
    2

    See Also
    --------
    set
    """
    get = get or _get
    if isinstance(key, list):
        v = (get(d, k, get=get, concrete=concrete) for k in key)
        if concrete:
            v = list(v)
    elif not ishashable(key) or key not in d:
        return key
    else:
        v = d[key]
    if istask(v):
        func, args = v[0], v[1:]
        args2 = [get(d, arg, get=get, concrete=False) for arg in args]
        return func(*[get(d, arg, get=get) for arg in args2])
    else:
        return v

_get = get


def set(d, key, val, args=[]):
    """ Set value for key in Dask

    Exmaple
    -------

    >>> d = {}
    >>> set(d, 'x', 1)
    >>> d
    {'x': 1}

    >>> inc = lambda x: x + 1
    >>> set(d, 'y', inc, args=['x'])

    >>> get(d, 'y')
    2

    See Also
    --------
    get
    """
    assert key not in d
    if callable(val):
        val = (val,) + tuple(args)
    d[key] = val
