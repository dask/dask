def istask(x):
    return isinstance(x, tuple) and x and callable(x[0])


def get(d, key, get=None):
    get = get or _get
    v = d[key]
    if istask(v):
        func, args = v[0], v[1:]
        return func(*[get(d, arg, get=get) for arg in args])
    else:
        return v

_get = get


def set(d, key, val, args=[]):
    assert key not in d
    if callable(val):
        val = (val,) + tuple(args)
    d[key] = val
