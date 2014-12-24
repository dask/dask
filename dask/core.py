def ishashable(x):
    try:
        hash(x)
        return True
    except TypeError:
        return False


def istask(x):
    return isinstance(x, tuple) and x and callable(x[0])


def get(d, key, get=None):
    get = get or _get
    if isinstance(key, list):
        v = (get(d, k, get=get) for k in key)
    elif not ishashable(key) or key not in d:
        return key
    else:
        v = d[key]
    if istask(v):
        func, args = v[0], v[1:]
        args2 = [get(d, arg, get=get) for arg in args]
        return func(*[get(d, arg, get=get) for arg in args2])
    else:
        return v

_get = get


def set(d, key, val, args=[]):
    assert key not in d
    if callable(val):
        val = (val,) + tuple(args)
    d[key] = val
