from toolz import concat
from collections import Mapping


class ShareDict(Mapping):
    """ A MutableMapping composed of other MutableMappings

    This mapping is composed of a mapping of dictionaries

    Examples
    --------
    >>> a = {'x': 1, 'y': 2}
    >>> b = {'z': 3}
    >>> s = ShareDict()
    >>> s.update(a)
    >>> s.update(b)

    >>> dict(s)  # doctest: +SKIP
    {'x': 1, 'y': 2, 'z': 3}

    These dictionaries are stored within an internal dictionary of dictionaries

    >>> list(s.dicts.values())  # doctest: +SKIP
    [{'x': 1, 'y': 2}, {'z': 3}]

    By default these are named by their object id.  However, you can also
    provide explicit names.

    >>> s = ShareDict()
    >>> s.update(a, key='a')
    >>> s.update(b, key='b')
    >>> s.dicts  # doctest: +SKIP
    {'a': {'x': 1, 'y': 2}, 'b': {'z': 3}}
    """
    def __init__(self):
        self.dicts = dict()

    def update_with_key(self, arg, key=None):
        if type(arg) is ShareDict:
            self.dicts.update(arg.dicts)
            return

        if key is None:
            key = id(arg)

        assert isinstance(arg, dict)
        self.dicts[key] = arg

    def update(self, arg):
        self.update_with_key(arg)

    def __getitem__(self, key):
        for d in self.dicts.values():
            if key in d:
                return d[key]
        raise KeyError(key)

    def __len__(self):
        return sum(map(len, self.dicts.values()))

    def items(self):
        seen = set()
        for d in self.dicts.values():
            for key in d:
                if key not in seen:
                    seen.add(key)
                    yield (key, d[key])

    def __iter__(self):
        return concat(self.dicts.values())


def sortkey(d):
    if type(d) is ShareDict:
        return (0, -len(d.dicts))
    else:
        return (1, -len(d))


def merge(*dicts):
    result = ShareDict()
    for d in sorted(dicts, key=sortkey):
        if isinstance(d, tuple):
            key, d = d
            result.update_with_key(d, key=key)
        else:
            result.update_with_key(d)
    return result
