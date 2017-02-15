from toolz import concat, unique
from collections import MutableMapping


class ShareDict(MutableMapping):
    """ A MutableMapping composed of other MutableMappings

    This mapping is composed of a mapping of dictionaries

    Examples
    --------
    >>> a = {'x': 1, 'y': 2}
    >>> b = {'x': 10, 'z': 3}
    >>> s = ShareDict()
    >>> s.update(a)
    >>> s.update(b)

    >>> dict(s)  # doctest: +SKIP
    {'x': 10, 'y': 2, 'z': 3}

    These dictionaries are stored within an internal dictionary of dictionaries

    >>> list(s.dicts.values())  # doctest: +SKIP
    [{'x': 1, 'y': 2}, {'x': 10, 'z': 3}]

    By default these are named by their object id.  However, you can also
    provide explicit names.

    >>> s = ShareDict()
    >>> s.update(a=a)
    >>> s.update(b=b)
    >>> s.dicts  # doctest: +SKIP
    {'a': {'x': 1, 'y': 2}, 'b': {'x': 10, 'z': 3}}

    Precedence among these dicts are ordered by an internal list

    >>> s.order
    ['a', 'b']
    """
    def __init__(self):
        self.dicts = dict()
        self.order = []

    def _add_dict(self, name, d):
        if isinstance(d, ShareDict):
            for o in d.order:
                self._add_dict(o, d.dicts[o])
            return
        if name in self.dicts:
            self.order.remove(name)
        else:
            assert isinstance(d, dict)
            self.dicts[name] = d
        self.order.append(name)

    def update(self, *args, **kwargs):
        if 'key' in kwargs:
            assert len(args) == 1 and len(kwargs) == 1
            self._add_dict(kwargs['key'], args[0])
            return
        for arg in args:
            self._add_dict(id(arg), arg)
        for key, value in kwargs.items():
            self._add_dict(key, value)

    def __getitem__(self, key):
        for o in self.order[::-1]:
            d = self.dicts[o]
            if key in d:
                return d[key]
        raise KeyError(key)

    def __len__(self):
        return len(set.union(*map(set, self.dicts.values())))

    def items(self):
        seen = set()
        for o in self.order[::-1]:
            d = self.dicts[o]
            for key in d:
                if key not in seen:
                    seen.add(key)
                    yield (key, d[key])

    def __iter__(self):
        return unique(concat([self.dicts[o] for o in self.order[::-1]]))

    def __setitem__(self, key, value):
        raise NotImplementedError()

    def __delitem__(self, key):
        raise NotImplementedError()


def merge(*dicts):
    result = ShareDict()
    for d in dicts:
        if isinstance(d, tuple):
            key, d = d
            result.update(d, key=key)
        else:
            result.update(d)
    return result
