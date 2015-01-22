import sys

PY3 = sys.version_info[0] == 3
PY2 = sys.version_info[0] == 2

if PY3:
    import builtins
    from queue import Queue
    from itertools import accumulate
else:
    import __builtin__ as builtins
    from Queue import Queue
    import operator
    def accumulate(iterable, func=operator.add):
        it = iter(iterable)
        total = next(it)
        yield total
        for element in it:
            total = func(total, element)
            yield total
