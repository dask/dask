from __future__ import absolute_import, division, print_function

def raises(err, lamda):
    try:
        lamda()
        return False
    except err:
        return True


def deepmap(func, seq):
    """ Apply function inside nested lists

    >>> inc = lambda x: x + 1
    >>> deepmap(inc, [[1, 2], [3, 4]])
    [[2, 3], [4, 5]]
    """
    if isinstance(seq, list):
        return [deepmap(func, item) for item in seq]
    else:
        return func(seq)
