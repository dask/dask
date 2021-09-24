def ishashable(x):
    """Is x hashable?

    Examples
    --------

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
    """Is x a runnable task?

    A task is a tuple with a callable first argument

    Examples
    --------

    >>> inc = lambda x: x + 1
    >>> istask((inc, 1))
    True
    >>> istask(1)
    False
    """
    return type(x) is tuple and x and callable(x[0])