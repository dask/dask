from collections import Iterator


def concrete(seq):
    """ Make nested iterators concrete lists

    >>> data = [[1, 2], [3, 4]]
    >>> seq = iter(map(iter, data))
    >>> concrete(seq)
    [[1, 2], [3, 4]]
    """
    if isinstance(seq, Iterator):
        seq = list(seq)
    if isinstance(seq, list):
        seq = list(map(concrete, seq))
    return seq


