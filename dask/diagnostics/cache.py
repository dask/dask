from .core import Diagnostic
from timeit import default_timer
from cachey import Cache, nbytes


class cache(Diagnostic):
    """ Use cache for computation

    """

    def __init__(self, cache):
        self.cache = cache
        self.starttimes = dict()

    def _start(self, dsk):
        overlap = set(dsk) & set(self.cache.data)
        for key in overlap:
            dsk[key] = self.cache.data[key]

    def _pretask(self, key, dsk, state):
        self.starttimes[key] = default_timer()

    def _posttask(self, key, value, dsk, state, id):
        duration = default_timer() - self.starttimes[key]
        nb = nbytes(value)
        self.cache.put(key, value, cost=duration / nb / 1e9, nbytes=nb)

    def _finish(self, dsk, state, errored):
        pass
