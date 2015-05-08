import numpy as np

try:
    isclose = np.isclose
except AttributeError:
    def isclose(*args, **kwargs):
        raise RuntimeError("You need numpy version 1.7 or greater to use "
                           "isclose.")

try:
    full = np.full
except AttributeError:
    def full(shape, fill_value, dtype=None, order=None):
        """Our implementation of numpy.full because your numpy is old."""
        if order is not None:
            raise NotImplementedError("`order` kwarg is not supported upgrade "
                                      "to Numpy 1.8 or greater for support.")
        return np.multiply(fill_value, np.ones(shape, dtype=dtype),
                           dtype=dtype)
