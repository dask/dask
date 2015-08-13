from .core import Array
from glob import glob
import os
try:
    from skimage.io import imread as sk_imread
except ImportError:
    pass


def tokenize(*args):
    from hashlib import md5
    return md5(str(args).encode()).hexdigest()

def add_leading_dimension(x):
    return x[None, ...]


def imread(filename):
    """ Read a stack of images into a dask array

    Parameters
    ----------

    filename: string
        A globstring like 'myfile.*.png'

    Example
    -------

    >>> from dask.array.image import imread
    >>> im = imread('2015-*-*.png')  # doctest: +SKIP
    >>> im.shape  # doctest: +SKIP
    (365, 1000, 1000, 3)

    Returns
    -------

    Dask array of all images stacked along the first dimension.  All images
    will be treated as individual chunks
    """
    filenames = sorted(glob(filename))
    if not filenames:
        raise ValueError("No files found under name %s" % filename)

    name = 'imread-%s' % tokenize(filenames, map(os.path.getmtime, filenames))

    sample = sk_imread(filenames[0])

    dsk = dict(((name, i) + (0,) * len(sample.shape),
                (add_leading_dimension, (sk_imread, filename)))
                for i, filename in enumerate(filenames))

    chunks = ((1,) * len(filenames),) + tuple((d,) for d in sample.shape)

    return Array(dsk, name, chunks, sample.dtype)
