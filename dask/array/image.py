import os
from glob import glob

import numpy as np

try:
    from skimage.io import imread as sk_imread
except (AttributeError, ImportError):
    pass

try:
    from pims import open as pims_open
except (AttributeError, ImportError):
    pass

from ..base import tokenize
from .core import Array


def add_leading_dimension(x):
    return x[None, ...]


def imread(filename, imread=None, preprocess=None):
    """Read a stack of images into a dask array

    Parameters
    ----------

    filename: string
        A globstring like 'myfile.*.png'
    imread: function (optional)
        Optionally provide custom imread function.
        Function should expect a filename and produce a numpy array.
        Defaults to ``skimage.io.imread``.
    preprocess: function (optional)
        Optionally provide custom function to preprocess the image.
        Function should expect a numpy array for a single image.

    Examples
    --------

    >>> from dask.array.image import imread
    >>> im = imread('2015-*-*.png')  # doctest: +SKIP
    >>> im.shape  # doctest: +SKIP
    (365, 1000, 1000, 3)

    Returns
    -------

    Dask array of all images stacked along the first dimension.  All images
    will be treated as individual chunks
    """
    imread = imread or sk_imread
    filenames = sorted(glob(filename))
    if not filenames:
        raise ValueError("No files found under name %s" % filename)

    name = "imread-%s" % tokenize(filenames, map(os.path.getmtime, filenames))

    if pims_open and not preprocess:
        sample = pims_open(filenames[0])
        dtype = np.dtype(sample.pixel_type)
        shape = sample.frame_shape
        if len(sample) > 1:
            shape = (len(sample),) + shape
    else:
        sample = imread(filenames[0])
        if preprocess:
            sample = preprocess(sample)
        dtype = sample.dtype
        shape = sample.shape

    keys = [(name, i) + (0,) * len(shape) for i in range(len(filenames))]
    if preprocess:
        values = [
            (add_leading_dimension, (preprocess, (imread, fn))) for fn in filenames
        ]
    else:
        values = [(add_leading_dimension, (imread, fn)) for fn in filenames]
    dsk = dict(zip(keys, values))

    chunks = ((1,) * len(filenames),) + tuple((d,) for d in shape)

    return Array(dsk, name, chunks, dtype)
