"""
A thin wrapper for scipy.ndimage.filters
"""

from . import ghost

def _make_ghost_kws(filt, arr, filter_kwargs):

    def boundary(filter_kwargs):
        """
        Create a value for the dask.array.ghost.ghost boundary kwarg from the
        arguments to the ndimage filter.
        """

        # every (3) ndimage filters I looked at had reflect as the
        # default mode. So they all are hopefully like that.

        # ndimage ignores the `cval` when it is set but `mode` not set or
        # is not `constant`. We mimic this.

        # get the info
        mode = filter_kwargs.get('mode', 'reflect')

        # for reference:
        # ours = ['periodic', 'reflect', any-constant]
        # theirs = ['nearest', 'wrap', 'reflect', 'constant']
        # translate to our kwargs
        if mode == 'reflect':
            return dict((i, 'reflect') for i in range(arr.ndim))
        elif mode == 'constant':
            cval = filter_kwargs.get('cval', 0.0)
            return dict((i, cval) for i in range(arr.ndim))
        else:
            raise ValueError("mode argument % not supported, only 'reflect'"
                             " and 'constant' supported.")

    def depth(arr, filt, filter_kwargs):
        """
        create the value for the `depth` kwarg. This should be
        len(ciel(shape[axis_length]/2.)) for each axis.
        """
        """
        TODO work for all filters

        we could optimize this by not applying the filter on ghost cells

        generalize ghosting for 1d filters
        """
        # gaussian depth
        def gauss_depth(arr, filt, filter_kwargs):
            """
            The needed depth depends on the `sigma` and `truncate` kwargs.
            """
            sigma = filter_kwargs['sigma']
            truncate = filter_kwargs.get('truncate', 4.0)
            depth = int(truncate * float(sigma) + 0.5) + 1
            return dict((i, depth) for i in range(arr.ndim))

        def gauss1d_depth(arr, filt, filter_kwargs):
            sigma = filter_kwargs['sigma']
            truncate = filter_kwargs.get('truncate', 4.0)
            depth = int(truncate * float(sigma) + 0.5) + 1
            keys = [0] * arr.ndim
            keys[-1] = depth
            return dict(zip(range(arr.ndim), keys))

        # correlate1d
        def correlate1d_depth(arr, filt, filter_kwargs):
            weights = filter_kwargs['weights']
            depth =(len(weights) // 2) + 1
            return dict((i, depth) for i in range(arr.ndim))

        # choose a deth function from the ndimage filter name
        depths = {'gaussian_filter' : gauss_depth,
                  'gaussian_filter1d': gauss1d_depth,
                  'correlate1d': correlate1d_depth,
                  'convolve1d' : correlate1d_depth,}

        return depths[filt.__name__](arr, filt, filter_kwargs)

    return {'depth' : depth(arr, filt, filter_kwargs),
            'boundary' : boundary(filter_kwargs)}


# filt is a ndimage filter (note filter is a python name)
def filter_(filt, arr, **filter_kwargs):
    """Apply a scipy ndimage filter to a dask array

    Parameters
    ----------
    filter : scipy.ndimage.filters filter
    darr : dask Array
        Two dimensional array
    filter_kwargs:
        keyword arguments to pass to the filter.

    consider taking two seperate dictionaries of arguments for the filter and
    for ghosting.

    How sohuld the positional arguments to the ndimage filters be handeled?
    Currently they are just given their obvious name in the filter_kwargs.
    
    Example
    -------

    >>> import dask.array as da
    >>> x = da.random.random((10, 10), chunks=(5, 2))

    >>> from scipy.ndimage.filters import gaussian_filter
    
    >>> da.image.filter_(gaussian_filter, x, sigma=1)
    dask.array<x_??, shape=(10, 10), chunks=((???)), dtype=float64>
    """

    """
    TODO
    inspect the footprint of the filter to determine how much ghosting we need.
    """

    def wrapped_func(block):
        return filt(block, **filter_kwargs)
    ghost_kws = _make_ghost_kws(filt, arr, filter_kwargs)
    print(ghost_kws)

    ghosted = ghost.ghost(arr, **ghost_kws)
    mapped = ghosted.map_blocks(wrapped_func)
    return ghost.trim_internal(mapped, ghost_kws['depth'])
