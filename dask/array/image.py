"""
A thin wrapper for scipy.ndimage.filters
"""

def _make_ghost_arr(filt, arr, filter_kwargs):

    def boundary(filter_kwargs):
        """
        Create a value for the dask.array.ghost.ghost boundary kwarg from the
        arguments to the ndimage filter.
        """

        # every (3) ndimage filte I looked at had reflect as the
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
            return {i : 'reflect' for i in range(arr.ndim)}
        elif mode == 'constant':
            cval = filter_kwargs.get('cval', 0.0)
            return {i : cval for i in range(arr.ndim)}
        else:
            raise ValueError("mode argument % not supported, only 'reflect'"
                             " and 'constant' supported.")

    def depth(filt, filter_kwargs):
        """
        create the value for the `depth` kwarg. This should be
        len(ciel(shape[axis_length]/2.)) for each axis.
        """
        pass


# filt is a ndimage filter (note filter is a python name)
def filter_(filt, arr, **filter_kwargs):
    """Apply a scipy ndimage filter to a dask array

    Parameters
    ----------
    filter : scipy.ndimage.filters filter
    darr : das Array
        Two dimensional array
    kwargs :
        Options to pass to the filter
    
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

    return arr.map_blocks(wrapped_func)
