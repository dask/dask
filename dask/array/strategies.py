from typing import Tuple, Union

import hypothesis.strategies as st


@st.composite
def block_lengths(
    draw: st.DrawFn,
    ax_length: int,
    min_chunk_length: int = 1,
    max_chunk_length: int = None,
) -> st.SearchStrategy[Tuple[int, ...]]:
    """Generate different chunking patterns along one dimension of an array."""

    chunks = []
    remaining_length = ax_length
    while remaining_length > 0:
        _max_chunk_length = min(remaining_length, max_chunk_length) if max_chunk_length else remaining_length

        if min_chunk_length > _max_chunk_length:
            # if we are at the end of the array we have no choice but to use a smaller chunk
            chunk = remaining_length
        else:
            chunk = draw(st.integers(min_value=min_chunk_length, max_value=_max_chunk_length))

        chunks.append(chunk)
        remaining_length = remaining_length - chunk

    return tuple(chunks)


@st.composite
def chunks(
    draw: st.DrawFn,
    shape: Tuple[int, ...],
    axes: Union[int, Tuple[int, ...]] = None,
    min_chunk_length: int = 1,
    max_chunk_length: int = None,
) -> st.SearchStrategy[Tuple[Tuple[int, ...], ...]]:
    """
    Generates different chunking patterns for an N-D array with a given shape.

    Returns chunking structure as a tuple of tuples of ints, with each inner tuple containing
    the block lengths along one dimension of the array.

    You can limit chunking to specific axes using the `axes` kwarg, and specify minimum and
    maximum block lengths.

    Requires the hypothesis package to be installed.

    Parameters
    ----------
    shape : tuple of ints
        Shape of the array for which you want to generate a chunking pattern.
    axes : None or int or tuple of ints, optional
        ...
    min_chunk_length : int, default is 1
        Minimum chunk length to use along all axes.
    max_chunk_length: int, optional
        Maximum chunk length to use along all axes.
        Default is that the chunk can be as long as the length of the array along that axis.

    Examples
    --------

    Chunking along all axes by default

    >>> chunks(shape=(2,3)).example()
    ((1, 1), (1, 2))

    Chunking only along the second axis

    >>> chunks(shape=(2,3), axis=1).example()
    ((2,), (1, 1, 1))

    Minimum size chunks of length 2 along all axes

    >>> chunks(shape=(2,3), min_chunk_length=2).example()
    ((2,), (2, 1))

    Smallest possible chunks along all axes

    >>> chunks(shape=(2,3), max_chunk_length=1).example()
    ((1, 1), (1, 1, 1))

    Maximum size chunks along all axes

    >>> chunks(shape=(2,3), axes=()).example()
    ((2,), (3,))
    """

    if min_chunk_length < 1 or not isinstance(min_chunk_length, int):
        raise ValueError("min_chunk_length must be an integer >= 1")

    if max_chunk_length:
        if max_chunk_length < 1 or not isinstance(min_chunk_length, int):
            raise ValueError("max_chunk_length must be an integer >= 1")

    if axes is None:
        axes = tuple(range(len(shape)))
    elif isinstance(axes, int):
        axes = (axes,)

    chunks = []
    for axis, ax_length in enumerate(shape):

        _max_chunk_length = min(max_chunk_length, ax_length) if max_chunk_length else ax_length

        print(min_chunk_length)
        print(_max_chunk_length)

        if axes is not None and axis in axes:
            block_lengths_along_ax = draw(
                block_lengths(
                    ax_length,
                    min_chunk_length=min_chunk_length,
                    max_chunk_length=_max_chunk_length,
                )
            )
        else:
            # don't chunk along this dimension
            block_lengths_along_ax = (ax_length,)

        chunks.append(block_lengths_along_ax)

    return tuple(chunks)
