import copyreg
import math

import numpy as np
import pandas as pd

try:
    import pyarrow as pa
except ImportError:
    pa = None

# Pickling of pyarrow arrays is effectively broken - pickling a slice of an
# array ends up pickling the entire backing array.
#
# See https://issues.apache.org/jira/browse/ARROW-10739
#
# This comes up when using pandas `string[pyarrow]` dtypes, which are backed by
# a `pyarrow.StringArray`.  To fix this, we register a *global* override for
# pickling `pandas.core.arrays.ArrowStringArray` types. We do this at the
# pandas level rather than the pyarrow level for efficiency reasons (a pandas
# ArrowStringArray may contain many small pyarrow StringArray objects).
#
# This pickling implementation manually mucks with the backing buffers in a
# fairly efficient way:
#
# - The data buffer is never copied
# - The offsets buffer is only copied if the array is sliced with a start index
#   (x[start:])
# - The mask buffer is never copied
#
# This implementation works with pickle protocol 5, allowing support for true
# zero-copy sends.
#
# XXX: Once pyarrow (or pandas) has fixed this bug, we should skip registering
# with copyreg for versions that lack this issue.


def pyarrow_stringarray_to_parts(array):
    """Decompose a ``pyarrow.StringArray`` into a tuple of components.

    The resulting tuple can be passed to
    ``pyarrow_stringarray_from_parts(*components)`` to reconstruct the
    ``pyarrow.StringArray``.
    """
    # Access the backing buffers.
    #
    # - mask: None, or a bitmask of length ceil(nitems / 8). 0 bits mark NULL
    #   elements, only present if NULL data is present, commonly None.
    # - offsets: A uint32 array of nitems + 1 items marking the start/stop
    #   indices for the individual elements in `data`
    # - data: All the utf8 string data concatenated together
    #
    # The structure of these buffers comes from the arrow format, documented at
    # https://arrow.apache.org/docs/format/Columnar.html#physical-memory-layout.
    # In particular, this is a `StringArray` (4 byte offsets), rather than a
    # `LargeStringArray` (8 byte offsets).
    assert pa.types.is_string(array.type)

    mask, offsets, data = array.buffers()
    nitems = len(array)

    if not array.offset:
        # No leading offset, only need to slice any unnecessary data from the
        # backing buffers
        offsets = offsets[: 4 * (nitems + 1)]
        data_stop = int.from_bytes(offsets[-4:], "little")
        data = data[:data_stop]
        if mask is None:
            return nitems, offsets, data
        else:
            mask = mask[: math.ceil(nitems / 8)]
            return nitems, offsets, data, mask

    # There is a leading offset. This complicates things a bit.
    offsets_start = array.offset * 4
    offsets_stop = offsets_start + (nitems + 1) * 4
    data_start = int.from_bytes(offsets[offsets_start : offsets_start + 4], "little")
    data_stop = int.from_bytes(offsets[offsets_stop - 4 : offsets_stop], "little")
    data = data[data_start:data_stop]

    if mask is None:
        npad = 0
    else:
        # Since the mask is a bitmask, it can only represent even units of 8
        # elements.  To avoid shifting any bits, we pad the array with up to 7
        # elements so the mask array can always be serialized zero copy.
        npad = array.offset % 8
        mask_start = array.offset // 8
        mask_stop = mask_start + math.ceil(nitems / 8)
        mask = mask[mask_start:mask_stop]

    # Subtract the offset of the starting element from every used offset in the
    # offsets array, ensuring the first element in the serialized `offsets`
    # array is always 0.
    offsets_array = np.frombuffer(offsets, dtype="i4")
    offsets_array = (
        offsets_array[array.offset : array.offset + nitems + 1]
        - offsets_array[array.offset]
    )
    # Pad the new offsets by `npad` offsets of 0 (see the `mask` comment above). We wrap
    # this in a `pyarrow.py_buffer`, since this type transparently supports pickle 5,
    # avoiding an extra copy inside the pickler.
    offsets = pa.py_buffer(
        b"\x00" * (4 * npad) + offsets_array.data if npad else offsets_array.data
    )

    if mask is None:
        return nitems, offsets, data
    else:
        return nitems, offsets, data, mask, npad


def pyarrow_stringarray_from_parts(nitems, data_offsets, data, mask=None, offset=0):
    """Reconstruct a ``pyarrow.StringArray`` from the parts returned by
    ``pyarrow_stringarray_to_parts``."""
    return pa.StringArray.from_buffers(nitems, data_offsets, data, mask, offset=offset)


def rebuild_arrowstringarray(*chunk_parts):
    """Rebuild a ``pandas.core.arrays.ArrowStringArray``"""
    array = pa.chunked_array(
        [pyarrow_stringarray_from_parts(*parts) for parts in chunk_parts],
        type=pa.string(),
    )
    return pd.arrays.ArrowStringArray(array)


def reduce_arrowstringarray(x):
    """A pickle override for ``pandas.core.arrays.ArrowStringArray`` that avoids
    serializing unnecessary data, while also avoiding/minimizing data copies"""
    # Decompose each chunk in the backing ChunkedArray into their individual
    # components for serialization. We filter out 0-length chunks, since they
    # add no meaningful value to the chunked array.
    chunks = tuple(
        pyarrow_stringarray_to_parts(chunk)
        for chunk in x._data.chunks
        if len(chunk) > 0
    )
    return (rebuild_arrowstringarray, chunks)


if hasattr(pd.arrays, "ArrowStringArray") and pa is not None:
    copyreg.dispatch_table[pd.arrays.ArrowStringArray] = reduce_arrowstringarray
