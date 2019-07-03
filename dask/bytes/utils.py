from fsspec.core import infer_compression, expand_paths_if_needed
from fsspec.utils import (seek_delimiter, stringify_path, build_name_function,
                          infer_storage_options, update_storage_options,
                          read_block)

import io
import gzip
import bz2
import lzma
import zipfile


def zip_compress(data):
    """Write data into zipfile and return the bytes"""
    out = io.BytesIO()
    with zipfile.ZipFile(file=out, mode='w') as z:
        with z.open('myfile', 'w') as zf:
            zf.write(data)
    out.seek(0)
    return out.read()


compress = {'gzip': gzip.compress,
            'bz2': bz2.compress,
            None: lambda x: x,
            'xz': lzma.compress,
            'zip': zip_compress}


def SeekableFile(x):
    return x
