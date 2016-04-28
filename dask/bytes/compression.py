import bz2
import sys
import zlib

from toolz import identity

from ..compatibility import gzip_compress, gzip_decompress, GzipFile
from ..utils import ignoring


def noop_file(file, **kwargs):
    return file

compress = {'gzip': gzip_compress,
            'zlib': zlib.compress,
            'bz2': bz2.compress,
            None: identity}
decompress = {'gzip': gzip_decompress,
              'zlib': zlib.decompress,
              'bz2': bz2.decompress,
              None: identity}
files = {'gzip': lambda f, **kwargs: GzipFile(fileobj=f, **kwargs),
         None: noop_file}
seekable_files = {None: noop_file}


with ignoring(ImportError):
    import snappy
    compress['snappy'] = snappy.compress
    decompress['snappy'] = snappy.decompress


with ignoring(ImportError):
    import lz4
    compress['lz4'] = lz4.LZ4_compress
    decompress['lz4'] = lz4.LZ4_uncompress

with ignoring(ImportError):
    from ..compatibility import LZMAFile, lzma_compress, lzma_decompress
    compress['xz'] = lzma_compress
    decompress['xz'] = lzma_decompress
    files['xz'] = LZMAFile

with ignoring(ImportError):
    import lzma
    seekable_files['xz'] = lzma.LZMAFile

with ignoring(ImportError):
    import lzmaffi
    seekable_files['xz'] = lzmaffi.LZMAFile


if sys.version_info[0] >= 3:
    import bz2
    files['bz2'] = bz2.BZ2File
