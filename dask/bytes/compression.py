import sys
import zlib

from toolz import identity

from ..compatibility import gzip_compress, gzip_decompress, GzipFile
from ..utils import ignoring


compress = {'gzip': gzip_compress,
            'zlib': zlib.compress,
            None: identity}
decompress = {'gzip': gzip_decompress,
              'zlib': zlib.decompress,
              None: identity}
files = {'gzip': lambda f, **kwargs: GzipFile(fileobj=f, **kwargs)}


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

if sys.version_info[0] >= 3:
    import bz2
    compress['bz2'] = bz2.compress
    decompress['bz2'] = bz2.decompress
    files['bz2'] = bz2.BZ2File
