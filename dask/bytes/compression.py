import sys
import zlib

from toolz import identity

from ..compatibility import gzip_compress, gzip_decompress, GzipFile
from ..utils import ignoring


compressors = {'gzip': gzip_compress,
               'zlib': zlib.compress,
               None: identity}
decompressors = {'gzip': gzip_decompress,
                 'zlib': zlib.decompress,
                 None: identity}
files = {'gzip': lambda f, **kwargs: GzipFile(fileobj=f, **kwargs)}

with ignoring(ImportError):
    import snappy
    compressors['snappy'] = snappy.compress
    decompressors['snappy'] = snappy.decompress


with ignoring(ImportError):
    import lz4
    compressors['lz4'] = lz4.LZ4_compress
    decompressors['lz4'] = lz4.LZ4_uncompress

with ignoring(ImportError):
    from ..compatibility import LZMAFile, lzma_compress, lzma_decompress
    compressors['xz'] = lzma_compress
    decompressors['xz'] = lzma_decompress
    files['xz'] = LZMAFile

if sys.version_info[0] >= 3:
    import bz2
    compressors['bz2'] = bz2.compress
    decompressors['bz2'] = bz2.decompress
    files['bz2'] = bz2.BZ2File
