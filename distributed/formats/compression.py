from toolz import identity

from ..compatibility import gzip_compress, gzip_decompress


compressors = {'gzip': gzip_compress,
               None: identity}
decompressors = {'gzip': gzip_decompress,
                 None: identity}
