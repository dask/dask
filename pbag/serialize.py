from pandas import msgpack
import msgpack
import struct
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

# import blosc
# blosc.set_nthreads(1)


def dump(obj, file):
    text = msgpack.packb(obj)                      # Serialize
    # payload = blosc.compress(text, 1)            # Compress
    payload = text                                 # No compression
    file.write(struct.pack('l', len(payload)))     # store length of payload
    file.write(payload)                            # store payload


def load(file):
    try:
        size = struct.unpack('l', file.read(8))[0]  # Read length of payload
    except:
        raise IOError('Unable to read more from file')
    payload = file.read(size)                       # Read payload
    text = payload                                  # No compression
    # text = blosc.decompress(compressed)           # Decompress
    return msgpack.unpackb(text)                    # Deserialize
