try:
    from pandas import msgpack
except ImportError:
    import msgpack
import struct
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO


def dump(obj, file):
    text = msgpack.packb(obj)                      # Serialize
    payload = text                                 # No compression
    file.write(struct.pack('Q', len(payload)))     # store length of payload
    file.write(payload)                            # store payload


def load(file):
    try:
        size = struct.unpack('Q', file.read(8))[0]  # Read length of payload
    except:
        raise IOError('Unable to read more from file')
    payload = file.read(size)                       # Read payload
    text = payload                                  # No compression
    return msgpack.unpackb(text)                    # Deserialize
