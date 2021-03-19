import binascii
import hashlib


hashers = []  # In decreasing performance order


# Timings on a largish array:
# - xxHash3 is faster than CityHash
# - CityHash is 2x faster than xxHash
# - xxHash is faster than MurmurHash
# - MurmurHash is 8x faster than SHA1
# - SHA1 is significantly faster than all other hashlib algorithms

try:
    import xxhash  # `python -m pip install xxhash`
except ImportError:
    xxhash_flag = False
    pass
else:
    xxhash_flag = True
    if xxhash.VERSION >= "2.0.0":

        def _hash_xxhash3(buf):
            """
            Produce a 8-bytes hash of *buf* using xxHash3.
            """
            return xxhash.xxh3_64(buf).digest()

        hashers.append(_hash_xxhash3)


try:
    import cityhash  # `python -m pip install cityhash`
except ImportError:
    pass
else:
    # CityHash disabled unless the reference leak in
    # https://github.com/escherba/python-cityhash/pull/16
    # is fixed.
    if cityhash.__version__ >= "0.2.2":

        def _hash_cityhash(buf):
            """
            Produce a 16-bytes hash of *buf* using CityHash.
            """
            h = cityhash.CityHash128(buf)
            return h.to_bytes(16, "little")

        hashers.append(_hash_cityhash)

if xxhash_flag:

    def _hash_xxhash(buf):
        """
        Produce a 8-bytes hash of *buf* using xxHash.
        """
        return xxhash.xxh64(buf).digest()

    hashers.append(_hash_xxhash)

try:
    import mmh3  # `python -m pip install mmh3`
except ImportError:
    pass
else:

    def _hash_murmurhash(buf):
        """
        Produce a 16-bytes hash of *buf* using MurmurHash.
        """
        return mmh3.hash_bytes(buf)

    hashers.append(_hash_murmurhash)


def _hash_sha1(buf):
    """
    Produce a 20-bytes hash of *buf* using SHA1.
    """
    return hashlib.sha1(buf).digest()


hashers.append(_hash_sha1)


def hash_buffer(buf, hasher=None):
    """
    Hash a bytes-like (buffer-compatible) object.  This function returns
    a good quality hash but is not cryptographically secure.  The fastest
    available algorithm is selected.  A fixed-length bytes object is returned.
    """
    if hasher is not None:
        try:
            return hasher(buf)
        except (TypeError, OverflowError):
            # Some hash libraries may have overly-strict type checking,
            # not accepting all buffers
            pass
    for hasher in hashers:
        try:
            return hasher(buf)
        except (TypeError, OverflowError):
            pass
    raise TypeError("unsupported type for hashing: %s" % (type(buf),))


def hash_buffer_hex(buf, hasher=None):
    """
    Same as hash_buffer, but returns its result in hex-encoded form.
    """
    h = hash_buffer(buf, hasher)
    s = binascii.b2a_hex(h)
    return s.decode()
