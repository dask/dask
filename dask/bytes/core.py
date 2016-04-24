
storage_systems = dict()

def read_bytes(path, delimiter=None, not_zero=False, blocksize=2**27,
                     sample=True, compression=None):
    if '://' in path:
        protocol, path = path.split('://', 1)
        try:
            read_bytes = storage_systems[protocol]
        except KeyError:
            raise NotImplementedError("Unknown protocol %s://%s" %
                                      (protocol, path))
    else:
        read_bytes = storage_systems['local']

    return read_bytes(path, delimiter=delimiter, not_zero=not_zero,
            blocksize=blocksize, sample=sample, compression=compression)
