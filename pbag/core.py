try:
    import cPickle as pickle
except ImportError:
    import pickle

import tempfile
from cytoolz import groupby, partial, take, concat
import os
import shutil
from collections import Iterator, Iterable


class PBag(object):
    """ Partitioned, on-disk, Bag

    TODO: Find better name

    A PBag partitions and stores a sequence on disk.

    It assigns a group to each element of the input and stores batches of
    similarly grouped inputs to a file on disk.  It does this in a streaming
    way to enable the partitioning of large sequences on disk.

    It also enables the extraction of any one of those groups.

    >>> pb = PBag(grouper=lambda x: x[0], npartitions=10)
    >>> pb.extend([[0, 'Alice', 100], [1, 'Bob', 200], [0, 'Charlie', 300]])

    >>> pb.get_partition(0)
    [[0, 'Alice', 100], [0, 'Charlie', 300]]
    """
    def __init__(self, grouper, npartitions, path=None, open=open,
                 dump=partial(pickle.dump, protocol=pickle.HIGHEST_PROTOCOL),
                 load=pickle.load):
        self.grouper = grouper
        if path is None:
            self.path = tempfile.mkdtemp('.pbag')
            self._explicitly_given_path = False
        else:
            self.path = path
            if not os.path.exists(path):
                os.mkdir(self.path)
            self._explicitly_given_path = True

        self.npartitions = npartitions
        self.open = open
        self.isopen = False
        self.dump = dump
        self.load = load

    @property
    def filenames(self):
        return [os.path.join(self.path, '%d.part' % i)
                    for i in range(self.npartitions)]

    def _open_files(self):
        if not self.isopen:
            self.isopen = True
            self.files = [self.open(fn, 'a') for fn in self.filenames]

    def _close_files(self):
        if self.isopen:
            for f in self.files:
                f.close()
            self.isopen = False

    def __enter__(self):
        self._open_files()
        return self

    def __exit__(self, dType, eValue, eTrace):
        self._close_files()

    def partition_of(self, item):
        return hash(self.grouper(item)) % self.npartitions

    def extend_chunk(self, seq):
        self._open_files()
        grouper = self.grouper
        npart = self.npartitions
        groups = groupby(grouper, seq)

        # Unify groups that hash the same
        groups2 = dict()
        for k, v in groups.items():
            key = hash(k) % self.npartitions
            if key not in groups2:
                groups2[key] = []
            groups2[key].extend(v)

        # Store to disk
        for k, group in groups2.items():
            if group:
                self.dump(group, self.files[k])

    def extend(self, seq, chunksize=100000):
        if isinstance(seq, Iterator):
            chunks = partition_all(chunksize, seq)
            for chunk in chunks:
                self.extend_chunk(chunk)
        else:
            self.extend_chunk(seq)

    def get_partition(self, i):
        self._close_files()
        with self.open(self.filenames[i]) as f:
            segments = []
            while True:
                try:
                    segments.append(self.load(f))
                except (EOFError, IOError):
                    break
        if not segments:
            return segments
        return sum(segments, type(segments[0])())

    def __del__(self):
        self._close_files()
        if not self._explicitly_given_path:
            self.drop()

    def drop(self):
        shutil.rmtree(self.path)


def partition_all(n, seq):
    """ Take chunks from the sequence, n elements at a time

    >>> parts = partition_all(3, [1, 2, 3, 4, 5, 6, 7, 8])
    >>> for part in parts:
    ...     print(tuple(part))
    (1, 2, 3)
    (4, 5, 6)
    (7, 8)

    The results are themselves lazy and so must be evaluated entirely before
    the next block is requested
    """
    seq = iter(seq)
    stop, seq = isempty(seq)
    while not stop:
        yield take(n, seq)
        stop, seq = isempty(seq)


def isempty(seq):
    """ Is the sequence empty?

    >>> seq = iter([1, 2, 3])
    >>> empty, seq = isempty(seq)
    >>> empty
    False

    >>> list(seq)  # seq is preserved
    [1, 2, 3]

    >>> seq = iter([])
    >>> empty, seq = isempty(seq)
    >>> empty
    True
    """
    try:
        first = next(seq)
        return False, concat([[first], seq])
    except StopIteration:
        return True, False
