import pytest
pytest.importorskip('numpy')

from dask.optimize import fuse
from dask.array.optimization import (getitem, optimize, optimize_slices,
                                     fuse_slice)

from dask.array.core import getarray, getarray_nofancy


def test_fuse_getitem():
    pairs = [((getarray, (getarray, 'x', slice(1000, 2000)), slice(15, 20)),
              (getarray, 'x', slice(1015, 1020))),

             ((getitem, (getarray, 'x', (slice(1000, 2000), slice(100, 200))),
                        (slice(15, 20), slice(50, 60))),
              (getarray, 'x', (slice(1015, 1020), slice(150, 160)))),

             ((getitem, (getarray_nofancy, 'x', (slice(1000, 2000), slice(100, 200))),
                        (slice(15, 20), slice(50, 60))),
              (getarray_nofancy, 'x', (slice(1015, 1020), slice(150, 160)))),

             ((getarray, (getarray, 'x', slice(1000, 2000)), 10),
              (getarray, 'x', 1010)),

             ((getitem, (getarray, 'x', (slice(1000, 2000), 10)),
                        (slice(15, 20),)),
              (getarray, 'x', (slice(1015, 1020), 10))),

             ((getitem, (getarray_nofancy, 'x', (slice(1000, 2000), 10)),
                        (slice(15, 20),)),
              (getarray_nofancy, 'x', (slice(1015, 1020), 10))),

             ((getarray, (getarray, 'x', (10, slice(1000, 2000))),
               (slice(15, 20), )),
              (getarray, 'x', (10, slice(1015, 1020)))),

             ((getarray, (getarray, 'x', (slice(1000, 2000), slice(100, 200))),
               (slice(None, None), slice(50, 60))),
              (getarray, 'x', (slice(1000, 2000), slice(150, 160)))),

             ((getarray, (getarray, 'x', (None, slice(None, None))),
               (slice(None, None), 5)),
              (getarray, 'x', (None, 5))),

             ((getarray, (getarray, 'x', (slice(1000, 2000), slice(10, 20))),
               (slice(5, 10),)),
              (getarray, 'x', (slice(1005, 1010), slice(10, 20)))),

             ((getitem, (getitem, 'x', (slice(1000, 2000),)),
               (slice(5, 10), slice(10, 20))),
              (getitem, 'x', (slice(1005, 1010), slice(10, 20))))]

    for inp, expected in pairs:
        result = optimize_slices({'y': inp})
        assert result == {'y': expected}


def test_optimize_with_getitem_fusion():
    dsk = {'a': 'some-array',
           'b': (getarray, 'a', (slice(10, 20), slice(100, 200))),
           'c': (getarray, 'b', (5, slice(50, 60)))}

    result = optimize(dsk, ['c'])
    expected = {'c': (getarray, 'some-array', (15, slice(150, 160)))}
    assert result == expected


def test_optimize_slicing():
    dsk = {'a': (range, 10),
           'b': (getarray, 'a', (slice(None, None, None),)),
           'c': (getarray, 'b', (slice(None, None, None),)),
           'd': (getarray, 'c', (slice(0, 5, None),)),
           'e': (getarray, 'd', (slice(None, None, None),))}

    expected = {'e': (getarray, (range, 10), (slice(0, 5, None),))}
    result = optimize_slices(fuse(dsk, [])[0])
    assert result == expected

    # protect output keys
    expected = {'c': (getarray, (range, 10), (slice(0, None, None),)),
                'd': (getarray, 'c', (slice(0, 5, None),)),
                'e': (getarray, 'd', (slice(None, None, None),))}
    result = optimize_slices(fuse(dsk, ['c', 'd', 'e'])[0])

    assert result == expected


def test_fuse_slice():
    assert fuse_slice(slice(10, 15), slice(0, 5, 2)) == slice(10, 15, 2)

    assert (fuse_slice((slice(100, 200),), (None, slice(10, 20))) ==
            (None, slice(110, 120)))
    assert (fuse_slice((slice(100, 200),), (slice(10, 20), None)) ==
            (slice(110, 120), None))
    assert (fuse_slice((1,), (None,)) ==
            (1, None))
    assert (fuse_slice((1, slice(10, 20)), (None, None, 3, None)) ==
            (1, None, None, 13, None))

    with pytest.raises(NotImplementedError):
        fuse_slice(slice(10, 15, 2), -1)


def test_fuse_slice_with_lists():
    assert fuse_slice(slice(10, 20, 2), [1, 2, 3]) == [12, 14, 16]
    assert fuse_slice([10, 20, 30, 40, 50], [3, 1, 2]) == [40, 20, 30]
    assert fuse_slice([10, 20, 30, 40, 50], 3) == 40
    assert fuse_slice([10, 20, 30, 40, 50], -1) == 50
    assert fuse_slice([10, 20, 30, 40, 50], slice(1, None, 2)) == [20, 40]


def test_hard_fuse_slice_cases():
    dsk = {'x': (getarray, (getarray, 'x', (None, slice(None, None))),
                 (slice(None, None), 5))}
    assert optimize_slices(dsk) == {'x': (getarray, 'x', (None, 5))}


def test_dont_fuse_fancy_indexing_in_getarray_nofancy():
    dsk = {'a': (getitem, (getarray_nofancy, 'x', (slice(10, 20, None), slice(100, 200, None))),
                 ([1, 3], slice(50, 60, None)))}
    assert optimize_slices(dsk) == dsk

    dsk = {'a': (getitem, (getarray_nofancy, 'x', [1, 2, 3]), 0)}
    assert optimize_slices(dsk) == dsk
