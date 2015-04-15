from dask.array.optimization import (getitem, rewrite_rules, optimize,
        remove_full_slices, fuse_slice)
from dask.utils import raises
from dask.array.core import getarray


def test_fuse_getitem():
    pairs = [((getarray, (getarray, 'x', slice(1000, 2000)), slice(15, 20)),
              (getarray, 'x', slice(1015, 1020))),

             ((getitem, (getarray, 'x', (slice(1000, 2000), slice(100, 200))),
                        (slice(15, 20), slice(50, 60))),
              (getarray, 'x', (slice(1015, 1020), slice(150, 160)))),

             ((getarray, (getarray, 'x', slice(1000, 2000)), 10),
              (getarray, 'x', 1010)),

             ((getitem, (getarray, 'x', (slice(1000, 2000), 10)),
                        (slice(15, 20),)),
              (getarray, 'x', (slice(1015, 1020), 10))),

             ((getarray, (getarray, 'x', (10, slice(1000, 2000))),
                        (slice(15, 20),)),
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
              (getitem, 'x', (slice(1005, 1010), slice(10, 20))))

        ]

    for inp, expected in pairs:
        result = rewrite_rules.rewrite(inp)
        assert result == expected


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
           'd': (getarray, 'c', (slice(None, 5, None),)),
           'e': (getarray, 'd', (slice(None, None, None),))}

    expected = {'a': (range, 10),
                'e': (getarray, 'a', (slice(None, 5, None),))}

    assert remove_full_slices(dsk) == expected


def test_fuse_slice():
    assert fuse_slice(slice(10, 15), slice(0, 5, 2)) == slice(10, 15, 2)

    assert fuse_slice((slice(100, 200),), (None, slice(10, 20))) == \
            (None, slice(110, 120))
    assert fuse_slice((slice(100, 200),), (slice(10, 20), None)) == \
            (slice(110, 120), None)
    assert fuse_slice((1,), (None,)) == \
            (1, None)
    assert fuse_slice((1, slice(10, 20)), (None, None, 3, None)) == \
            (1, None, None, 13, None)

    assert (raises(NotImplementedError,
                  lambda: fuse_slice(slice(10, 15, 2), -1)) or
            fuse_slice(slice(10, 15, 2), -1) == 14)


def test_fuse_slice_with_lists():
    assert fuse_slice(slice(10, 20, 2), [1, 2, 3]) == [12, 14, 16]
    assert fuse_slice([10, 20, 30, 40, 50], [3, 1, 2]) == [40, 20, 30]
    assert fuse_slice([10, 20, 30, 40, 50], 3) == 40
    assert fuse_slice([10, 20, 30, 40, 50], -1) == 50
    assert fuse_slice([10, 20, 30, 40, 50], slice(1, None, 2)) == [20, 40]


def test_hard_fuse_slice_cases():
    term = (getarray, (getarray, 'x', (None, slice(None, None))),
                     (slice(None, None), 5))
    assert rewrite_rules.rewrite(term) == (getarray, 'x', (None, 5))
