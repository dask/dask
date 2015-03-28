from dask.array.optimization import (getitem, rewrite_rules, optimize,
        remove_full_slices, fuse_slice)


def test_fuse_getitem():
    pairs = [((getitem, (getitem, 'x', slice(1000, 2000)), slice(15, 20)),
              (getitem, 'x', slice(1015, 1020))),

             ((getitem, (getitem, 'x', (slice(1000, 2000), slice(100, 200))),
                        (slice(15, 20), slice(50, 60))),
              (getitem, 'x', (slice(1015, 1020), slice(150, 160)))),

             ((getitem, (getitem, 'x', slice(1000, 2000)), 10),
              (getitem, 'x', 1010)),

             ((getitem, (getitem, 'x', (slice(1000, 2000), 10)),
                        (slice(15, 20),)),
              (getitem, 'x', (slice(1015, 1020), 10))),

             ((getitem, (getitem, 'x', (10, slice(1000, 2000))),
                        (slice(15, 20),)),
              (getitem, 'x', (10, slice(1015, 1020)))),

             ((getitem, (getitem, 'x', (slice(1000, 2000), slice(100, 200))),
                        (slice(None, None), slice(50, 60))),
              (getitem, 'x', (slice(1000, 2000), slice(150, 160)))),

             ((getitem, (getitem, 'x', (None, slice(None, None))),
                        (slice(None, None), 5)),
              (getitem, 'x', (None, 5))),

        ]

    for inp, expected in pairs:
        result = rewrite_rules.rewrite(inp)
        assert result == expected


def test_optimize_with_getitem_fusion():
    dsk = {'a': 'some-array',
           'b': (getitem, 'a', (slice(10, 20), slice(100, 200))),
           'c': (getitem, 'b', (5, slice(50, 60)))}

    result = optimize(dsk, ['c'])
    expected = {'c': (getitem, 'some-array', (15, slice(150, 160)))}
    assert result == expected


def test_optimize_slicing():
    dsk = {'a': (range, 10),
           'b': (getitem, 'a', (slice(None, None, None),)),
           'c': (getitem, 'b', (slice(None, None, None),)),
           'd': (getitem, 'c', (slice(None, 5, None),)),
           'e': (getitem, 'd', (slice(None, None, None),))}

    expected = {'a': (range, 10),
                'e': (getitem, 'a', (slice(None, 5, None),))}

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


def test_hard_fuse_slice_cases():
    term = (getitem, (getitem, 'x', (None, slice(None, None))),
                     (slice(None, None), 5))
    assert rewrite_rules.rewrite(term) == (getitem, 'x', (None, 5))
