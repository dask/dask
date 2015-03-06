from dask.bag.reblock import intersect_blockdims
# I have changed this file a lot so I'm not sure if these are 
# ok valid tests.
class TestIntersectBlockDims(object):
    def test_1(self):
        """ Example from Matthew's docs"""
        old=((10, 10, 10, 10, 10),)
        new = ((25, 5, 20), )
        answer = ((((0, slice(0, 10, None)), 
                    (1, slice(0, 10, None)), 
                    (2, slice(0, 5, None))), 
                    ((2, slice(5, 10, None)),),
                     ((3, slice(0, 10, None)), 
                        (4, slice(0, 10, None)))),)
        got = intersect_blockdims(old, new)
        assert answer == got, 'Failed on intersect_blockdims'
    def test_2(self):
        
        old = ((20, 20, 20, 20, 20), )
        new = ((58, 4, 20, 18),)
        answer = (
            ((0, slice(0, 20, None)), (2, slice(0, 20, None)), (1, slice(0, 18, None))),
            ((1, slice(0, 2, None)), (2, slice(0, 2, None))),
            ((2, slice(0, 18, None)), (3, slice(0, 2, None))))
        got = intersect_blockdims(old, new)
        assert answer == got, 'Failed on intersect_blockdims'

