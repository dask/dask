from __future__ import absolute_import, division, print_function
import itertools
from operator import add
import numpy as np
from dask.utils import ignoring
from toolz import accumulate
def cumdims(blockdims):
    return [list(accumulate(add, (0,) + bds)) for bds in blockdims]

def intersect_blockdims(old, new):
    oc, nc = map(cumdims, (old, new))
    new_cum = tuple(tuple(zip((0,) * len(nci),  nci,(0,) + nw,  tuple(range(len(nw))))) for nci, nw in zip(nc, new))
    old_cum = tuple(tuple(zip((1,) * len(oci),  oci,(0,) + od, (None,) + tuple(range(len(od))))) for oci, od in zip(oc, old))
    breaks = tuple(sorted(oci + nci, key = lambda x: (x[1], x[0])) for oci, nci in zip(old_cum, new_cum))
   
    old_inds = [ [0] + [b[idx][1] - b[idx-1][1] for idx in range(1,len(b))] for b in breaks]
    inds = tuple(tuple( b + (o,) for b, o in zip(breaks[idx],old_inds[idx])) for idx in range(len(breaks)))
    final = []
    # clean up all these naming messes
    for ind in inds:
        old_idx = 0
        idx_new_old2 = 0
        ret = [[]]
        for (is_old, cumm, old_chunk, idx_new_old, length) in ind:
            if idx_new_old2 != idx_new_old:
                old_idx = 0
            if is_old:
                idx_new_old2 = idx_new_old
            if length:
                ret[-1].append((idx_new_old2, slice(old_idx, old_idx + length)))
            old_idx = old_idx + length
            if not is_old:
                ret.append([])
        final.append(tuple(filter(None, map(tuple, ret))))
    return tuple(final)
    
def reblock(dsk, old_to_new, block_id):
    getting = dict()
    dsk2 = dict()
    layer1 = map(itertools.product, itertools.product(*old_to_new))
    # do more stuff here...
    return layer1
if __name__ == "__main__":
    import pprint
    old = ((20,20,20),(10,)*2)
    new = ((15,)*2, (15,5))
    print('With old = ', old, 'new = ', new,'intersect_blockdims = ')
    i = intersect_blockdims(old, new)
    pprint.pprint(i)
    