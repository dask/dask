from .core import tensordot_lookup, concatenate_lookup, einsum_lookup
from .core import ones_like_lookup, todense_lookup


@tensordot_lookup.register_lazy('cupy')
@concatenate_lookup.register_lazy('cupy')
@todense_lookup.register_lazy('cupy')
def register_cupy():
    import cupy
    concatenate_lookup.register(cupy.ndarray, cupy.concatenate)
    tensordot_lookup.register(cupy.ndarray, cupy.tensordot)
    todense_lookup.register(cupy.ndarray, cupy.ndarray.view)

    @einsum_lookup.register(cupy.ndarray)
    def _cupy_einsum(*args, **kwargs):
        # NB: cupy does not accept `order` or `casting` kwargs - ignore
        kwargs.pop('casting', None)
        kwargs.pop('order', None)
        return cupy.einsum(*args, **kwargs)


@tensordot_lookup.register_lazy('sparse')
@concatenate_lookup.register_lazy('sparse')
@ones_like_lookup.register_lazy('sparse')
@todense_lookup.register_lazy('sparse')
def register_sparse():
    import sparse
    concatenate_lookup.register(sparse.COO, sparse.concatenate)
    tensordot_lookup.register(sparse.COO, sparse.tensordot)
    ones_like_lookup.register(sparse.COO, sparse.ones_like)
    todense_lookup.register(sparse.COO, sparse.COO.todense)


@concatenate_lookup.register_lazy('scipy')
def register_scipy_sparse():
    import scipy.sparse

    def _concatenate(L, axis=0):
        if axis == 0:
            return scipy.sparse.vstack(L)
        elif axis == 1:
            return scipy.sparse.hstack(L)
        else:
            msg = ("Can only concatenate scipy sparse matrices for axis in "
                   "{0, 1}.  Got %s" % axis)
            raise ValueError(msg)
    concatenate_lookup.register(scipy.sparse.spmatrix, _concatenate)
