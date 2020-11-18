from .core import tensordot_lookup, concatenate_lookup, einsum_lookup


@tensordot_lookup.register_lazy("cupy")
@concatenate_lookup.register_lazy("cupy")
def register_cupy():
    import cupy

    concatenate_lookup.register(cupy.ndarray, cupy.concatenate)
    tensordot_lookup.register(cupy.ndarray, cupy.tensordot)

    @einsum_lookup.register(cupy.ndarray)
    def _cupy_einsum(*args, **kwargs):
        # NB: cupy does not accept `order` or `casting` kwargs - ignore
        kwargs.pop("casting", None)
        kwargs.pop("order", None)
        return cupy.einsum(*args, **kwargs)


@tensordot_lookup.register_lazy("cupyx")
@concatenate_lookup.register_lazy("cupyx")
def register_cupyx():

    from cupyx.scipy.sparse import spmatrix

    try:
        from cupyx.scipy.sparse import hstack
        from cupyx.scipy.sparse import vstack
    except ImportError as e:
        raise ImportError(
            "Stacking of sparse arrays requires at least CuPy version 8.0.0"
        ) from e

    def _concat_cupy_sparse(L, axis=0):
        if axis == 0:
            return vstack(L)
        elif axis == 1:
            return hstack(L)
        else:
            msg = (
                "Can only concatenate cupy sparse matrices for axis in "
                "{0, 1}.  Got %s" % axis
            )
            raise ValueError(msg)

    concatenate_lookup.register(spmatrix, _concat_cupy_sparse)
    tensordot_lookup.register(spmatrix, _tensordot_scipy_sparse)


@tensordot_lookup.register_lazy("sparse")
@concatenate_lookup.register_lazy("sparse")
def register_sparse():
    import sparse

    concatenate_lookup.register(sparse.COO, sparse.concatenate)
    tensordot_lookup.register(sparse.COO, sparse.tensordot)


@tensordot_lookup.register_lazy("scipy")
@concatenate_lookup.register_lazy("scipy")
def register_scipy_sparse():
    import scipy.sparse

    def _concatenate(L, axis=0):
        if axis == 0:
            return scipy.sparse.vstack(L)
        elif axis == 1:
            return scipy.sparse.hstack(L)
        else:
            msg = (
                "Can only concatenate scipy sparse matrices for axis in "
                "{0, 1}.  Got %s" % axis
            )
            raise ValueError(msg)

    concatenate_lookup.register(scipy.sparse.spmatrix, _concatenate)
    tensordot_lookup.register(scipy.sparse.spmatrix, _tensordot_scipy_sparse)


def _tensordot_scipy_sparse(a, b, axes):
    assert a.ndim == b.ndim == 2
    assert len(axes[0]) == len(axes[1]) == 1
    (a_axis,) = axes[0]
    (b_axis,) = axes[1]
    assert a_axis in (0, 1) and b_axis in (0, 1)
    assert a.shape[a_axis] == b.shape[b_axis]
    if a_axis == 0 and b_axis == 0:
        return a.T * b
    elif a_axis == 0 and b_axis == 1:
        return a.T * b.T
    elif a_axis == 1 and b_axis == 0:
        return a * b
    elif a_axis == 1 and b_axis == 1:
        return a * b.T
