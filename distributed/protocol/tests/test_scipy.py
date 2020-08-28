import pytest
from distributed.protocol import deserialize, serialize

numpy = pytest.importorskip("numpy")
scipy = pytest.importorskip("scipy")
scipy_sparse = pytest.importorskip("scipy.sparse")


@pytest.mark.parametrize(
    "sparse_type",
    [
        scipy_sparse.bsr_matrix,
        scipy_sparse.coo_matrix,
        scipy_sparse.csc_matrix,
        scipy_sparse.csr_matrix,
        scipy_sparse.dia_matrix,
        scipy_sparse.dok_matrix,
        scipy_sparse.lil_matrix,
    ],
)
@pytest.mark.parametrize(
    "dtype",
    [numpy.dtype("<f4"), numpy.dtype(">f4"), numpy.dtype("<f8"), numpy.dtype(">f8")],
)
def test_serialize_scipy_sparse(sparse_type, dtype):
    a = numpy.array([[0, 1, 0], [2, 0, 3], [0, 4, 0]], dtype=dtype)

    anz = a.nonzero()
    acoo = scipy_sparse.coo_matrix((a[anz], anz))
    asp = sparse_type(acoo)

    header, frames = serialize(asp, serializers=["dask"])
    asp2 = deserialize(header, frames)

    a2 = asp2.todense()

    assert (a == a2).all()
