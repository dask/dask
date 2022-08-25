import os
import sys
from array import array

import pytest

from dask.multiprocessing import get_context
from dask.sizeof import sizeof
from dask.utils import funcname


def test_base():
    assert sizeof(1) == sys.getsizeof(1)


def test_name():
    assert funcname(sizeof) == "sizeof"


def test_containers():
    assert sizeof([1, 2, [3]]) > (sys.getsizeof(3) * 3 + sys.getsizeof([]))


def test_bytes_like():
    assert 1000 <= sizeof(bytes(1000)) <= 2000
    assert 1000 <= sizeof(bytearray(1000)) <= 2000
    assert 1000 <= sizeof(memoryview(bytes(1000))) <= 2000
    assert 8000 <= sizeof(array("d", range(1000))) <= 9000


def test_numpy():
    np = pytest.importorskip("numpy")
    assert 8000 <= sizeof(np.empty(1000, dtype="f8")) <= 9000
    dt = np.dtype("f8")
    assert sizeof(dt) == sys.getsizeof(dt)


def test_numpy_0_strided():
    np = pytest.importorskip("numpy")
    x = np.broadcast_to(1, (100, 100, 100))
    assert sizeof(x) <= 8


def test_pandas():
    pd = pytest.importorskip("pandas")
    df = pd.DataFrame(
        {"x": [1, 2, 3], "y": ["a" * 100, "b" * 100, "c" * 100]}, index=[10, 20, 30]
    )

    assert sizeof(df) >= sizeof(df.x) + sizeof(df.y) - sizeof(df.index)
    assert sizeof(df.x) >= sizeof(df.index)
    assert sizeof(df.y) >= 100 * 3
    assert sizeof(df.index) >= 20

    assert isinstance(sizeof(df), int)
    assert isinstance(sizeof(df.x), int)
    assert isinstance(sizeof(df.index), int)


def test_pandas_multiindex():
    pd = pytest.importorskip("pandas")
    index = pd.MultiIndex.from_product([range(5), ["a", "b", "c", "d", "e"]])
    actual_size = sys.getsizeof(index) + 1000  # adjust for serialization overhead

    assert 0.5 * actual_size < sizeof(index) < 2 * actual_size
    assert isinstance(sizeof(index), int)


def test_pandas_repeated_column():
    pd = pytest.importorskip("pandas")
    df = pd.DataFrame({"x": [1, 2, 3]})

    assert sizeof(df[["x", "x", "x"]]) > sizeof(df)


def test_sparse_matrix():
    sparse = pytest.importorskip("scipy.sparse")
    sp = sparse.eye(10)
    # These are the 32-bit Python 2.7 values.
    assert sizeof(sp.todia()) >= 152
    assert sizeof(sp.tobsr()) >= 232
    assert sizeof(sp.tocoo()) >= 240
    assert sizeof(sp.tocsc()) >= 232
    assert sizeof(sp.tocsr()) >= 232
    assert sizeof(sp.todok()) >= 192
    assert sizeof(sp.tolil()) >= 204


def test_serires_object_dtype():
    pd = pytest.importorskip("pandas")
    s = pd.Series(["a"] * 1000)
    assert sizeof("a") * 1000 < sizeof(s) < 2 * sizeof("a") * 1000

    s = pd.Series(["a" * 1000] * 1000)
    assert sizeof(s) > 1000000


def test_dataframe_object_dtype():
    pd = pytest.importorskip("pandas")
    df = pd.DataFrame({"x": ["a"] * 1000})
    assert sizeof("a") * 1000 < sizeof(df) < 2 * sizeof("a") * 1000

    s = pd.Series(["a" * 1000] * 1000)
    assert sizeof(s) > 1000000


def test_empty():
    pd = pytest.importorskip("pandas")
    df = pd.DataFrame(
        {"x": [1, 2, 3], "y": ["a" * 100, "b" * 100, "c" * 100]}, index=[10, 20, 30]
    )
    empty = df.head(0)

    assert sizeof(empty) > 0
    assert sizeof(empty.x) > 0
    assert sizeof(empty.y) > 0
    assert sizeof(empty.index) > 0


def test_pyarrow_table():
    pd = pytest.importorskip("pandas")
    pa = pytest.importorskip("pyarrow")
    df = pd.DataFrame(
        {"x": [1, 2, 3], "y": ["a" * 100, "b" * 100, "c" * 100]}, index=[10, 20, 30]
    )
    table = pa.Table.from_pandas(df)

    assert sizeof(table) > sizeof(table.schema.metadata)
    assert isinstance(sizeof(table), int)
    assert isinstance(sizeof(table.columns[0]), int)
    assert isinstance(sizeof(table.columns[1]), int)
    assert isinstance(sizeof(table.columns[2]), int)

    empty = pa.Table.from_pandas(df.head(0))

    assert sizeof(empty) > sizeof(empty.schema.metadata)
    assert sizeof(empty.columns[0]) > 0
    assert sizeof(empty.columns[1]) > 0
    assert sizeof(empty.columns[2]) > 0


def test_dict():
    np = pytest.importorskip("numpy")
    x = np.ones(10000)
    assert sizeof({"x": x}) > x.nbytes
    assert sizeof({"x": [x]}) > x.nbytes
    assert sizeof({"x": [{"y": x}]}) > x.nbytes

    d = {i: x for i in range(100)}
    assert sizeof(d) > x.nbytes * 100
    assert isinstance(sizeof(d), int)


def _get_sizeof_on_path(path, size):
    sys.path.append(os.fsdecode(path))

    # Dask will have already called _register_entry_point_plugins
    # before we can modify sys.path, so we re-register here.
    import dask.sizeof

    dask.sizeof._register_entry_point_plugins()

    import class_impl

    cls = class_impl.Impl(size)
    return sizeof(cls)


def test_register_backend_entrypoint(tmp_path):
    # Create special sizeof implementation for a dummy class
    (tmp_path / "impl_sizeof.py").write_bytes(
        b"def sizeof_plugin(sizeof):\n"
        b'    print("REG")\n'
        b'    @sizeof.register_lazy("class_impl")\n'
        b"    def register_impl():\n"
        b"        import class_impl\n"
        b"        @sizeof.register(class_impl.Impl)\n"
        b"        def sizeof_impl(obj):\n"
        b"            return obj.size \n"
    )
    # Define dummy class that possesses a size attribute
    (tmp_path / "class_impl.py").write_bytes(
        b"class Impl:\n    def __init__(self, size):\n        self.size = size"
    )
    dist_info = tmp_path / "impl_sizeof-0.0.0.dist-info"
    dist_info.mkdir()
    (dist_info / "entry_points.txt").write_bytes(
        b"[dask.sizeof]\nimpl = impl_sizeof:sizeof_plugin\n"
    )

    with get_context().Pool(1) as pool:
        assert (
            pool.apply(_get_sizeof_on_path, args=(tmp_path, 3_14159265)) == 3_14159265
        )
    pool.join()
