import contextlib

import pytest


pd = pytest.importorskip("pandas")
import dask.dataframe as dd


@contextlib.contextmanager
def ensure_removed(obj, attr):
    """Ensure that an attribute added to 'obj' during the test is
    removed when we're done"""
    try:
        yield
    finally:
        try:
            delattr(obj, attr)
        except AttributeError:
            pass
        obj._accessors.discard(attr)


class MyAccessor:
    def __init__(self, obj):
        self.obj = obj
        self.item = "item"

    @property
    def prop(self):
        return self.item

    def method(self):
        return self.item


@pytest.mark.parametrize(
    "obj, registrar",
    [
        (dd.Series, dd.extensions.register_series_accessor),
        (dd.DataFrame, dd.extensions.register_dataframe_accessor),
        (dd.Index, dd.extensions.register_index_accessor),
    ],
)
def test_register(obj, registrar):
    with ensure_removed(obj, "mine"):
        before = set(dir(obj))
        registrar("mine")(MyAccessor)
        instance = dd.from_pandas(obj._partition_type([]), 2)
        assert instance.mine.prop == "item"
        after = set(dir(obj))
        assert (before ^ after) == {"mine"}
        assert "mine" in obj._accessors


def test_accessor_works():
    with ensure_removed(dd.Series, "mine"):
        dd.extensions.register_series_accessor("mine")(MyAccessor)

        a = pd.Series([1, 2])
        b = dd.from_pandas(a, 2)
        assert b.mine.obj is b

        assert b.mine.prop == "item"
        assert b.mine.method() == "item"
