import pytest

import dask


def test_PY_VERSION_deprecated():

    with pytest.warns(FutureWarning, match="dask.compatibility._PY_VERSION"):
        from dask.compatibility import PY_VERSION
    assert PY_VERSION is dask.compatibility._PY_VERSION
