import pytest

import dask


def test_PY_VERSION_deprecated():

    with pytest.warns(FutureWarning, match="removed in a future release"):
        from dask.compatibility import PY_VERSION
    assert PY_VERSION is dask.compatibility._PY_VERSION
