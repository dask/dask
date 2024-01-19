from __future__ import annotations

import os
import sys

PKG = os.path.dirname(os.path.dirname(__file__))


def test_dataframe(query_planning="True") -> None:
    import os

    import pytest

    cmd = PKG + "/dataframe"
    print(f"running: pytest {cmd}")
    os.environ["DASK_DATAFRAME__QUERY_PLANNING"] = query_planning
    sys.exit(pytest.main(["-n 4"] + [cmd]))


__all__ = ["test_dataframe"]
