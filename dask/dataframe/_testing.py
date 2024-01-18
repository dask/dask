from __future__ import annotations

import os
import sys

PKG = os.path.dirname(os.path.dirname(__file__))


def test_dataframe() -> None:
    import pytest

    cmd = PKG + "/dataframe"
    print(f"running: pytest {cmd}")
    sys.exit(pytest.main(["-n 4"] + [cmd]))


__all__ = ["test_dataframe"]
