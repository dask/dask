import os

import pytest


def pytest_ignore_collect(path, config):
    if os.path.split(str(path))[1].startswith("fft.py"):
        return True

pytest.register_assert_rewrite('dask.array.utils')
