import importlib

import pytest
from dask import config


def _backend_name() -> str:
    return config.get("dataframe.backend", "pandas")


def _backend_library():
    return importlib.import_module(_backend_name())


def xfail_gpu(reason=None):
    condition = _backend_name() == "cudf"
    reason = reason or "Failure expected for cudf backend."
    return pytest.mark.xfail(condition, reason=reason)
