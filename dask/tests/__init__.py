import pytest

import dask

xfail_with_pyarrow_strings = pytest.mark.xfail(
    bool(dask.config.get("dataframe.convert_string")),
    reason="Known failure with pyarrow strings",
)
