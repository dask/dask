import pytest

import dask

xfail_with_pyarrow_strings = pytest.mark.xfail(
    bool(dask.config.get("dataframe.convert_string")),
    reason="Known failure with pyarrow strings",
)

skip_with_pyarrow_strings = pytest.mark.skipif(
    bool(dask.config.get("dataframe.convert_string")),
    reason="No need to run with pyarrow strings",
)
