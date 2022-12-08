import importlib.metadata
import os

import pytest
from packaging.version import Version


@pytest.mark.skipif(
    not os.environ.get("UPSTREAM_DEV", False),
    reason="Only check for dev packages in `upstream` CI build",
)
def test_upstream_packages_installed():
    packages = [
        "bokeh",
        "dask",
        "distributed",
        "fastparquet",
        "fsspec",
        "numpy",
        "pandas",
        "partd",
        "pyarrow",
        "s3fs",
        "scipy",
        "sparse",
        "zarr",
        "zict",
    ]
    for package in packages:
        version = importlib.metadata.version(package)
        assert Version(version).is_prerelease, (package, version)
