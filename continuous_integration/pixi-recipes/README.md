# Hacked Dask dependencies for pixi
These pixi recipes are invoked by pixi (pixi.toml in the root directory)
to hack around limitations of the upstream packages.

## Recipes used by the `nightly` environment
### fsspec
Builds a conda package from git tip.
We can't just use::

    [pypi-dependencies]
    fsspec = { git = "https://github.com/fsspec/filesystem_spec" }

because fsspec is a dependency of conda package dask-core (defined in pixi.toml), and
conda packages can't depend on pip packages.
A clean solution would be to push `fsspec/pixi.toml` upstream, but currently it would be
broken due to poor pixi-build support for SCM versioning
(https://github.com/prefix-dev/pixi/issues/2923)

### s3fs
Builds a conda package from git tip.
We can't just use::

    [pypi-dependencies]
    s3fs = { git = "https://github.com/fsspec/s3fs" }

again, due to lack of pixi-build support for SCM versioning in fsspec; the s3fs project
pins the fsspec version which fails to match the current fsspec recipe. This s3fs recipe
works around the problem by removing the fsspec pin.

## Recipes used by the `py314t` environment
### brotli
Creates non-importable conda-package `brotli-python=1.2.0`.
brotli is an optional dependency of urllib3, which is in turn a dependency of requests,
which is in turn an optional dependency of fsspec.
As of 1.2.0, brotli does not release the GIL on import in Python 3.14t.
This causes `import requests` to lock the GIL.

### msgpack
Pure-python variant of conda package `msgpack-python=1.1.2`, which in conda-forge
is compiled with the C extensions enabled and does not release the GIL.

**Note:** this is expected to substantially degrade dask.distributed performance. For
benchmarks you should use the conda-forge version with the `PYTHON_GIL=0` environment
variable set.
