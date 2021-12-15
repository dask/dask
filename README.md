<p align="center"><a href="#"><img width=60% alt="" src="https://docs.dask.org/en/latest/_static/images/dask-horizontal-white.svg"></a>

<h2 align="center">A flexible parallel computing library for analytics
</h2>

<div align="center">

---

[![BuildStatus](https://github.com/dask/dask/workflows/CI/badge.svg?branch=main)](https://github.com/dask/dask/actions?query=workflow%3A%22CI%22)
[![Coverage status](https://codecov.io/gh/dask/dask/branch/main/graph/badge.svg)](https://codecov.io/gh/dask/dask/branch/main)
[![Documentation Status](https://readthedocs.org/projects/dask/badge/?version=latest)](https://dask.org)
[![Discuss Dask-related things and ask for help](https://img.shields.io/discourse/users?logo=discourse&server=https%3A%2F%2Fdask.discourse.group)](https://dask.discourse.group)
[![Version
Status](https://img.shields.io/pypi/v/dask.svg)](https://pypi.python.org/pypi/dask/)
[![NumFOCUS](https://img.shields.io/badge/powered%20by-NumFOCUS-orange.svg?style=flat&colorA=E1523D&colorB=007D8A)](https://www.numfocus.org/)
</div>

See [documentation](https://dask.org) for more information.



Install
============
Simple installation from Conda

``` bash
conda install dask
```
This installs Dask and all common dependencies, including Pandas and NumPy. Dask packages are maintained both on the default channel and on conda-forge.

you can obtain a minimal Dask installation using the following command:
``` bash
conda install dask-core
```

<details>
  <summary>Other installation options</summary>

# Install with Pip

You can install everything required for most common uses of Dask (arrays, dataframes, …) This installs both Dask and dependencies like NumPy, Pandas, and so on that are necessary for different workloads. This is often the right choice for Dask users:

``` bash
python -m pip install "dask[complete]"    # Install everything
```

You can also install only the Dask library. Modules like `dask.array`, `dask.dataframe`, or `dask.distributed` won’t work until you also install NumPy, Pandas, or Tornado, respectively. This is common for downstream library maintainers:

``` bash 
python -m pip install dask                # Install only core parts of dask
```

We also maintain other dependency sets for different subsets of functionality:

``` bash
python -m pip install "dask[array]"       # Install requirements for dask array
python -m pip install "dask[dataframe]"   # Install requirements for dask dataframe
python -m pip install "dask[diagnostics]" # Install requirements for dask diagnostics
python -m pip install "dask[distributed]" # Install requirements for distributed dask

```

We have these options so that users of the lightweight core Dask scheduler aren’t required to download the more exotic dependencies of the collections (Numpy, Pandas, Tornado, etc.).

# Install from Source

To install Dask from source, clone the repository from github:

``` bash 
git clone https://github.com/dask/dask.git
cd dask
python -m pip install .
```
You can also install all dependencies as well:

``` bash
python -m pip install ".[complete]"
```

You can view the list of all dependencies within the `extras_require` field of setup.py.

Or do a developer install by using the `-e` flag:

``` bash
python -m pip install -e .
```


</details>

For more details about install dask , see [installation page](https://docs.dask.org/en/stable/install.html).



Contribution guidelines
============ 

If you want to contribute to Dask, be sure to review the
[contribution guidelines](CONTRIBUTING.md).


LICENSE
=======

New BSD. See [LicenseFile](https://github.com/dask/dask/blob/main/LICENSE.txt).
