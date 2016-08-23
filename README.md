[![Travis](https://img.shields.io/travis/dask/dask.svg?maxAge=2592000)](https://travis-ci.org/dask/dask)
[![Coveralls](https://img.shields.io/coveralls/dask/dask.svg?maxAge=2592000)](https://coveralls.io/r/dask/dask)
[![Docs](http://readthedocs.org/projects/dask/badge/?version=latest)](http://dask.pydata.org/en/latest/)
[![Gitter](https://img.shields.io/gitter/room/dask/dask.js.svg?maxAge=2592000)](https://gitter.im/dask/dask?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![PyPI](https://img.shields.io/pypi/v/dask.svg?maxAge=2592000)](https://pypi.python.org/pypi/dask/)
[![Dask Examples](http://mybinder.org/badge.svg)](http://mybinder.org/repo/dask/dask-examples)

# Dask

Dask is a flexible parallel computing library for analytics. Dask emphasizes the following virtues:

* **Familiar**: Provides parallelized NumPy array and Pandas DataFrame objects
* **Native**: Enables distributed computing in Pure Python with access to the PyData stack.
* **Fast**: Operates with low overhead, low latency, and minimal serialization necessary for fast numerical algorithms
* **Flexible**: Supports complex and messy workloads
* **Scales up**: Runs resiliently on clusters with 100s of nodes
* **Scales down**: Trivial to set up and run on a laptop in a single process
* **Responsive**: Designed with interactive computing in mind it provides rapid feedback and diagnostics to aid humans

# Installation
#### Through pip:
To install Dask with pip there are a few options, depending on which dependencies you would like to keep up to date:
* Install only dask, which depends only on the standard
```shell
pip install dask
```
* Install dask and numpy
```shell
pip install dask[array]
```
* Install dask and cloudpickle
```shell
pip install dask[bag]
```
* Install dask, numpy, and pandas
```shell
pip install dask[dataframe]
```

#### Through conda:
```shell
conda install dask -c conda-forge
```

# Examples
More in-depth examples can be found in the [documentation](http://dask.pydata.org/en/latest/examples-tutorials.html)

Dask DataFrame mimics Pandas
```python
import pandas as pd                     import dask.dataframe as dd
df = pd.read_csv('2015-01-01.csv')      df = dd.read_csv('2015-*-*.csv')
df.groupby(df.user_id).value.mean()     df.groupby(df.user_id).value.mean().compute()
```
Dask Array mimics NumPy
```python
import numpy as np                       import dask.array as da
f = h5py.File('myfile.hdf5')             f = h5py.File('myfile.hdf5')
x = np.array(f['/small-data'])           x = da.from_array(f['/big-data'],
                                                           chunks=(1000, 1000))
x - x.mean(axis=1)                       x - x.mean(axis=1).compute()
```
Dask Bag mimics iterators, Toolz, PySpark
```python
import dask.bag as db
b = db.read_text('2015-*-*.json.gz').map(json.loads)
b.pluck('name').frequencies().topk(10, lambda pair: pair[1]).compute()
```
Dask Delayed mimics for loops and wraps custom code
```python
from dask import delayed
L = []
for fn in filenames:                  # Use for loops to build up computation
    data = delayed(load)(fn)          # Delay execution of function
    L.append(delayed(process)(data))  # Build connections between variables

result = delayed(summarize)(L)
result.compute()
```
