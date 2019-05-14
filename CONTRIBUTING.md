For more information, see https://docs.dask.org/en/latest/develop.html#contributing-to-code


## Style
Distributed conforms with the [flake8] and [black] styles. To make sure your
code conforms with these styles, run

``` shell
$ pip install black flake8
$ cd path/to/distributed
$ black distributed
$ flake8 distributed
```

[flake8]:http://flake8.pycqa.org/en/latest/
[black]:https://github.com/python/black

## Docstrings

Dask Distributed roughly follows the [numpydoc] standard. More information is
available at https://docs.dask.org/en/latest/develop.html#docstrings.

[numpydoc]:https://github.com/numpy/numpy/blob/master/doc/HOWTO_DOCUMENT.rst.txt

## Tests

Dask employs extensive unit tests to ensure correctness of code both for today
and for the future. Test coverage is expected for all code contributions. More
detail is at https://docs.dask.org/en/latest/develop.html#test
