from __future__ import absolute_import, division, print_function

import pytest
np = pytest.importorskip('numpy')

import dask.array as da


def test_ufunc_meta():
    assert da.log.__name__ == 'log'
    assert da.log.__doc__.replace('    # doctest: +SKIP', '') == np.log.__doc__
