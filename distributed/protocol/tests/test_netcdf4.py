import pytest

netCDF4 = pytest.importorskip('netCDF4')
np = pytest.importorskip('numpy')

from distributed.protocol import serialize, deserialize, dumps, loads

from distributed.utils import tmpfile


def create_test_dataset(fn):
    with netCDF4.Dataset(fn, mode='w') as ds:
        ds.createDimension('x', 3)
        v = ds.createVariable('x', np.int32, ('x',))
        v[:] = np.arange(3)

        g = ds.createGroup('group')
        g2 = ds.createGroup('group/group1')

        v2 = ds.createVariable('group/y', np.int32, ('x',))
        v2[:] = np.arange(3) + 1

        v3 = ds.createVariable('group/group1/z', np.int32, ('x',))
        v3[:] = np.arange(3) + 2


def test_serialize_deserialize_dataset():
    with tmpfile() as fn:
        create_test_dataset(fn)
        with netCDF4.Dataset(fn, mode='r') as f:
            g = deserialize(*serialize(f))
            assert f.filepath() == g.filepath()
            assert isinstance(g, netCDF4.Dataset)

            assert g.variables['x'].dimensions == ('x',)
            assert g.variables['x'].dtype == np.int32
            assert (g.variables['x'][:] == np.arange(3)).all()


def test_serialize_deserialize_variable():
    with tmpfile() as fn:
        create_test_dataset(fn)
        with netCDF4.Dataset(fn, mode='r') as f:
            x = f.variables['x']
            y = deserialize(*serialize(x))
            assert isinstance(y, netCDF4.Variable)
            assert y.dimensions == ('x',)
            assert (x.dtype == y.dtype)
            assert (x[:] == y[:]).all()


def test_serialize_deserialize_group():
    with tmpfile() as fn:
        create_test_dataset(fn)
        with netCDF4.Dataset(fn, mode='r') as f:
            for path in ['group', 'group/group1']:
                g = f[path]
                h = deserialize(*serialize(g))
                assert isinstance(h, netCDF4.Group)
                assert h.name == g.name
                assert list(g.groups) == list(h.groups)
                assert list(g.variables) == list(h.variables)

            vars = [f.variables['x'],
                    f['group'].variables['y'],
                    f['group/group1'].variables['z']]

            for x in vars:
                y = deserialize(*serialize(x))
                assert isinstance(y, netCDF4.Variable)
                assert y.dimensions == ('x',)
                assert (x.dtype == y.dtype)
                assert (x[:] == y[:]).all()


from distributed.utils_test import gen_cluster
from distributed.client import _wait

from tornado import gen

import dask.array as da


@gen_cluster(client=True)
def test_netcdf4_serialize(c, s, a, b):
    with tmpfile() as fn:
        create_test_dataset(fn)
        with netCDF4.Dataset(fn, mode='r') as f:
            dset = f.variables['x']
            x = da.from_array(dset, chunks=2)
            y = c.compute(x)
            y = yield y
            assert (y[:] == dset[:]).all()
