Creating Dask arrays from HDF5 Datasets
=======================================

We can construct dask array objects from other array objects that support
numpy-style slicing.  In this example, we wrap a dask array around an HDF5 dataset,
chunking that dataset into blocks of size ``(1000, 1000)``:

.. code-block:: Python

   >>> import h5py
   >>> f = h5py.File('myfile.hdf5')
   >>> dset = f['/data/path']

   >>> import dask.array as da
   >>> x = da.from_array(dset, chunks=(1000, 1000))

Often we have many such datasets.  We can use the ``stack`` or ``concatenate``
functions to bind many dask arrays into one:

.. code-block:: Python

   >>> dsets = [h5py.File(fn)['/data'] for fn in sorted(glob('myfiles.*.hdf5')]
   >>> arrays = [da.from_array(dset, chunks=(1000, 1000)) for dset in dsets]

   >>> x = da.stack(arrays, axis=0)  # Stack along a new first axis

Note that none of the data is loaded into memory yet, the dask array just
contains a graph of tasks showing how to load the data. This allows
``dask.array`` to do work on datasets that don't fit into RAM.
