Compatibility with numpy functions
==================================

The following table describes the compatibilities between `numpy` and `dask.array`
functions.
Please be aware that some inconsistencies with the two versions may exist.

This table has been compiled manually and may not reflect the current Dask state.
Update contributions are welcome. 

* A blank entry indicates that the function is not implemented in Dask.
* Direct implementation are direct calls to numpy functions.
* Element-wise implementations are derived from numpy but applied element-wise: the
  argument should be a dask array.
* Dask equivalent are Dask implementations, which may lack or add parameters with respect
  to the numpy function.

A more in-depth comparison in the framework of the `Array API <https://data-apis.org/array-api/latest/>`_
is available via the `Array API Comparison repository <https://github.com/data-apis/array-api-comparison>`_.

.. csv-table::
   :header: NumPy, Dask, Implementation

   :obj:`numpy.absolute`, :obj:`dask.array.absolute` or :obj:`dask.array.abs`, direct (ufunc)
   :obj:`numpy.add`, :obj:`dask.array.add`, direct (ufunc)
   :obj:`numpy.all`, :obj:`dask.array.all` [#1]_, dask equivalent
   :obj:`numpy.allclose`, :obj:`dask.array.allclose`, dask equivalent
   :obj:`numpy.amax`, :obj:`dask.array.max` [#1]_ [#2]_, dask equivalent
   :obj:`numpy.amin`, :obj:`dask.array.min` [#1]_ [#2]_, dask equivalent
   :obj:`numpy.angle`, :obj:`dask.array.angle` [#3]_, dask equivalent
   :obj:`numpy.any`, :obj:`dask.array.any` [#1]_, dask equivalent
   :obj:`numpy.append`, :obj:`dask.array.append`, dask equivalent
   :obj:`numpy.apply_along_axis`, :obj:`dask.array.apply_along_axis`, dask equivalent
   :obj:`numpy.apply_over_axes`, :obj:`dask.array.apply_over_axes`, dask equivalent
   :obj:`numpy.arange`, :obj:`dask.array.arange`, dask equivalent
   :obj:`numpy.arccos`, :obj:`dask.array.arccos`, direct (ufunc)
   :obj:`numpy.arccosh`, :obj:`dask.array.arccosh`, direct (ufunc)
   :obj:`numpy.arcsin`, :obj:`dask.array.arcsin`, direct (ufunc)
   :obj:`numpy.arcsinh`, :obj:`dask.array.arcsinh`, direct (ufunc)
   :obj:`numpy.arctan`, :obj:`dask.array.arctan`, direct (ufunc)
   :obj:`numpy.arctan2`, :obj:`dask.array.arctan2`, direct (ufunc)
   :obj:`numpy.arctanh`, :obj:`dask.array.arctanh`, direct (ufunc)
   :obj:`numpy.argmax`, :obj:`dask.array.argmax`, dask equivalent
   :obj:`numpy.argmin`, :obj:`dask.array.argmin`, dask equivalent
   :obj:`numpy.argpartition`, \-
   :obj:`numpy.argsort`, :obj:`dask.array.argtopk` [#5]_
   :obj:`numpy.argwhere`, :obj:`dask.array.argwhere`, dask equivalent
   :obj:`numpy.around`, :obj:`dask.array.around` [#3]_ [#6]_ or :obj:`dask.array.round`, dask equivalent
   :obj:`numpy.array`, :obj:`dask.array.array`, dask equivalent
   :obj:`numpy.array2string`, \-
   :obj:`numpy.array_equal`, \-
   :obj:`numpy.array_equiv`, \-
   :obj:`numpy.array_repr`, \-
   :obj:`numpy.array_split`, \-
   :obj:`numpy.array_str`, \-
   :obj:`numpy.asanyarray`, :obj:`dask.array.asanyarray`, dask equivalent
   :obj:`numpy.asarray`, :obj:`dask.array.asarray`, dask equivalent
   :obj:`numpy.asarray_chkfinite`, \-
   :obj:`numpy.ascontiguousarray`, \-
   :obj:`numpy.asfarray`, \-
   :obj:`numpy.asfortranarray`, \-
   :obj:`numpy.asmatrix`, \- [#7]_
   :obj:`numpy.atleast_1d`, :obj:`dask.array.atleast_1d`, dask equivalent
   :obj:`numpy.atleast_2d`, :obj:`dask.array.atleast_2d`, dask equivalent
   :obj:`numpy.atleast_3d`, :obj:`dask.array.atleast_3d`, dask equivalent
   :obj:`numpy.average`, :obj:`dask.array.average`, dask equivalent
   :obj:`numpy.bartlett`, \-
   :obj:`numpy.bincount`, :obj:`dask.array.bincount`, dask equivalent
   :obj:`numpy.bitwise_and`, :obj:`dask.array.bitwise_and`, direct (ufunc)
   :obj:`numpy.bitwise_or`, :obj:`dask.array.bitwise_or`, direct (ufunc)
   :obj:`numpy.bitwise_xor`, :obj:`dask.array.bitwise_xor`, direct (ufunc)
   :obj:`numpy.blackman`, \-
   :obj:`numpy.block`, :obj:`dask.array.block`, dask equivalent
   :obj:`numpy.bmat`, \- [#7]_
   :obj:`numpy.broadcast`, \-
   :obj:`numpy.broadcast_arrays`, :obj:`dask.array.broadcast_arrays`, dask equivalent
   :obj:`numpy.broadcast_shapes`, \-
   :obj:`numpy.broadcast_to`, :obj:`dask.array.broadcast_to`, dask equivalent
   :obj:`numpy.byte_bounds`, \-
   :obj:`numpy.c_`, \-
   :obj:`numpy.can_cast`, \-
   :obj:`numpy.cbrt`, :obj:`dask.array.cbrt`, direct (ufunc)
   :obj:`numpy.ceil`, :obj:`dask.array.ceil`, direct (ufunc)
   :obj:`numpy.choose`, :obj:`dask.array.choose` [#8]_, dask equivalent
   :obj:`numpy.clip`, :obj:`dask.array.clip` [#3]_ [#6]_, direct (non-ufunc elementwise)
   :obj:`numpy.column_stack`, \-
   :obj:`numpy.common_type`, \-
   :obj:`numpy.compress`, :obj:`dask.array.compress` [#6]_, dask equivalent
   :obj:`numpy.concatenate`, :obj:`dask.array.concatenate`, dask equivalent
   :obj:`numpy.conj`, :obj:`dask.array.conj`, direct (ufunc)
   :obj:`numpy.conjugate`,  :obj:`dask.array.conj`, direct (ufunc)
   :obj:`numpy.convolve`, \-
   :obj:`numpy.copy`, \-
   :obj:`numpy.copysign`, :obj:`dask.array.copysign`, direct (ufunc)
   :obj:`numpy.copyto`, \-
   :obj:`numpy.corrcoef`, :obj:`dask.array.corrcoef`, dask equivalent
   :obj:`numpy.correlate`, \-
   :obj:`numpy.cos`, :obj:`dask.array.cos`, direct (ufunc)
   :obj:`numpy.cosh`, :obj:`dask.array.cosh`, direct (ufunc)
   :obj:`numpy.count_nonzero`, :obj:`dask.array.count_nonzero` [#9]_, dask equivalent
   :obj:`numpy.cov`, :obj:`dask.array.cov` [#10]_, dask equivalent
   :obj:`numpy.cross`, \-
   :obj:`numpy.cumprod`, :obj:`dask.array.cumprod` [#3]_ [#18]_, dask equivalent
   :obj:`numpy.cumsum`, :obj:`dask.array.cumsum` [#3]_ [#18]_, dask equivalent
   :obj:`numpy.datetime_as_string`, \-
   :obj:`numpy.deg2rad`, :obj:`dask.array.deg2rad`, direct (ufunc)
   :obj:`numpy.degrees`, :obj:`dask.array.degrees`, direct (ufunc)
   :obj:`numpy.delete`, :obj:`dask.array.delete`, dask equivalent
   :obj:`numpy.diag`, :obj:`dask.array.diag`, dask equivalent
   :obj:`numpy.diag_indices`, \-
   :obj:`numpy.diag_indices_from`, \-
   :obj:`numpy.diagflat`, \-
   :obj:`numpy.diagonal`, :obj:`dask.array.diagonal`, dask equivalent
   :obj:`numpy.diff`, :obj:`dask.array.diff`, dask equivalent
   :obj:`numpy.digitize`, :obj:`dask.array.digitize` [#3]_, dask equivalent
   :obj:`numpy.divide`, :obj:`dask.array.divide`, direct (ufunc)
   :obj:`numpy.divmod`, :obj:`dask.array.divmod`, dask equivalent
   :obj:`numpy.dot`, :obj:`dask.array.dot` [#6]_, dask equivalent
   :obj:`numpy.dsplit`, \-
   :obj:`numpy.dstack`, :obj:`dask.array.dstack`, dask equivalent
   :obj:`numpy.ediff1d`, :obj:`dask.array.ediff1d`, dask equivalent
   :obj:`numpy.einsum`, :obj:`dask.array.einsum` [#6]_, dask equivalent
   :obj:`numpy.einsum_path`, \-
   :obj:`numpy.empty`, :obj:`dask.array.empty`, dask equivalent
   :obj:`numpy.empty_like`, :obj:`dask.array.empty_like`, dask equivalent
   :obj:`numpy.equal`, :obj:`dask.array.equal`, direct (ufunc)
   :obj:`numpy.exp`, :obj:`dask.array.exp`, direct (ufunc)
   :obj:`numpy.exp2`, :obj:`dask.array.exp2`, direct (ufunc)
   :obj:`numpy.expand_dims`, :obj:`dask.array.expand_dims`, dask equivalent
   :obj:`numpy.expm1`, :obj:`dask.array.expm1`, direct (ufunc)
   :obj:`numpy.extract`, :obj:`dask.array.extract`, dask equivalent
   :obj:`numpy.eye`, :obj:`dask.array.eye`, dask equivalent
   :obj:`numpy.fabs`, :obj:`dask.array.fabs`, direct (ufunc)
   :obj:`numpy.fill_diagonal`, \-
   :obj:`numpy.fix`, :obj:`dask.array.fix`, direct (non-ufunc elementwise)
   :obj:`numpy.flatnonzero`, :obj:`dask.array.flatnonzero`, dask equivalent
   :obj:`numpy.flip`, :obj:`dask.array.flip`, dask equivalent
   :obj:`numpy.fliplr`, :obj:`dask.array.fliplr`, dask equivalent
   :obj:`numpy.flipud`, :obj:`dask.array.flipud`, dask equivalent
   :obj:`numpy.float_power`, :obj:`dask.array.float_power`, direct (ufunc)
   :obj:`numpy.floor`, :obj:`dask.array.floor`, direct (ufunc)
   :obj:`numpy.floor_divide`, :obj:`dask.array.floor_divide`, direct (ufunc)
   :obj:`numpy.fmax`, :obj:`dask.array.fmax`, direct (ufunc)
   :obj:`numpy.fmin`, :obj:`dask.array.fmin`, direct (ufunc)
   :obj:`numpy.fmod`, :obj:`dask.array.fmod`, direct (ufunc)
   :obj:`numpy.frexp`, :obj:`dask.array.frexp`, dask equivalent
   :obj:`numpy.from_dlpack`, \-
   :obj:`numpy.frombuffer`, \-
   :obj:`numpy.fromfile`, \-
   :obj:`numpy.fromfunction`, :obj:`dask.array.fromfunction` [#11]_, dask equivalent
   :obj:`numpy.fromiter`, \-
   :obj:`numpy.frompyfunc`, :obj:`dask.array.frompyfunc` [#12]_, dask equivalent
   :obj:`numpy.fromregex`, \-
   :obj:`numpy.fromstring`, \-
   :obj:`numpy.full`, :obj:`dask.array.full`, dask equivalent
   :obj:`numpy.full_like`, :obj:`dask.array.full_like`, dask equivalent
   :obj:`numpy.gcd`, \-
   :obj:`numpy.genfromtxt`, \-
   :obj:`numpy.geomspace`, \-
   :obj:`numpy.gradient`, :obj:`dask.array.gradient` [#13]_, dask equivalent
   :obj:`numpy.greater`, :obj:`dask.array.greater`, direct (ufunc)
   :obj:`numpy.greater_equal`, :obj:`dask.array.greater_equal`, direct (ufunc)
   :obj:`numpy.hamming`, \-
   :obj:`numpy.hanning`, \-
   :obj:`numpy.heaviside`, \-
   :obj:`numpy.histogram`, :obj:`dask.array.histogram`, dask equivalent
   :obj:`numpy.histogram2d`, :obj:`dask.array.histogram2d`, dask equivalent
   :obj:`numpy.histogram_bin_edges`, \-
   :obj:`numpy.histogramdd`, :obj:`dask.array.histogramdd` [#14]_, dask equivalent
   :obj:`numpy.hsplit`, \-
   :obj:`numpy.hstack`, :obj:`dask.array.hstack`, dask equivalent
   :obj:`numpy.hypot`, :obj:`dask.array.hypot`, direct (ufunc)
   :obj:`numpy.i0`, :obj:`dask.array.i0`, direct (non-ufunc elementwise)
   :obj:`numpy.identity`, \-
   :obj:`numpy.imag`, :obj:`dask.array.imag`, direct (non-ufunc elementwise)
   :obj:`numpy.in1d`, \-
   :obj:`numpy.indices`, :obj:`dask.array.indices`, dask equivalent
   :obj:`numpy.inner`, \-
   :obj:`numpy.insert`, :obj:`dask.array.insert` [#15]_, dask equivalent
   :obj:`numpy.interp`, \-
   :obj:`numpy.intersect1d`, \-
   :obj:`numpy.invert`, :obj:`dask.array.invert` or :obj:`dask.array.bitwise_not`, direct (ufunc)
   :obj:`numpy.is_busday`, \-
   :obj:`numpy.isclose`, :obj:`dask.array.isclose`, dask equivalent
   :obj:`numpy.iscomplex`, :obj:`dask.array.iscomplex`, direct (non-ufunc elementwise)
   :obj:`numpy.iscomplexobj`, \-
   :obj:`numpy.isfinite`, :obj:`dask.array.isfinite`, direct (ufunc)
   :obj:`numpy.isfortran`, \-
   :obj:`numpy.isin`, :obj:`dask.array.isin`, dask equivalent
   :obj:`numpy.isinf`, :obj:`dask.array.isinf`, direct (ufunc)
   :obj:`numpy.isnan`, :obj:`dask.array.isnan`, direct (ufunc)
   :obj:`numpy.isnat`, \-
   :obj:`numpy.isneginf`, :obj:`dask.array.isneginf`, direct (ufunc)
   :obj:`numpy.isposinf`, :obj:`dask.array.isposinf`, direct (ufunc)
   :obj:`numpy.isreal`, :obj:`dask.array.isreal`, direct (non-ufunc elementwise)
   :obj:`numpy.ix_`, \-
   :obj:`numpy.kaiser`, \-
   :obj:`numpy.kron`, \-
   :obj:`numpy.lcm`, \-
   :obj:`numpy.ldexp`, :obj:`dask.array.ldexp`, direct (ufunc)
   :obj:`numpy.left_shift`, :obj:`dask.array.left_shift`, direct (ufunc)
   :obj:`numpy.less`, :obj:`dask.array.less`, direct (ufunc)
   :obj:`numpy.less_equal`, :obj:`dask.array.less_equal`, direct (ufunc)
   :obj:`numpy.lexsort`, \-
   :obj:`numpy.linspace`, :obj:`dask.array.linspace`, dask equivalent
   :obj:`numpy.load`, \-
   :obj:`numpy.loadtxt`, \-
   :obj:`numpy.log`, :obj:`dask.array.log`, direct (ufunc)
   :obj:`numpy.log10`, :obj:`dask.array.log10`, direct (ufunc)
   :obj:`numpy.log1p`, :obj:`dask.array.log1p`, direct (ufunc)
   :obj:`numpy.log2`, :obj:`dask.array.log2`, direct (ufunc)
   :obj:`numpy.logaddexp`, :obj:`dask.array.logaddexp`, direct (ufunc)
   :obj:`numpy.logaddexp2`, :obj:`dask.array.logaddexp2`, direct (ufunc)
   :obj:`numpy.logical_and`, :obj:`dask.array.logical_and`, direct (ufunc)
   :obj:`numpy.logical_not`, :obj:`dask.array.logical_not`, direct (ufunc)
   :obj:`numpy.logical_or`, :obj:`dask.array.logical_or`, direct (ufunc)
   :obj:`numpy.logical_xor`, :obj:`dask.array.logical_xor`, direct (ufunc)
   :obj:`numpy.logspace`, \-
   :obj:`numpy.mask_indices`, \-
   :obj:`numpy.mat`, \- [#7]_
   :obj:`numpy.matmul`, :obj:`dask.array.matmul`, dask equivalent
   :obj:`numpy.matrix`, \- [#7]_
   :obj:`numpy.maximum`, :obj:`dask.array.maximum`, direct (ufunc)
   :obj:`numpy.may_share_memory`, \-
   :obj:`numpy.mean`, :obj:`dask.array.mean` [#1]_, dask equivalent
   :obj:`numpy.median`, :obj:`dask.array.median` [#16]_, dask equivalent
   :obj:`numpy.memmap`, \-
   :obj:`numpy.meshgrid`, :obj:`dask.array.meshgrid` [#17]_, dask equivalent
   :obj:`numpy.mgrid`, \-
   :obj:`numpy.minimum`, :obj:`dask.array.minimum`, direct (ufunc)
   :obj:`numpy.mintypecode`, \-
   :obj:`numpy.mod`, :obj:`dask.array.mod`, direct (ufunc)
   :obj:`numpy.modf`, :obj:`dask.array.modf`, dask equivalent
   :obj:`numpy.moveaxis`, :obj:`dask.array.moveaxis`, dask equivalent
   :obj:`numpy.multiply`, :obj:`dask.array.multiply`, direct (ufunc)
   :obj:`numpy.nan_to_num`, :obj:`dask.array.nan_to_num`, direct (non-ufunc elementwise)
   :obj:`numpy.nanargmax`, :obj:`dask.array.nanargmax`, dask equivalent
   :obj:`numpy.nanargmin`, :obj:`dask.array.nanargmin`, dask equivalent
   :obj:`numpy.nancumprod`, :obj:`dask.array.nancumprod` [#3]_ [#18]_, dask equivalent
   :obj:`numpy.nancumsum`, :obj:`dask.array.nancumsum` [#3]_ [#18]_, dask equivalent
   :obj:`numpy.nanmax`, :obj:`dask.array.nanmax` [#1]_ [#2]_, dask equivalent
   :obj:`numpy.nanmean`, :obj:`dask.array.nanmean` [#1]_, dask equivalent
   :obj:`numpy.nanmedian`, :obj:`dask.array.nanmedian` [#16]_, dask equivalent
   :obj:`numpy.nanmin`, :obj:`dask.array.nanmin` [#1]_ [#2]_, dask equivalent
   :obj:`numpy.nanpercentile`, :obj:`dask.array.nanpercentile`
   :obj:`numpy.nanprod`, :obj:`dask.array.nanprod` [#1]_ [#2]_, dask equivalent
   :obj:`numpy.nanquantile`, :obj:`dask.array.nanquantile`
   :obj:`numpy.nanstd`, :obj:`dask.array.nanstd` [#1]_, dask equivalent
   :obj:`numpy.nansum`, :obj:`dask.array.nansum` [#1]_ [#2]_, dask equivalent
   :obj:`numpy.nanvar`, :obj:`dask.array.nanvar` [#1]_, dask equivalent
   :obj:`numpy.ndenumerate`, \-
   :obj:`numpy.ndindex`, \-
   :obj:`numpy.nditer`, \-
   :obj:`numpy.negative`, :obj:`dask.array.negative`, direct (ufunc)
   :obj:`numpy.nested_iters`, \-
   :obj:`numpy.nextafter`, :obj:`dask.array.nextafter`, direct (ufunc)
   :obj:`numpy.nonzero`, :obj:`dask.array.nonzero`, dask equivalent
   :obj:`numpy.not_equal`, :obj:`dask.array.not_equal`, direct (ufunc)
   :obj:`numpy.ogrid`, \-
   :obj:`numpy.ones`, :obj:`dask.array.ones`, dask equivalent
   :obj:`numpy.ones_like`, :obj:`dask.array.ones_like`, dask equivalent
   :obj:`numpy.outer`, :obj:`dask.array.outer`, dask equivalent
   :obj:`numpy.packbits`, \-
   :obj:`numpy.pad`, :obj:`dask.array.pad`, dask equivalent
   :obj:`numpy.partition`, \-
   :obj:`numpy.percentile`, :obj:`dask.array.percentile`, dask equivalent
   :obj:`numpy.piecewise`, :obj:`dask.array.piecewise`, dask equivalent
   :obj:`numpy.place`, \-
   :obj:`numpy.poly`, \-
   :obj:`numpy.poly1d`, \-
   :obj:`numpy.polyadd`, \-
   :obj:`numpy.polyder`, \-
   :obj:`numpy.polydiv`, \-
   :obj:`numpy.polyfit`, \-
   :obj:`numpy.polyint`, \-
   :obj:`numpy.polymul`, \-
   :obj:`numpy.polysub`, \-
   :obj:`numpy.polyval`, \-
   :obj:`numpy.positive`, :obj:`dask.array.positive`, direct (ufunc)
   :obj:`numpy.power`, :obj:`dask.array.power`, direct (ufunc)
   :obj:`numpy.prod`, :obj:`dask.array.prod`, dask equivalent
   :obj:`numpy.ptp`, :obj:`dask.array.ptp`, dask equivalent
   :obj:`numpy.put`, \-
   :obj:`numpy.put_along_axis`, \-
   :obj:`numpy.putmask`, \-
   :obj:`numpy.quantile`, :obj:`dask.array.quantile`
   :obj:`numpy.r_`, \-
   :obj:`numpy.rad2deg`, :obj:`dask.array.rad2deg`, direct (ufunc)
   :obj:`numpy.radians`, :obj:`dask.array.radians`, direct (ufunc)
   :obj:`numpy.ravel`, :obj:`dask.array.ravel` [#3]_ [#4]_, dask equivalent
   :obj:`numpy.ravel_multi_index`, :obj:`dask.array.ravel_multi_index`, dask equivalent
   :obj:`numpy.real`, :obj:`dask.array.real`, direct (non-ufunc elementwise)
   :obj:`numpy.real_if_close`, \-
   :obj:`numpy.reciprocal`, :obj:`dask.array.reciprocal`, direct (ufunc)
   :obj:`numpy.remainder`, :obj:`dask.array.remainder`, direct (ufunc)
   :obj:`numpy.repeat`, :obj:`dask.array.repeat`, dask equivalent
   :obj:`numpy.require`, \-
   :obj:`numpy.reshape`, :obj:`dask.array.reshape`, dask equivalent
   :obj:`numpy.resize`, \-
   :obj:`numpy.result_type`, :obj:`dask.array.result_type`, dask equivalent
   :obj:`numpy.right_shift`, :obj:`dask.array.right_shift`, direct (ufunc)
   :obj:`numpy.rint`, :obj:`dask.array.rint`, direct (ufunc)
   :obj:`numpy.roll`, :obj:`dask.array.roll`, dask equivalent
   :obj:`numpy.rollaxis`, :obj:`dask.array.rollaxis`, dask equivalent
   :obj:`numpy.roots`, \-
   :obj:`numpy.rot90`, :obj:`dask.array.rot90`, dask equivalent
   :obj:`numpy.row_stack`, \-
   :obj:`numpy.save`, \-
   :obj:`numpy.savetxt`, \-
   :obj:`numpy.savez`, \-
   :obj:`numpy.savez_compressed`, \-
   :obj:`numpy.searchsorted`, :obj:`dask.array.searchsorted`, dask equivalent
   :obj:`numpy.select`, :obj:`dask.array.select`, dask equivalent
   :obj:`numpy.setdiff1d`, \-
   :obj:`numpy.setxor1d`, \-
   :obj:`numpy.shape`, :obj:`dask.array.shape` [#3]_, dask equivalent
   :obj:`numpy.shares_memory`, \-
   :obj:`numpy.sign`, :obj:`dask.array.sign`, direct (ufunc)
   :obj:`numpy.signbit`, :obj:`dask.array.signbit`, direct (ufunc)
   :obj:`numpy.sin`, :obj:`dask.array.sin`, direct (ufunc)
   :obj:`numpy.sinc`, :obj:`dask.array.sinc`, direct (non-ufunc elementwise)
   :obj:`numpy.sinh`, :obj:`dask.array.sinh`, direct (ufunc)
   :obj:`numpy.sort`, :obj:`dask.array.topk` [#5]_
   :obj:`numpy.sort_complex`, \-
   :obj:`numpy.source`, \-
   :obj:`numpy.spacing`, :obj:`dask.array.spacing`, direct (ufunc)
   :obj:`numpy.split`, \-
   :obj:`numpy.sqrt`, :obj:`dask.array.sqrt`, direct (ufunc)
   :obj:`numpy.square`, :obj:`dask.array.square`, direct (ufunc)
   :obj:`numpy.squeeze`, :obj:`dask.array.squeeze`, dask equivalent
   :obj:`numpy.stack`, :obj:`dask.array.stack`, dask equivalent
   :obj:`numpy.std`, :obj:`dask.array.std` [#1]_, dask equivalent
   :obj:`numpy.subtract`, :obj:`dask.array.subtract`, direct (ufunc)
   :obj:`numpy.sum`, :obj:`dask.array.sum` [#1]_ [#2]_, dask equivalent
   :obj:`numpy.swapaxes`, :obj:`dask.array.swapaxes`, dask equivalent
   :obj:`numpy.take`, :obj:`dask.array.take` [#8]_, dask equivalent
   :obj:`numpy.take_along_axis`, \-
   :obj:`numpy.tan`, :obj:`dask.array.tan`, direct (ufunc)
   :obj:`numpy.tanh`, :obj:`dask.array.tanh`, direct (ufunc)
   :obj:`numpy.tensordot`, :obj:`dask.array.tensordot`, dask equivalent
   :obj:`numpy.tile`, :obj:`dask.array.tile`, dask equivalent
   :obj:`numpy.trace`, :obj:`dask.array.trace` [#6]_, dask equivalent
   :obj:`numpy.transpose`, :obj:`dask.array.transpose`, dask equivalent
   :obj:`numpy.trapz`, \-
   :obj:`numpy.tri`, :obj:`dask.array.tri`, dask equivalent
   :obj:`numpy.tril`, :obj:`dask.array.tril`, dask equivalent
   :obj:`numpy.tril_indices`, :obj:`dask.array.tril_indices`, dask equivalent
   :obj:`numpy.tril_indices_from`, :obj:`dask.array.tril_indices_from`, dask equivalent
   :obj:`numpy.trim_zeros`, \-
   :obj:`numpy.triu`, :obj:`dask.array.triu`, dask equivalent
   :obj:`numpy.triu_indices`, :obj:`dask.array.triu_indices`, dask equivalent
   :obj:`numpy.triu_indices_from`, :obj:`dask.array.triu_indices_from`, dask equivalent
   :obj:`numpy.true_divide`, :obj:`dask.array.true_divide`, direct (ufunc)
   :obj:`numpy.trunc`, :obj:`dask.array.trunc`, direct (ufunc)
   :obj:`numpy.union1d`, :obj:`dask.array.union1d`, dask equivalent
   :obj:`numpy.unique`, :obj:`dask.array.unique` [#19]_, dask equivalent
   :obj:`numpy.unpackbits`, \-
   :obj:`numpy.unravel_index`, :obj:`dask.array.unravel_index`, dask equivalent
   :obj:`numpy.unwrap`, \-
   :obj:`numpy.vander`, \-
   :obj:`numpy.var`, :obj:`dask.array.var` [#1]_, dask equivalent
   :obj:`numpy.vdot`, :obj:`dask.array.vdot`, dask equivalent
   :obj:`numpy.vsplit`, \-
   :obj:`numpy.vstack`, :obj:`dask.array.vstack` [#20]_, dask equivalent
   :obj:`numpy.where`, :obj:`dask.array.where`, dask equivalent
   :obj:`numpy.zeros`, :obj:`dask.array.zeros`, dask equivalent
   :obj:`numpy.zeros_like`, :obj:`dask.array.zeros_like`, dask equivalent

.. rubric:: Footnotes

.. [#1] ``where`` parameter not supported.
.. [#2] ``initial`` parameter not supported.
.. [#3] Input must be a dask array.
.. [#4] ``order`` parameter not supported.
.. [#5] Sort operations are notoriously difficult to do in parallel. Parallel-friendly alternatives sort the k largest elements.
.. [#6] ``out`` parameter not supported.
.. [#7] Use of numpy.matrix is discouraged in NumPy and thus there is no need to add it.
.. [#8] ``mode`` parameter not supported.
.. [#9] ``keepdims`` parameter not supported.
.. [#10] ``fweights``, ``aweights``, ``dtype`` parameters not supported.
.. [#11] ``like`` parameter not supported. Callable functions not supported.
.. [#12] Not implemented with more than one output.
.. [#13] ``edge_order`` parameter not supported.
.. [#14] Chunking of the input data (sample) is only allowed along the 0th (row) axis.
.. [#15] Only implemented for monotonic ``obj`` arguments.
.. [#16] ``overwrite_input`` parameter not supported.
.. [#17] ``copy`` parameter not supported.
.. [#18] Dask implementation introduces an additional parameter ``method``.
.. [#19] ``axis`` parameter not supported.
.. [#20] ``casting`` parameter not supported.
