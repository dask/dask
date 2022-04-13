API
---

.. currentmodule:: dask.array

Top level functions
~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/

   abs
   absolute
   add
   all
   allclose
   angle
   any
   append
   apply_along_axis
   apply_over_axes
   arange
   arccos
   arccosh
   arcsin
   arcsinh
   arctan
   arctan2
   arctanh
   argmax
   argmin
   argtopk
   argwhere
   around
   array
   asanyarray
   asarray
   atleast_1d
   atleast_2d
   atleast_3d
   average
   bincount
   bitwise_and
   bitwise_not
   bitwise_or
   bitwise_xor
   block
   blockwise
   broadcast_arrays
   broadcast_to
   cbrt
   coarsen
   ceil
   choose
   clip
   compress
   concatenate
   conj
   copysign
   corrcoef
   cos
   cosh
   count_nonzero
   cov
   cumprod
   cumsum
   deg2rad
   degrees
   diag
   diagonal
   diff
   divmod
   digitize
   dot
   dstack
   ediff1d
   einsum
   empty
   empty_like
   equal
   exp
   exp2
   expm1
   eye
   fabs
   fix
   flatnonzero
   flip
   flipud
   fliplr
   float_power
   floor
   floor_divide
   fmax
   fmin
   fmod
   frexp
   fromfunction
   frompyfunc
   full
   full_like
   gradient
   greater
   greater_equal
   histogram
   histogram2d
   histogramdd
   hstack
   hypot
   imag
   indices
   insert
   invert
   isclose
   iscomplex
   isfinite
   isin
   isinf
   isneginf
   isnan
   isnull
   isposinf
   isreal
   ldexp
   left_shift
   less
   linspace
   log
   log10
   log1p
   log2
   logaddexp
   logaddexp2
   logical_and
   logical_not
   logical_or
   logical_xor
   map_overlap
   map_blocks
   matmul
   max
   maximum
   mean
   median
   meshgrid
   min
   minimum
   mod
   modf
   moment
   moveaxis
   multiply
   nanargmax
   nanargmin
   nancumprod
   nancumsum
   nanmax
   nanmean
   nanmedian
   nanmin
   nanprod
   nanstd
   nansum
   nanvar
   nan_to_num
   negative
   nextafter
   nonzero
   not_equal
   notnull
   ones
   ones_like
   outer
   pad
   percentile
   ~core.PerformanceWarning
   piecewise
   positive
   power
   prod
   ptp
   rad2deg
   radians
   ravel
   real
   reciprocal
   rechunk
   reduction
   register_chunk_type
   remainder
   repeat
   reshape
   result_type
   right_shift
   rint
   roll
   rollaxis
   rot90
   round
   searchsorted
   sign
   signbit
   sin
   sinc
   sinh
   sqrt
   square
   squeeze
   stack
   std
   subtract
   sum
   take
   tan
   tanh
   tensordot
   tile
   topk
   trace
   transpose
   true_divide
   tril
   triu
   trunc
   unique
   unravel_index
   var
   vdot
   vstack
   where
   zeros
   zeros_like

Array
~~~~~

.. autosummary::
   :toctree: generated/

   Array
   Array.all
   Array.any
   Array.argmax
   Array.argmin
   Array.argtopk
   Array.astype
   Array.blocks
   Array.choose
   Array.chunks
   Array.chunksize
   Array.clip
   Array.compute
   Array.compute_chunk_sizes
   Array.conj
   Array.copy
   Array.cumprod
   Array.cumsum
   Array.dask
   Array.dot
   Array.dtype
   Array.flatten
   Array.imag
   Array.itemsize
   Array.map_blocks
   Array.map_overlap
   Array.max
   Array.mean
   Array.min
   Array.moment
   Array.name
   Array.nbytes
   Array.ndim
   Array.nonzero
   Array.npartitions
   Array.numblocks
   Array.partitions
   Array.persist
   Array.prod
   Array.ravel
   Array.real
   Array.rechunk
   Array.repeat
   Array.reshape
   Array.round
   Array.shape
   Array.size
   Array.squeeze
   Array.std
   Array.store
   Array.sum
   Array.swapaxes
   Array.to_dask_dataframe
   Array.to_delayed
   Array.to_hdf5
   Array.to_svg
   Array.to_tiledb
   Array.to_zarr
   Array.topk
   Array.trace
   Array.transpose
   Array.var
   Array.view
   Array.vindex
   Array.visualize


Fast Fourier Transforms
~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/

   fft.fft_wrap
   fft.fft
   fft.fft2
   fft.fftn
   fft.ifft
   fft.ifft2
   fft.ifftn
   fft.rfft
   fft.rfft2
   fft.rfftn
   fft.irfft
   fft.irfft2
   fft.irfftn
   fft.hfft
   fft.ihfft
   fft.fftfreq
   fft.rfftfreq
   fft.fftshift
   fft.ifftshift

Linear Algebra
~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/

   linalg.cholesky
   linalg.inv
   linalg.lstsq
   linalg.lu
   linalg.norm
   linalg.qr
   linalg.solve
   linalg.solve_triangular
   linalg.svd
   linalg.svd_compressed
   linalg.sfqr
   linalg.tsqr

Masked Arrays
~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/

   ma.average
   ma.filled
   ma.fix_invalid
   ma.getdata
   ma.getmaskarray
   ma.masked_array
   ma.masked_equal
   ma.masked_greater
   ma.masked_greater_equal
   ma.masked_inside
   ma.masked_invalid
   ma.masked_less
   ma.masked_less_equal
   ma.masked_not_equal
   ma.masked_outside
   ma.masked_values
   ma.masked_where
   ma.set_fill_value

Random
~~~~~~

.. autosummary::
   :toctree: generated/

   random.beta
   random.binomial
   random.chisquare
   random.choice
   random.exponential
   random.f
   random.gamma
   random.geometric
   random.gumbel
   random.hypergeometric
   random.laplace
   random.logistic
   random.lognormal
   random.logseries
   random.negative_binomial
   random.noncentral_chisquare
   random.noncentral_f
   random.normal
   random.pareto
   random.permutation
   random.poisson
   random.power
   random.randint
   random.random
   random.random_sample
   random.rayleigh
   random.standard_cauchy
   random.standard_exponential
   random.standard_gamma
   random.standard_normal
   random.standard_t
   random.triangular
   random.uniform
   random.vonmises
   random.wald
   random.weibull
   random.zipf

Stats
~~~~~

.. autosummary::
   :toctree: generated/

   stats.ttest_ind
   stats.ttest_1samp
   stats.ttest_rel
   stats.chisquare
   stats.power_divergence
   stats.skew
   stats.skewtest
   stats.kurtosis
   stats.kurtosistest
   stats.normaltest
   stats.f_oneway
   stats.moment

Image Support
~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/

   image.imread

Slightly Overlapping Computations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/

   overlap.overlap
   overlap.map_overlap
   lib.stride_tricks.sliding_window_view
   overlap.trim_internal
   overlap.trim_overlap


Create and Store Arrays
~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/

   from_array
   from_delayed
   from_npy_stack
   from_zarr
   from_tiledb
   store
   to_hdf5
   to_zarr
   to_npy_stack
   to_tiledb

Generalized Ufuncs
~~~~~~~~~~~~~~~~~~

.. currentmodule:: dask.array.gufunc

.. autosummary::
   :toctree: generated/

   apply_gufunc
   as_gufunc
   gufunc


Internal functions
~~~~~~~~~~~~~~~~~~

.. currentmodule:: dask.array.core

.. autosummary::
   :toctree: generated/

   blockwise
   normalize_chunks
   unify_chunks
