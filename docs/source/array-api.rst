API
---

.. currentmodule:: dask.array

Top level user functions:

.. autosummary::
   add
   all
   allclose
   angle
   any
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
   rint
   roll
   rollaxis
   round
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
   unify_chunks
   unique
   unravel_index
   var
   vdot
   vstack
   where
   zeros
   zeros_like

Fast Fourier Transforms
~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
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
   image.imread

Slightly Overlapping Computations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   overlap.overlap
   overlap.map_overlap
   overlap.trim_internal
   overlap.trim_overlap


Create and Store Arrays
~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
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
   apply_gufunc
   as_gufunc
   gufunc


Internal functions
~~~~~~~~~~~~~~~~~~

.. currentmodule:: dask.array.core

.. autosummary::
   blockwise
   normalize_chunks


Other functions
~~~~~~~~~~~~~~~

.. currentmodule:: dask.array

.. autofunction:: from_array
.. autofunction:: from_delayed
.. autofunction:: store
.. autofunction:: coarsen
.. autofunction:: stack
.. autofunction:: concatenate

.. autofunction:: add
.. autofunction:: all
.. autofunction:: allclose
.. autofunction:: angle
.. autofunction:: any
.. autofunction:: apply_along_axis
.. autofunction:: apply_over_axes
.. autofunction:: arange
.. autofunction:: arccos
.. autofunction:: arccosh
.. autofunction:: arcsin
.. autofunction:: arcsinh
.. autofunction:: arctan
.. autofunction:: arctan2
.. autofunction:: arctanh
.. autofunction:: argmax
.. autofunction:: argmin
.. autofunction:: argtopk
.. autofunction:: argwhere
.. autofunction:: around
.. autofunction:: array
.. autofunction:: asanyarray
.. autofunction:: asarray
.. autofunction:: atleast_1d
.. autofunction:: atleast_2d
.. autofunction:: atleast_3d
.. autofunction:: average
.. autofunction:: bincount
.. autofunction:: bitwise_and
.. autofunction:: bitwise_not
.. autofunction:: bitwise_or
.. autofunction:: bitwise_xor
.. autofunction:: block
.. autofunction:: blockwise
.. autofunction:: broadcast_arrays
.. autofunction:: broadcast_to
.. autofunction:: cbrt
.. autofunction:: coarsen
.. autofunction:: ceil
.. autofunction:: choose
.. autofunction:: clip
.. autofunction:: compress
.. autofunction:: concatenate
.. autofunction:: conj
.. autofunction:: copysign
.. autofunction:: corrcoef
.. autofunction:: cos
.. autofunction:: cosh
.. autofunction:: count_nonzero
.. autofunction:: cov
.. autofunction:: cumprod
.. autofunction:: cumsum
.. autofunction:: deg2rad
.. autofunction:: degrees
.. autofunction:: diag
.. autofunction:: diagonal
.. autofunction:: diff
.. autofunction:: digitize
.. autofunction:: dot
.. autofunction:: dstack
.. autofunction:: ediff1d
.. autofunction:: empty
.. autofunction:: empty_like
.. autofunction:: equal
.. autofunction:: einsum
.. autofunction:: exp
.. autofunction:: exp2
.. autofunction:: expm1
.. autofunction:: eye
.. autofunction:: fabs
.. autofunction:: fix
.. autofunction:: flatnonzero
.. autofunction:: flip
.. autofunction:: flipud
.. autofunction:: fliplr
.. autofunction:: float_power
.. autofunction:: floor
.. autofunction:: floor_divide
.. autofunction:: fmax
.. autofunction:: fmin
.. autofunction:: fmod
.. autofunction:: frexp
.. autofunction:: fromfunction
.. autofunction:: frompyfunc
.. autofunction:: full
.. autofunction:: full_like
.. autofunction:: gradient
.. autofunction:: greater
.. autofunction:: greater_equal
.. autofunction:: histogram
.. autofunction:: hstack
.. autofunction:: hypot
.. autofunction:: imag
.. autofunction:: indices
.. autofunction:: insert
.. autofunction:: invert
.. autofunction:: isclose
.. autofunction:: iscomplex
.. autofunction:: isfinite
.. autofunction:: isin
.. autofunction:: isinf
.. autofunction:: isneginf
.. autofunction:: isnan
.. autofunction:: isnull
.. autofunction:: isposinf
.. autofunction:: isreal
.. autofunction:: ldexp
.. autofunction:: less
.. autofunction:: linspace
.. autofunction:: log
.. autofunction:: log10
.. autofunction:: log1p
.. autofunction:: log2
.. autofunction:: logaddexp
.. autofunction:: logaddexp2
.. autofunction:: logical_and
.. autofunction:: logical_not
.. autofunction:: logical_or
.. autofunction:: logical_xor
.. autofunction:: map_blocks
.. autofunction:: matmul
.. autofunction:: max
.. autofunction:: maximum
.. autofunction:: mean
.. autofunction:: median
.. autofunction:: meshgrid
.. autofunction:: min
.. autofunction:: minimum
.. autofunction:: mod
.. autofunction:: modf
.. autofunction:: moment
.. autofunction:: moveaxis
.. autofunction:: multiply
.. autofunction:: nanargmax
.. autofunction:: nanargmin
.. autofunction:: nancumprod
.. autofunction:: nancumsum
.. autofunction:: nanmax
.. autofunction:: nanmean
.. autofunction:: nanmedian
.. autofunction:: nanmin
.. autofunction:: nanprod
.. autofunction:: nanstd
.. autofunction:: nansum
.. autofunction:: nanvar
.. autofunction:: nan_to_num
.. autofunction:: negative
.. autofunction:: nextafter
.. autofunction:: nonzero
.. autofunction:: not_equal
.. autofunction:: notnull
.. autofunction:: ones
.. autofunction:: ones_like
.. autofunction:: outer
.. autofunction:: pad
.. autofunction:: percentile
.. autofunction:: piecewise
.. autofunction:: power
.. autofunction:: prod
.. autofunction:: ptp
.. autofunction:: rad2deg
.. autofunction:: radians
.. autofunction:: ravel
.. autofunction:: real
.. autofunction:: reciprocal
.. autofunction:: rechunk
.. autofunction:: reduction
.. autofunction:: register_chunk_type
.. autofunction:: remainder
.. autofunction:: repeat
.. autofunction:: reshape
.. autofunction:: result_type
.. autofunction:: rint
.. autofunction:: roll
.. autofunction:: rollaxis
.. autofunction:: round
.. autofunction:: sign
.. autofunction:: signbit
.. autofunction:: sin
.. autofunction:: sinc
.. autofunction:: sinh
.. autofunction:: sqrt
.. autofunction:: square
.. autofunction:: squeeze
.. autofunction:: stack
.. autofunction:: std
.. autofunction:: subtract
.. autofunction:: sum
.. autofunction:: take
.. autofunction:: tan
.. autofunction:: tanh
.. autofunction:: tensordot
.. autofunction:: tile
.. autofunction:: topk
.. autofunction:: transpose
.. autofunction:: true_divide
.. autofunction:: tril
.. autofunction:: triu
.. autofunction:: trunc
.. autofunction:: unique
.. autofunction:: unravel_index
.. autofunction:: var
.. autofunction:: vdot
.. autofunction:: vstack
.. autofunction:: where
.. autofunction:: zeros
.. autofunction:: zeros_like

.. currentmodule:: dask.array.linalg

.. autofunction:: cholesky
.. autofunction:: inv
.. autofunction:: lstsq
.. autofunction:: lu
.. autofunction:: norm
.. autofunction:: qr
.. autofunction:: solve
.. autofunction:: solve_triangular
.. autofunction:: svd
.. autofunction:: svd_compressed
.. autofunction:: sfqr
.. autofunction:: tsqr

.. currentmodule:: dask.array.ma
.. autofunction:: average
.. autofunction:: filled
.. autofunction:: fix_invalid
.. autofunction:: getdata
.. autofunction:: getmaskarray
.. autofunction:: masked_array
.. autofunction:: masked_equal
.. autofunction:: masked_greater
.. autofunction:: masked_greater_equal
.. autofunction:: masked_inside
.. autofunction:: masked_invalid
.. autofunction:: masked_less
.. autofunction:: masked_less_equal
.. autofunction:: masked_not_equal
.. autofunction:: masked_outside
.. autofunction:: masked_values
.. autofunction:: masked_where
.. autofunction:: set_fill_value

.. currentmodule:: dask.array.overlap

.. autofunction:: overlap
.. autofunction:: map_overlap
.. autofunction:: trim_internal
.. autofunction:: trim_overlap

.. currentmodule:: dask.array

.. autofunction:: from_array
.. autofunction:: from_delayed
.. autofunction:: from_npy_stack
.. autofunction:: from_zarr
.. autofunction:: from_tiledb
.. autofunction:: store
.. autofunction:: to_hdf5
.. autofunction:: to_zarr
.. autofunction:: to_npy_stack
.. autofunction:: to_tiledb

.. currentmodule:: dask.array.fft

.. autofunction:: fft_wrap
.. autofunction:: fft
.. autofunction:: fft2
.. autofunction:: fftn
.. autofunction:: ifft
.. autofunction:: ifft2
.. autofunction:: ifftn
.. autofunction:: rfft
.. autofunction:: rfft2
.. autofunction:: rfftn
.. autofunction:: irfft
.. autofunction:: irfft2
.. autofunction:: irfftn
.. autofunction:: hfft
.. autofunction:: ihfft
.. autofunction:: fftfreq
.. autofunction:: rfftfreq
.. autofunction:: fftshift
.. autofunction:: ifftshift

.. currentmodule:: dask.array.random

.. autofunction:: beta
.. autofunction:: binomial
.. autofunction:: chisquare
.. autofunction:: choice
.. autofunction:: exponential
.. autofunction:: f
.. autofunction:: gamma
.. autofunction:: geometric
.. autofunction:: gumbel
.. autofunction:: hypergeometric
.. autofunction:: laplace
.. autofunction:: logistic
.. autofunction:: lognormal
.. autofunction:: logseries
.. autofunction:: negative_binomial
.. autofunction:: noncentral_chisquare
.. autofunction:: noncentral_f
.. autofunction:: normal
.. autofunction:: pareto
.. autofunction:: poisson
.. autofunction:: power
.. autofunction:: randint
.. autofunction:: random
.. autofunction:: random_sample
.. autofunction:: rayleigh
.. autofunction:: standard_cauchy
.. autofunction:: standard_exponential
.. autofunction:: standard_gamma
.. autofunction:: standard_normal
.. autofunction:: standard_t
.. autofunction:: triangular
.. autofunction:: uniform
.. autofunction:: vonmises
.. autofunction:: wald
.. autofunction:: weibull
.. autofunction:: zipf

.. currentmodule:: dask.array.stats

.. autofunction:: ttest_ind
.. autofunction:: ttest_1samp
.. autofunction:: ttest_rel
.. autofunction:: chisquare
.. autofunction:: power_divergence
.. autofunction:: skew
.. autofunction:: skewtest
.. autofunction:: kurtosis
.. autofunction:: kurtosistest
.. autofunction:: normaltest
.. autofunction:: f_oneway
.. autofunction:: moment

.. currentmodule:: dask.array.image

.. autofunction:: imread

.. currentmodule:: dask.array.gufunc

.. autofunction:: apply_gufunc
.. autofunction:: as_gufunc
.. autofunction:: gufunc

.. currentmodule:: dask.array.core

.. autofunction:: map_blocks
.. autofunction:: blockwise
.. autofunction:: normalize_chunks

.. currentmodule:: dask.array

Array Methods
~~~~~~~~~~~~~

.. autoclass:: Array
   :members:
