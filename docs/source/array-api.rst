API
---

.. currentmodule:: dask.array

Top level user functions:

.. autosummary::
   all
   angle
   any
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
   around
   array
   bincount
   broadcast_to
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
   cov
   cumprod
   cumsum
   deg2rad
   degrees
   diag
   digitize
   dot
   dstack
   empty
   exp
   expm1
   eye
   fabs
   fix
   floor
   fmax
   fmin
   fmod
   frexp
   fromfunction
   full
   histogram
   hstack
   hypot
   imag
   indices
   insert
   isclose
   iscomplex
   isfinite
   isinf
   isnan
   isnull
   isreal
   ldexp
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
   max
   maximum
   mean
   min
   minimum
   modf
   moment
   nanargmax
   nanargmin
   nancumprod
   nancumsum
   nanmax
   nanmean
   nanmin
   nanprod
   nanstd
   nansum
   nanvar
   nextafter
   notnull
   ones
   percentile
   prod
   rad2deg
   radians
   ravel
   real
   rechunk
   repeat
   reshape
   rint
   roll
   round
   sign
   signbit
   sin
   sinh
   sqrt
   square
   squeeze
   stack
   std
   sum
   take
   tan
   tanh
   tensordot
   tile
   topk
   transpose
   tril
   triu
   trunc
   unique
   var
   vnorm
   vstack
   where
   zeros

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
   linalg.qr
   linalg.solve
   linalg.solve_triangular
   linalg.svd
   linalg.svd_compressed
   linalg.tsqr

Random
~~~~~~

.. autosummary::
   random.beta
   random.binomial
   random.chisquare
   random.different_seeds
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
   random.poisson
   random.power
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

Slightly Overlapping Ghost Computations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   ghost.ghost
   ghost.map_overlap


Create and Store Arrays
~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   from_array
   from_delayed
   from_npy_stack
   store
   to_hdf5
   to_npy_stack

Internal functions
~~~~~~~~~~~~~~~~~~

.. currentmodule:: dask.array.core

.. autosummary::
   map_blocks
   atop
   top


Other functions
~~~~~~~~~~~~~~~

.. currentmodule:: dask.array

.. autofunction:: from_array
.. autofunction:: from_delayed
.. autofunction:: store
.. autofunction:: topk
.. autofunction:: coarsen
.. autofunction:: stack
.. autofunction:: concatenate

.. autofunction:: all
.. autofunction:: angle
.. autofunction:: any
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
.. autofunction:: around
.. autofunction:: array
.. autofunction:: bincount
.. autofunction:: broadcast_to
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
.. autofunction:: cov
.. autofunction:: cumprod
.. autofunction:: cumsum
.. autofunction:: deg2rad
.. autofunction:: degrees
.. autofunction:: diag
.. autofunction:: digitize
.. autofunction:: dot
.. autofunction:: dstack
.. autofunction:: empty
.. autofunction:: exp
.. autofunction:: expm1
.. autofunction:: eye
.. autofunction:: fabs
.. autofunction:: fix
.. autofunction:: floor
.. autofunction:: fmax
.. autofunction:: fmin
.. autofunction:: fmod
.. autofunction:: frexp
.. autofunction:: fromfunction
.. autofunction:: full
.. autofunction:: histogram
.. autofunction:: hstack
.. autofunction:: hypot
.. autofunction:: imag
.. autofunction:: indices
.. autofunction:: insert
.. autofunction:: isclose
.. autofunction:: iscomplex
.. autofunction:: isfinite
.. autofunction:: isinf
.. autofunction:: isnan
.. autofunction:: isnull
.. autofunction:: isreal
.. autofunction:: ldexp
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
.. autofunction:: max
.. autofunction:: maximum
.. autofunction:: mean
.. autofunction:: min
.. autofunction:: minimum
.. autofunction:: modf
.. autofunction:: moment
.. autofunction:: nanargmax
.. autofunction:: nanargmin
.. autofunction:: nancumprod
.. autofunction:: nancumsum
.. autofunction:: nanmax
.. autofunction:: nanmean
.. autofunction:: nanmin
.. autofunction:: nanprod
.. autofunction:: nanstd
.. autofunction:: nansum
.. autofunction:: nanvar
.. autofunction:: nextafter
.. autofunction:: notnull
.. autofunction:: ones
.. autofunction:: percentile
.. autofunction:: prod
.. autofunction:: rad2deg
.. autofunction:: radians
.. autofunction:: ravel
.. autofunction:: real
.. autofunction:: rechunk
.. autofunction:: repeat
.. autofunction:: reshape
.. autofunction:: rint
.. autofunction:: roll
.. autofunction:: round
.. autofunction:: sign
.. autofunction:: signbit
.. autofunction:: sin
.. autofunction:: sinh
.. autofunction:: sqrt
.. autofunction:: square
.. autofunction:: squeeze
.. autofunction:: stack
.. autofunction:: std
.. autofunction:: sum
.. autofunction:: take
.. autofunction:: tan
.. autofunction:: tanh
.. autofunction:: tensordot
.. autofunction:: tile
.. autofunction:: topk
.. autofunction:: transpose
.. autofunction:: tril
.. autofunction:: triu
.. autofunction:: trunc
.. autofunction:: unique
.. autofunction:: var
.. autofunction:: vnorm
.. autofunction:: vstack
.. autofunction:: where
.. autofunction:: zeros

.. currentmodule:: dask.array.linalg

.. autofunction:: cholesky
.. autofunction:: inv
.. autofunction:: lstsq
.. autofunction:: lu
.. autofunction:: qr
.. autofunction:: solve
.. autofunction:: solve_triangular
.. autofunction:: svd
.. autofunction:: svd_compressed
.. autofunction:: tsqr

.. currentmodule:: dask.array.ghost

.. autofunction:: ghost
.. autofunction:: map_overlap

.. currentmodule:: dask.array

.. autofunction:: from_array
.. autofunction:: from_delayed
.. autofunction:: from_npy_stack
.. autofunction:: store
.. autofunction:: to_hdf5
.. autofunction:: to_npy_stack

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

.. currentmodule:: dask.array.core

.. autofunction:: map_blocks
.. autofunction:: atop
.. autofunction:: top

.. currentmodule:: dask.array

Array Methods
~~~~~~~~~~~~~

.. autoclass:: Array
   :members:
