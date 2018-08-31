Changelog
=========

0.19.1 / YYYY-MM-DD
-------------------

Array
+++++

-

Dataframe
+++++++++

-

Core
++++

-

Documentation
+++++++++++++

-


0.19.0 / 2018-08-29
-------------------

Array
+++++

-  Fix argtopk split_every bug (:pr:`3810`) `Guido Imperiale`_
-  Ensure result computing dask.array.isnull(`) always gives a numpy array (:pr:`3825`) `Stephan Hoyer`_
-  Support concatenate for scipy.sparse in dask array (:pr:`3836`) `Matthew Rocklin`_
-  Fix argtopk on 32-bit systems. (:pr:`3823`) `Elliott Sales de Andrade`_
-  Normalize keys in rechunk (:pr:`3820`) `Matthew Rocklin`_
-  Allow shape of dask.array to be a numpy array (:pr:`3844`) `Mark Harfouche`_
-  Fix numpy deprecation warning on tuple indexing (:pr:`3851`) `Tobias de Jong`_
-  Rename ghost module to overlap (:pr:`3830`) `Robert Sare`_
-  Re-add the ghost import to da __init__ (:pr:`3861`) `Jim Crist`_
-  Ensure copy preserves masked arrays (:pr:`3852`) `Tobias de Jong`_

DataFrame
++++++++++

-  Added ``dtype`` and ``sparse`` keywords to :func:`dask.dataframe.get_dummies` (:pr:`3792`) `Tom Augspurger`_
-  Added :meth:`dask.dataframe.to_dask_array` for converting a Dask Series or DataFrame to a
   Dask Array, possibly with known chunk sizes (:pr:`3884`) `Tom Augspurger`
-  Changed the behavior for :meth:`dask.array.asarray` for dask dataframe and series inputs. Previously,
   the series was eagerly converted to an in-memory NumPy array before creating a dask array with known
   chunks sizes. This caused unexpectedly high memory usage. Now, no intermediate NumPy array is created,
   and a Dask array with unknown chunk sizes is returned (:pr:`3884`) `Tom Augspurger`
-  DataFrame.iloc (:pr:`3805`) `Tom Augspurger`_
-  When reading multiple paths, expand globs. (:pr:`3828`) `Irina Truong`_
-  Added index column name after resample (:pr:`3833`) `Eric Bonfadini`_
-  Add (lazy) shape property to dataframe and series (:pr:`3212`) `Henrique Ribeiro`_
-  Fix failing hdfs test [test-hdfs] (:pr:`3858`) `Jim Crist`_
-  Fixes for pyarrow 0.10.0 release (:pr:`3860`) `Jim Crist`_
-  Rename to_csv keys for diagnostics (:pr:`3890`) `Matthew Rocklin`_
-  Match pandas warnings for concat sort (:pr:`3897`) `Tom Augspurger`_
-  Include filename in read_csv (:pr:`3908`) `Julia Signell`_

Core
++++

-  Better error message on import when missing common dependencies (:pr:`3771`) `Danilo Horta`_
-  Drop Python 3.4 support (:pr:`3840`) `Jim Crist`_
-  Remove expired deprecation warnings (:pr:`3841`) `Jim Crist`_
-  Add DASK_ROOT_CONFIG environment variable (:pr:`3849`) `Joe Hamman`_
-  Don't cull in local scheduler, do cull in delayed (:pr:`3856`) `Jim Crist`_
-  Increase conda download retries (:pr:`3857`) `Jim Crist`_
-  Add python_requires and Trove classifiers (:pr:`3855`) `@hugovk`_
-  Fix collections.abc deprecation warnings in Python 3.7.0 (:pr:`3876`) `Jan Margeta`_
-  Allow dot jpeg to xfail in visualize tests (:pr:`3896`) `Matthew Rocklin`_
-  Add Python 3.7 to travis.yml (:pr:`3894`) `Matthew Rocklin`_
-  Add expand_environment_variables to dask.config (:pr:`3893`) `Joe Hamman`_

Docs
++++

-  Fix typo in import statement of diagnostics (:pr:`3826`) `John Mrziglod`_
-  Add link to YARN docs (:pr:`3838`) `Jim Crist`_
-  fix of minor typos in landing page index.html (:pr:`3746`) `Christoph Moehl`_
-  Update delayed-custom.rst (:pr:`3850`) `Anderson Banihirwe`_
-  DOC: clarify delayed docstring (:pr:`3709`) `Scott Sievert`_
-  Add new presentations (:pr:`3880`) `@javad94`_
-  Add dask array normalize_chunks to documentation (:pr:`3878`) `Daniel Rothenberg`_
-  Docs: Fix link to snakeviz (:pr:`3900`) `Hans Moritz Günther`_
-  Add missing ` to docstring (:pr:`3915`) `@rtobar`_


0.18.2 / 2018-07-23
-------------------

Array
+++++

- Reimplemented ``argtopk`` to make it release the GIL (:pr:`3610`) `Guido Imperiale`_
- Don't overlap on non-overlapped dimensions in ``map_overlap`` (:pr:`3653`) `Matthew Rocklin`_
- Fix ``linalg.tsqr`` for dimensions of uncertain length (:pr:`3662`) `Jeremy Chen`_
- Break apart uneven array-of-int slicing to separate chunks (:pr:`3648`) `Matthew Rocklin`_
- Align auto chunks to provided chunks, rather than shape (:pr:`3679`) `Matthew Rocklin`_
- Adds endpoint and retstep support for linspace (:pr:`3675`) `James Bourbeau`_
- Implement ``.blocks`` accessor (:pr:`3689`) `Matthew Rocklin`_
- Add ``block_info`` keyword to ``map_blocks`` functions (:pr:`3686`) `Matthew Rocklin`_
- Slice by dask array of ints (:pr:`3407`) `Guido Imperiale`_
- Support ``dtype`` in ``arange`` (:pr:`3722`) `Guido Imperiale`_
- Fix ``argtopk`` with uneven chunks (:pr:`3720`) `Guido Imperiale`_
- Raise error when ``replace=False`` in ``da.choice`` (:pr:`3765`) `James Bourbeau`_
- Update chunks in ``Array.__setitem__`` (:pr:`3767`) `Itamar Turner-Trauring`_
- Add a ``chunksize`` convenience property (:pr:`3777`) `Jacob Tomlinson`_
- Fix and simplify array slicing behavior when ``step < 0`` (:pr:`3702`) `Ziyao Wei`_
- Ensure ``to_zarr`` with ``return_stored`` ``True`` returns a Dask Array (:pr:`3786`) `John A Kirkham`_

Bag
+++

- Add ``last_endline`` optional parameter in ``to_textfiles`` (:pr:`3745`) `George Sakkis`_

Dataframe
+++++++++

- Add aggregate function for rolling objects (:pr:`3772`) `Gerome Pistre`_
- Properly tokenize cumulative groupby aggregations (:pr:`3799`) `Cloves Almeida`_

Delayed
+++++++

- Add the ``@`` operator to the delayed objects (:pr:`3691`) `Mark Harfouche`_
- Add delayed best practices to documentation (:pr:`3737`) `Matthew Rocklin`_
- Fix ``@delayed`` decorator for methods and add tests (:pr:`3757`) `Ziyao Wei`_

Core
++++

- Fix extra progressbar (:pr:`3669`) `Mike Neish`_
- Allow tasks back onto ordering stack if they have one dependency (:pr:`3652`) `Matthew Rocklin`_
- Prefer end-tasks with low numbers of dependencies when ordering (:pr:`3588`) `Tom Augspurger`_
- Add ``assert_eq`` to top-level modules (:pr:`3726`) `Matthew Rocklin`_
- Test that dask collections can hold ``scipy.sparse`` arrays (:pr:`3738`) `Matthew Rocklin`_
- Fix setup of lz4 decompression functions (:pr:`3782`) `Elliott Sales de Andrade`_
- Add datasets module (:pr:`3780`) `Matthew Rocklin`_


0.18.1 / 2018-06-22
-------------------

Array
+++++

- ``from_array`` now supports scalar types and nested lists/tuples in input,
  just like all numpy functions do; it also produces a simpler graph when the
  input is a plain ndarray (:pr:`3568`) `Guido Imperiale`_
- Fix slicing of big arrays due to cumsum dtype bug (:pr:`3620`) `Marco Rossi`_
- Add Dask Array implementation of pad (:pr:`3578`) `John A Kirkham`_
- Fix array random API examples (:pr:`3625`) `James Bourbeau`_
- Add average function to dask array (:pr:`3640`) `James Bourbeau`_
- Tokenize ghost_internal with axes (:pr:`3643`)  `Matthew Rocklin`_
- Add outer for Dask Arrays (:pr:`3658`) `John A Kirkham`_

DataFrame
+++++++++

- Add Index.to_series method (:pr:`3613`) `Henrique Ribeiro`_
- Fix missing partition columns in pyarrow-parquet (:pr:`3636`) `Martin Durant`_

Core
++++

- Minor tweaks to CI (:pr:`3629`) `Guido Imperiale`_
- Add back dask.utils.effective_get (:pr:`3642`) `Matthew Rocklin`_
- DASK_CONFIG dictates config write location (:pr:`3621`) `Jim Crist`_
- Replace 'collections' key in unpack_collections with unique key (:pr:`3632`) `Yu Feng`_
- Avoid deepcopy in dask.config.set (:pr:`3649`) `Matthew Rocklin`_


0.18.0 / 2018-06-14
-------------------

Array
+++++

- Add to/read_zarr for Zarr-format datasets and arrays (:pr:`3460`) `Martin Durant`_
- Experimental addition of generalized ufunc support, ``apply_gufunc``, ``gufunc``, and
  ``as_gufunc`` (:pr:`3109`) (:pr:`3526`) (:pr:`3539`) `Markus Gonser`_
- Avoid unnecessary rechunking tasks (:pr:`3529`) `Matthew Rocklin`_
- Compute dtypes at runtime for fft (:pr:`3511`) `Matthew Rocklin`_
- Generate UUIDs for all da.store operations (:pr:`3540`) `Martin Durant`_
- Correct internal dimension of Dask's SVD (:pr:`3517`) `John A Kirkham`_
- BUG: do not raise IndexError for identity slice in array.vindex (:pr:`3559`) `Scott Sievert`_
- Adds `isneginf` and `isposinf` (:pr:`3581`) `John A Kirkham`_
- Drop Dask Array's `learn` module (:pr:`3580`) `John A Kirkham`_
- added sfqr (short-and-fat) as a counterpart to tsqr… (:pr:`3575`) `Jeremy Chen`_
- Allow 0-width chunks in dask.array.rechunk (:pr:`3591`) `Marc Pfister`_
- Document Dask Array's `nan_to_num` in public API (:pr:`3599`) `John A Kirkham`_
- Show block example (:pr:`3601`) `John A Kirkham`_
- Replace token= keyword with name= in map_blocks (:pr:`3597`) `Matthew Rocklin`_
- Disable locking in to_zarr (needed for using to_zarr in a distributed context) (:pr:`3607`) `John A Kirkham`_
- Support Zarr Arrays in `to_zarr`/`from_zarr` (:pr:`3561`) `John A Kirkham`_
- Added recursion to array/linalg/tsqr to better manage the single core bottleneck (:pr:`3586`) `Jeremy Chan`_
  (:pr:`3396`) `Guido Imperiale`_

Dataframe
+++++++++

- Add to/read_json (:pr:`3494`) `Martin Durant`_
- Adds ``index`` to unsupported arguments for ``DataFrame.rename`` method (:pr:`3522`) `James Bourbeau`_
- Adds support to subset Dask DataFrame columns using ``numpy.ndarray``, ``pandas.Series``, and
  ``pandas.Index`` objects (:pr:`3536`) `James Bourbeau`_
- Raise error if meta columns do not match dataframe (:pr:`3485`) `Christopher Ren`_
- Add index to unsupprted argument for DataFrame.rename (:pr:`3522`) `James Bourbeau`_
- Adds support for subsetting DataFrames with pandas Index/Series and numpy ndarrays (:pr:`3536`) `James Bourbeau`_
- Dataframe sample method docstring fix (:pr:`3566`) `James Bourbeau`_
- fixes dd.read_json to infer file compression (:pr:`3594`) `Matt Lee`_
- Adds n to sample method (:pr:`3606`) `James Bourbeau`_
- Add fastparquet ParquetFile object support (:pr:`3573`) `@andrethrill`_

Bag
+++

- Rename method= keyword to shuffle= in bag.groupby (:pr:`3470`) `Matthew Rocklin`_

Core
++++

- Replace get= keyword with scheduler= keyword (:pr:`3448`) `Matthew Rocklin`_
- Add centralized dask.config module to handle configuration for all Dask
  subprojects (:pr:`3432`) (:pr:`3513`) (:pr:`3520`) `Matthew Rocklin`_
- Add `dask-ssh` CLI Options and Description. (:pr:`3476`) `@beomi`_
- Read whole files fix regardless of header for HTTP (:pr:`3496`) `Martin Durant`_
- Adds synchronous scheduler syntax to debugging docs (:pr:`3509`) `James Bourbeau`_
- Replace dask.set_options with dask.config.set (:pr:`3502`) `Matthew Rocklin`_
- Update sphinx readthedocs-theme (:pr:`3516`) `Matthew Rocklin`_
- Introduce "auto" value for normalize_chunks (:pr:`3507`) `Matthew Rocklin`_
- Fix check in configuration with env=None (:pr:`3562`) `Simon Perkins`_
- Update sizeof definitions (:pr:`3582`) `Matthew Rocklin`_
- Remove --verbose flag from travis-ci (:pr:`3477`) `Matthew Rocklin`_
- Remove "da.random" from random array keys (:pr:`3604`) `Matthew Rocklin`_


0.17.5 / 2018-05-16
-------------------

Array
+++++

- Fix ``rechunk`` with chunksize of -1 in a dict (:pr:`3469`) `Stephan Hoyer`_
- ``einsum`` now accepts the ``split_every`` parameter (:pr:`3471`) `Guido Imperiale`_
- Improved slicing performance (:pr:`3479`) `Yu Feng`_

DataFrame
+++++++++

- Compatibility with pandas 0.23.0 (:pr:`3499`) `Tom Augspurger`_


0.17.4 / 2018-05-03
-------------------

Dataframe
+++++++++

- Add support for indexing Dask DataFrames with string subclasses (:pr:`3461`) `James Bourbeau`_
- Allow using both sorted_index and chunksize in read_hdf (:pr:`3463`) `Pierre Bartet`_
- Pass filesystem to arrow piece reader (:pr:`3466`) `Martin Durant`_
- Switches to using dask.compat string_types (:pr:`3462`) `James Bourbeau`_


0.17.3 / 2018-05-02
-------------------

Array
+++++

- Add ``einsum`` for Dask Arrays (:pr:`3412`) `Simon Perkins`_
- Add ``piecewise`` for Dask Arrays (:pr:`3350`) `John A Kirkham`_
- Fix handling of ``nan`` in ``broadcast_shapes`` (:pr:`3356`) `John A Kirkham`_
- Add ``isin`` for dask arrays (:pr:`3363`). `Stephan Hoyer`_
- Overhauled ``topk`` for Dask Arrays: faster algorithm, particularly for large k's; added support
  for multiple axes, recursive aggregation, and an option to pick the bottom k elements instead.
  (:pr:`3395`) `Guido Imperiale`_
- The ``topk`` API has changed from topk(k, array) to the more conventional topk(array, k).
  The legacy API still works but is now deprecated. (:pr:`2965`) `Guido Imperiale`_
- New function ``argtopk`` for Dask Arrays (:pr:`3396`) `Guido Imperiale`_
- Fix handling partial depth and boundary in ``map_overlap`` (:pr:`3445`) `John A Kirkham`_
- Add ``gradient`` for Dask Arrays (:pr:`3434`) `John A Kirkham`_

DataFrame
+++++++++

- Allow `t` as shorthand for `table` in `to_hdf` for pandas compatibility (:pr:`3330`) `Jörg Dietrich`_
- Added top level `isna` method for Dask DataFrames (:pr:`3294`) `Christopher Ren`_
- Fix selection on partition column on ``read_parquet`` for ``engine="pyarrow"`` (:pr:`3207`) `Uwe Korn`_
- Added DataFrame.squeeze method (:pr:`3366`) `Christopher Ren`_
- Added `infer_divisions` option to ``read_parquet`` to specify whether read engines should compute divisions (:pr:`3387`) `Jon Mease`_
- Added support for inferring division for ``engine="pyarrow"`` (:pr:`3387`) `Jon Mease`_
- Provide more informative error message for meta= errors (:pr:`3343`) `Matthew Rocklin`_
- add orc reader (:pr:`3284`) `Martin Durant`_
- Default compression for parquet now always Snappy, in line with pandas (:pr:`3373`) `Martin Durant`_
- Fixed bug in Dask DataFrame and Series comparisons with NumPy scalars (:pr:`3436`) `James Bourbeau`_
- Remove outdated requirement from repartition docstring (:pr:`3440`) `Jörg Dietrich`_
- Fixed bug in aggregation when only a Series is selected (:pr:`3446`) `Jörg Dietrich`_
- Add default values to make_timeseries (:pr:`3421`) `Matthew Rocklin`_

Core
++++

- Support traversing collections in persist, visualize, and optimize (:pr:`3410`) `Jim Crist`_
- Add schedule= keyword to compute and persist.  This replaces common use of the get= keyword (:pr:`3448`) `Matthew Rocklin`_


0.17.2 / 2018-03-21
-------------------

Array
+++++

- Add ``broadcast_arrays`` for Dask Arrays (:pr:`3217`) `John A Kirkham`_
- Add ``bitwise_*`` ufuncs (:pr:`3219`) `John A Kirkham`_
- Add optional ``axis`` argument to ``squeeze`` (:pr:`3261`) `John A Kirkham`_
- Validate inputs to atop (:pr:`3307`) `Matthew Rocklin`_
- Avoid calls to astype in concatenate if all parts have the same dtype (:pr:`3301`) `Martin Durant`_

DataFrame
+++++++++

- Fixed bug in shuffle due to aggressive truncation (:pr:`3201`) `Matthew Rocklin`_
- Support specifying categorical columns on ``read_parquet`` with ``categories=[…]`` for ``engine="pyarrow"`` (:pr:`3177`) `Uwe Korn`_
- Add ``dd.tseries.Resampler.agg`` (:pr:`3202`) `Richard Postelnik`_
- Support operations that mix dataframes and arrays (:pr:`3230`) `Matthew Rocklin`_
- Support extra Scalar and Delayed args in ``dd.groupby._Groupby.apply`` (:pr:`3256`) `Gabriele Lanaro`_

Bag
+++

- Support joining against single-partitioned bags and delayed objects (:pr:`3254`) `Matthew Rocklin`_

Core
++++

- Fixed bug when using unexpected but hashable types for keys (:pr:`3238`) `Daniel Collins`_
- Fix bug in task ordering so that we break ties consistently with the key name (:pr:`3271`) `Matthew Rocklin`_
- Avoid sorting tasks in order when the number of tasks is very large (:pr:`3298`) `Matthew Rocklin`_


0.17.1 / 2018-02-22
-------------------

Array
+++++

- Corrected dimension chunking in indices (:issue:`3166`, :pr:`3167`) `Simon Perkins`_
- Inline ``store_chunk`` calls for ``store``'s ``return_stored`` option (:pr:`3153`) `John A Kirkham`_
- Compatibility with struct dtypes for NumPy 1.14.1 release (:pr:`3187`) `Matthew Rocklin`_

DataFrame
+++++++++

- Bugfix to allow column assignment of pandas datetimes(:pr:`3164`) `Max Epstein`_

Core
++++

- New file-system for HTTP(S), allowing direct loading from specific URLs (:pr:`3160`) `Martin Durant`_
- Fix bug when tokenizing partials with no keywords (:pr:`3191`) `Matthew Rocklin`_
- Use more recent LZ4 API (:pr:`3157`) `Thrasibule`_
- Introduce output stream parameter for progress bar (:pr:`3185`) `Dieter Weber`_


0.17.0 / 2018-02-09
-------------------

Array
+++++

- Added a support object-type arrays for nansum, nanmin, and nanmax (:issue:`3133`) `Keisuke Fujii`_
- Update error handling when len is called with empty chunks (:issue:`3058`) `Xander Johnson`_
- Fixes a metadata bug with ``store``'s ``return_stored`` option (:pr:`3064`) `John A Kirkham`_
- Fix a bug in ``optimization.fuse_slice`` to properly handle when first input is ``None`` (:pr:`3076`) `James Bourbeau`_
- Support arrays with unknown chunk sizes in percentile (:pr:`3107`) `Matthew Rocklin`_
- Tokenize scipy.sparse arrays and np.matrix (:pr:`3060`) `Roman Yurchak`_

DataFrame
+++++++++
- Support month timedeltas in repartition(freq=...) (:pr:`3110`) `Matthew Rocklin`_
- Avoid mutation in dataframe groupby tests (:pr:`3118`) `Matthew Rocklin`_
- ``read_csv``, ``read_table``, and ``read_parquet`` accept iterables of paths
  (:pr:`3124`) `Jim Crist`_
- Deprecates the ``dd.to_delayed`` *function* in favor of the existing method
  (:pr:`3126`) `Jim Crist`_
- Return dask.arrays from df.map_partitions calls when the UDF returns a numpy array (:pr:`3147`) `Matthew Rocklin`_
- Change handling of ``columns`` and ``index`` in ``dd.read_parquet`` to be more
  consistent, especially in handling of multi-indices (:pr:`3149`) `Jim Crist`_
- fastparquet append=True allowed to create new dataset (:pr:`3097`) `Martin Durant`_
- dtype rationalization for sql queries (:pr:`3100`) `Martin Durant`_

Bag
+++

- Document ``bag.map_paritions`` function may recieve either a list or generator. (:pr:`3150`) `Nir`_

Core
++++

- Change default task ordering to prefer nodes with few dependents and then
  many downstream dependencies (:pr:`3056`) `Matthew Rocklin`_
- Add color= option to visualize to color by task order (:pr:`3057`) (:pr:`3122`) `Matthew Rocklin`_
- Deprecate ``dask.bytes.open_text_files`` (:pr:`3077`) `Jim Crist`_
- Remove short-circuit hdfs reads handling due to maintenance costs. May be
  re-added in a more robust manner later (:pr:`3079`) `Jim Crist`_
- Add ``dask.base.optimize`` for optimizing multiple collections without
  computing. (:pr:`3071`) `Jim Crist`_
- Rename ``dask.optimize`` module to ``dask.optimization`` (:pr:`3071`) `Jim Crist`_
- Change task ordering to do a full traversal (:pr:`3066`) `Matthew Rocklin`_
- Adds an ``optimize_graph`` keyword to all ``to_delayed`` methods to allow
  controlling whether optimizations occur on conversion. (:pr:`3126`) `Jim Crist`_
- Support using ``pyarrow`` for hdfs integration (:pr:`3123`) `Jim Crist`_
- Move HDFS integration and tests into dask repo (:pr:`3083`) `Jim Crist`_
- Remove write_bytes (:pr:`3116`) `Jim Crist`_


0.16.1 / 2018-01-09
-------------------

Array
+++++

- Fix handling of scalar percentile values in ``percentile`` (:pr:`3021`) `James Bourbeau`_
- Prevent ``bool()`` coercion from calling compute (:pr:`2958`) `Albert DeFusco`_
- Add ``matmul`` (:pr:`2904`) `John A Kirkham`_
- Support N-D arrays with ``matmul`` (:pr:`2909`) `John A Kirkham`_
- Add ``vdot`` (:pr:`2910`) `John A Kirkham`_
- Explicit ``chunks`` argument for ``broadcast_to`` (:pr:`2943`) `Stephan Hoyer`_
- Add ``meshgrid`` (:pr:`2938`) `John A Kirkham`_ and (:pr:`3001`) `Markus Gonser`_
- Preserve singleton chunks in ``fftshift``/``ifftshift`` (:pr:`2733`) `John A Kirkham`_
- Fix handling of negative indexes in ``vindex`` and raise errors for out of bounds indexes (:pr:`2967`) `Stephan Hoyer`_
- Add ``flip``, ``flipud``, ``fliplr`` (:pr:`2954`) `John A Kirkham`_
- Add ``float_power`` ufunc (:pr:`2962`) (:pr:`2969`) `John A Kirkham`_
- Compatability for changes to structured arrays in the upcoming NumPy 1.14 release (:pr:`2964`) `Tom Augspurger`_
- Add ``block`` (:pr:`2650`) `John A Kirkham`_
- Add ``frompyfunc`` (:pr:`3030`) `Jim Crist`_
- Add the ``return_stored`` option to ``store`` for chaining stored results (:pr:`2980`) `John A Kirkham`_

DataFrame
+++++++++

- Fixed naming bug in cumulative aggregations (:issue:`3037`) `Martijn Arts`_
- Fixed ``dd.read_csv`` when ``names`` is given but ``header`` is not set to ``None`` (:issue:`2976`) `Martijn Arts`_
- Fixed ``dd.read_csv`` so that passing instances of ``CategoricalDtype`` in ``dtype`` will result in known categoricals (:pr:`2997`) `Tom Augspurger`_
- Prevent ``bool()`` coercion from calling compute (:pr:`2958`) `Albert DeFusco`_
- ``DataFrame.read_sql()`` (:pr:`2928`) to an empty database tables returns an empty dask dataframe `Apostolos Vlachopoulos`_
- Compatability for reading Parquet files written by PyArrow 0.8.0 (:pr:`2973`) `Tom Augspurger`_
- Correctly handle the column name (`df.columns.name`) when reading in ``dd.read_parquet`` (:pr:`2973`) `Tom Augspurger`_
- Fixed ``dd.concat`` losing the index dtype when the data contained a categorical (:issue:`2932`) `Tom Augspurger`_
- Add ``dd.Series.rename`` (:pr:`3027`) `Jim Crist`_
- ``DataFrame.merge()`` now supports merging on a combination of columns and the index (:pr:`2960`) `Jon Mease`_
- Removed the deprecated ``dd.rolling*`` methods, in preperation for their removal in the next pandas release (:pr:`2995`) `Tom Augspurger`_
- Fix metadata inference bug in which single-partition series were mistakenly special cased (:pr:`3035`) `Jim Crist`_
- Add support for ``Series.str.cat`` (:pr:`3028`) `Jim Crist`_

Core
++++

- Improve 32-bit compatibility (:pr:`2937`) `Matthew Rocklin`_
- Change task prioritization to avoid upwards branching (:pr:`3017`) `Matthew Rocklin`_


0.16.0 / 2017-11-17
-------------------

This is a major release.  It includes breaking changes, new protocols, and a
large number of bug fixes.

Array
+++++

- Add ``atleast_1d``, ``atleast_2d``, and ``atleast_3d`` (:pr:`2760`) (:pr:`2765`) `John A Kirkham`_
- Add ``allclose`` (:pr:`2771`) by `John A Kirkham`_
- Remove ``random.different_seeds`` from Dask Array API docs (:pr:`2772`) `John A Kirkham`_
- Deprecate ``vnorm`` in favor of ``dask.array.linalg.norm`` (:pr:`2773`) `John A Kirkham`_
- Reimplement ``unique`` to be lazy (:pr:`2775`) `John A Kirkham`_
- Support broadcasting of Dask Arrays with 0-length dimensions (:pr:`2784`) `John A Kirkham`_
- Add ``asarray`` and ``asanyarray`` to Dask Array API docs (:pr:`2787`) `James Bourbeau`_
- Support ``unique``'s ``return_*`` arguments (:pr:`2779`) `John A Kirkham`_
- Simplify ``_unique_internal`` (:pr:`2850`) (:pr:`2855`) `John A Kirkham`_
- Avoid removing some getter calls in array optimizations (:pr:`2826`) `Jim Crist`_

DataFrame
+++++++++

- Support ``pyarrow`` in ``dd.to_parquet`` (:pr:`2868`) `Jim Crist`_
- Fixed ``DataFrame.quantile`` and ``Series.quantile`` returning ``nan`` when missing values are present (:pr:`2791`) `Tom Augspurger`_
- Fixed ``DataFrame.quantile`` losing the result ``.name`` when ``q`` is a scalar (:pr:`2791`) `Tom Augspurger`_
- Fixed ``dd.concat`` return a ``dask.Dataframe`` when concatenating a single series along the columns, matching pandas' behavior (:pr:`2800`) `James Munroe`_
- Fixed default inplace parameter for ``DataFrame.eval`` to match the pandas defualt for pandas >= 0.21.0 (:pr:`2838`) `Tom Augspurger`_
- Fix exception when calling ``DataFrame.set_index`` on text column where one of the partitions was empty (:pr:`2831`) `Jesse Vogt`_
- Do not raise exception when calling ``DataFrame.set_index`` on empty dataframe (:pr:`2827`) `Jesse Vogt`_
- Fixed bug in ``Dataframe.fillna`` when filling with a ``Series`` value (:pr:`2810`) `Tom Augspurger`_
- Deprecate old argument ordering in ``dd.to_parquet`` to better match convention of putting the dataframe first (:pr:`2867`) `Jim Crist`_
- df.astype(categorical_dtype -> known categoricals (:pr:`2835`) `Jim Crist`_
- Test against Pandas release candidate (:pr:`2814`) `Tom Augspurger`_
- Add more tests for read_parquet(engine='pyarrow') (:pr:`2822`) `Uwe Korn`_
- Remove unnecessary map_partitions in aggregate (:pr:`2712`) `Christopher Prohm`_
- Fix bug calling sample on empty partitions (:pr:`2818`) `@xwang777`_
- Error nicely when parsing dates in read_csv (:pr:`2863`) `Jim Crist`_
- Cleanup handling of passing filesystem objects to PyArrow readers (:pr:`2527`) `@fjetter`_
- Support repartitioning even if there are no divisions (:pr:`2873`) `@Ced4`_
- Support reading/writing to hdfs using ``pyarrow`` in ``dd.to_parquet`` (:pr:`2894`, :pr:`2881`) `Jim Crist`_

Core
++++

-  Allow tuples as sharedict keys (:pr:`2763`) `Matthew Rocklin`_
-  Calling compute within a dask.distributed task defaults to distributed scheduler (:pr:`2762`) `Matthew Rocklin`_
-  Auto-import gcsfs when gcs:// protocol is used (:pr:`2776`) `Matthew Rocklin`_
-  Fully remove dask.async module, use dask.local instead (:pr:`2828`) `Thomas Caswell`_
-  Compatability with bokeh 0.12.10 (:pr:`2844`) `Tom Augspurger`_
-  Reduce test memory usage (:pr:`2782`) `Jim Crist`_
-  Add Dask collection interface (:pr:`2748`) `Jim Crist`_
-  Update Dask collection interface during XArray integration (:pr:`2847`) `Matthew Rocklin`_
-  Close resource profiler process on __exit__ (:pr:`2871`) `Jim Crist`_
-  Fix S3 tests (:pr:`2875`) `Jim Crist`_
-  Fix port for bokeh dashboard in docs (:pr:`2889`) `Ian Hopkinson`_
-  Wrap Dask filesystems for PyArrow compatibility (:pr:`2881`) `Jim Crist`_


0.15.4 / 2017-10-06
-------------------

Array
+++++

-  ``da.random.choice`` now works with array arguments (:pr:`2781`)
-  Support indexing in arrays with np.int (fixes regression) (:pr:`2719`)
-  Handle zero dimension with rechunking (:pr:`2747`)
-  Support -1 as an alias for "size of the dimension" in ``chunks`` (:pr:`2749`)
-  Call mkdir in array.to_npy_stack (:pr:`2709`)

DataFrame
+++++++++

-  Added the `.str` accessor to Categoricals with string categories (:pr:`2743`)
-  Support int96 (spark) datetimes in parquet writer (:pr:`2711`)
-  Pass on file scheme to fastparquet (:pr:`2714`)
-  Support Pandas 0.21 (:pr:`2737`)

Bag
+++

- Add tree reduction support for foldby (:pr:`2710`)


Core
++++

-  Drop s3fs from ``pip install dask[complete]`` (:pr:`2750`)


0.15.3 / 2017-09-24
-------------------

Array
+++++

-  Add masked arrays (:pr:`2301`)
-  Add ``*_like array creation functions`` (:pr:`2640`)
-  Indexing with unsigned integer array (:pr:`2647`)
-  Improved slicing with boolean arrays of different dimensions (:pr:`2658`)
-  Support literals in ``top`` and ``atop`` (:pr:`2661`)
-  Optional axis argument in cumulative functions (:pr:`2664`)
-  Improve tests on scalars with ``assert_eq`` (:pr:`2681`)
-  Fix norm keepdims (:pr:`2683`)
-  Add ``ptp`` (:pr:`2691`)
-  Add apply_along_axis (:pr:`2690`) and apply_over_axes (:pr:`2702`)

DataFrame
+++++++++

-  Added ``Series.str[index]`` (:pr:`2634`)
-  Allow the groupby by param to handle columns and index levels (:pr:`2636`)
-  ``DataFrame.to_csv`` and ``Bag.to_textfiles`` now return the filenames to
    which they have written (:pr:`2655`)
-  Fix combination of ``partition_on`` and ``append`` in ``to_parquet``
   (:pr:`2645`)
-  Fix for parquet file schemes (:pr:`2667`)
-  Repartition works with mixed categoricals (:pr:`2676`)

Core
++++

-  ``python setup.py test`` now runs tests (:pr:`2641`)
-  Added new cheatsheet (:pr:`2649`)
-  Remove resize tool in Bokeh plots (:pr:`2688`)


0.15.2 / 2017-08-25
-------------------

Array
+++++

-  Remove spurious keys from map_overlap graph (:pr:`2520`)
-  where works with non-bool condition and scalar values (:pr:`2543`) (:pr:`2549`)
-  Improve compress (:pr:`2541`) (:pr:`2545`) (:pr:`2555`)
-  Add argwhere, _nonzero, and where(cond) (:pr:`2539`)
-  Generalize vindex in dask.array to handle multi-dimensional indices (:pr:`2573`)
-  Add choose method (:pr:`2584`)
-  Split code into reorganized files (:pr:`2595`)
-  Add linalg.norm (:pr:`2597`)
-  Add diff, ediff1d (:pr:`2607`), (:pr:`2609`)
-  Improve dtype inference and reflection (:pr:`2571`)

Bag
+++

-   Remove deprecated Bag behaviors (:pr:`2525`)

DataFrame
+++++++++

-  Support callables in assign (:pr:`2513`)
-  better error messages for read_csv (:pr:`2522`)
-  Add dd.to_timedelta (:pr:`2523`)
-  Verify metadata in from_delayed (:pr:`2534`) (:pr:`2591`)
-  Add DataFrame.isin (:pr:`2558`)
-  Read_hdf supports iterables of files (:pr:`2547`)

Core
++++

-  Remove bare ``except:`` blocks everywhere (:pr:`2590`)

0.15.1 / 2017-07-08
-------------------

-  Add storage_options to to_textfiles and to_csv (:pr:`2466`)
-  Rechunk and simplify rfftfreq (:pr:`2473`), (:pr:`2475`)
-  Better support ndarray subclasses (:pr:`2486`)
-  Import star in dask.distributed (:pr:`2503`)
-  Threadsafe cache handling with tokenization (:pr:`2511`)


0.15.0 / 2017-06-09
-------------------

Array
+++++

-  Add dask.array.stats submodule (:pr:`2269`)
-  Support ``ufunc.outer`` (:pr:`2345`)
-  Optimize fancy indexing by reducing graph overhead (:pr:`2333`) (:pr:`2394`)
-  Faster array tokenization using alternative hashes (:pr:`2377`)
-  Added the matmul ``@`` operator (:pr:`2349`)
-  Improved coverage of the ``numpy.fft`` module (:pr:`2320`) (:pr:`2322`) (:pr:`2327`) (:pr:`2323`)
-  Support NumPy's ``__array_ufunc__`` protocol (:pr:`2438`)

Bag
+++

-  Fix bug where reductions on bags with no partitions would fail (:pr:`2324`)
-  Add broadcasting and variadic ``db.map`` top-level function.  Also remove
   auto-expansion of tuples as map arguments (:pr:`2339`)
-  Rename ``Bag.concat`` to ``Bag.flatten`` (:pr:`2402`)

DataFrame
+++++++++

-  Parquet improvements (:pr:`2277`) (:pr:`2422`)

Core
++++

-  Move dask.async module to dask.local (:pr:`2318`)
-  Support callbacks with nested scheduler calls (:pr:`2397`)
-  Support pathlib.Path objects as uris  (:pr:`2310`)


0.14.3 / 2017-05-05
-------------------

DataFrame
+++++++++

-  Pandas 0.20.0 support

0.14.2 / 2017-05-03
-------------------

Array
+++++

-  Add da.indices (:pr:`2268`), da.tile (:pr:`2153`), da.roll (:pr:`2135`)
-  Simultaneously support drop_axis and new_axis in da.map_blocks (:pr:`2264`)
-  Rechunk and concatenate work with unknown chunksizes (:pr:`2235`) and (:pr:`2251`)
-  Support non-numpy container arrays, notably sparse arrays (:pr:`2234`)
-  Tensordot contracts over multiple axes (:pr:`2186`)
-  Allow delayed targets in da.store (:pr:`2181`)
-  Support interactions against lists and tuples (:pr:`2148`)
-  Constructor plugins for debugging (:pr:`2142`)
-  Multi-dimensional FFTs (single chunk) (:pr:`2116`)

Bag
+++

-  to_dataframe enforces consistent types (:pr:`2199`)

DataFrame
+++++++++

-  Set_index always fully sorts the index (:pr:`2290`)
-  Support compatibility with pandas 0.20.0 (:pr:`2249`), (:pr:`2248`), and (:pr:`2246`)
-  Support Arrow Parquet reader (:pr:`2223`)
-  Time-based rolling windows (:pr:`2198`)
-  Repartition can now create more partitions, not just less (:pr:`2168`)

Core
++++

-  Always use absolute paths when on POSIX file system (:pr:`2263`)
-  Support user provided graph optimizations (:pr:`2219`)
-  Refactor path handling (:pr:`2207`)
-  Improve fusion performance (:pr:`2129`), (:pr:`2131`), and (:pr:`2112`)


0.14.1 / 2017-03-22
-------------------

Array
+++++

-  Micro-optimize optimizations (:pr:`2058`)
-  Change slicing optimizations to avoid fusing raw numpy arrays (:pr:`2075`) (:pr:`2080`)
-  Dask.array operations now work on numpy arrays (:pr:`2079`)
-  Reshape now works in a much broader set of cases (:pr:`2089`)
-  Support deepcopy python protocol (:pr:`2090`)
-  Allow user-provided FFT implementations in ``da.fft`` (:pr:`2093`)

DataFrame
+++++++++

-  Fix to_parquet with empty partitions (:pr:`2020`)
-  Optional ``npartitions='auto'`` mode in ``set_index`` (:pr:`2025`)
-  Optimize shuffle performance (:pr:`2032`)
-  Support efficient repartitioning along time windows like ``repartition(freq='12h')`` (:pr:`2059`)
-  Improve speed of categorize (:pr:`2010`)
-  Support single-row dataframe arithmetic (:pr:`2085`)
-  Automatically avoid shuffle when setting index with a sorted column (:pr:`2091`)
-  Improve handling of integer-na handling in read_csv (:pr:`2098`)

Delayed
+++++++

-  Repeated attribute access on delayed objects uses the same key (:pr:`2084`)

Core
++++

-   Improve naming of nodes in dot visuals to avoid generic ``apply``
    (:pr:`2070`)
-   Ensure that worker processes have different random seeds (:pr:`2094`)


0.14.0 / 2017-02-24
-------------------

Array
+++++

- Fix corner cases with zero shape and misaligned values in ``arange``
  (:pr:`1902`), (:pr:`1904`), (:pr:`1935`), (:pr:`1955`), (:pr:`1956`)
- Improve concatenation efficiency (:pr:`1923`)
- Avoid hashing in ``from_array`` if name is provided (:pr:`1972`)

Bag
+++

- Repartition can now increase number of partitions (:pr:`1934`)
- Fix bugs in some reductions with empty partitions (:pr:`1939`), (:pr:`1950`),
  (:pr:`1953`)


DataFrame
+++++++++

- Support non-uniform categoricals (:pr:`1877`), (:pr:`1930`)
- Groupby cumulative reductions (:pr:`1909`)
- DataFrame.loc indexing now supports lists (:pr:`1913`)
- Improve multi-level groupbys (:pr:`1914`)
- Improved HTML and string repr for DataFrames (:pr:`1637`)
- Parquet append (:pr:`1940`)
- Add ``dd.demo.daily_stock`` function for teaching (:pr:`1992`)

Delayed
+++++++

- Add ``traverse=`` keyword to delayed to optionally avoid traversing nested
  data structures (:pr:`1899`)
- Support Futures in from_delayed functions (:pr:`1961`)
- Improve serialization of decorated delayed functions (:pr:`1969`)

Core
++++

- Improve windows path parsing in corner cases (:pr:`1910`)
- Rename tasks when fusing (:pr:`1919`)
- Add top level ``persist`` function (:pr:`1927`)
- Propagate ``errors=`` keyword in byte handling (:pr:`1954`)
- Dask.compute traverses Python collections (:pr:`1975`)
- Structural sharing between graphs in dask.array and dask.delayed (:pr:`1985`)


0.13.0 / 2017-01-02
-------------------

Array
+++++

- Mandatory dtypes on dask.array.  All operations maintain dtype information
  and UDF functions like map_blocks now require a dtype= keyword if it can not
  be inferred.  (:pr:`1755`)
- Support arrays without known shapes, such as arises when slicing arrays with
  arrays or converting dataframes to arrays (:pr:`1838`)
- Support mutation by setting one array with another (:pr:`1840`)
- Tree reductions for covariance and correlations.  (:pr:`1758`)
- Add SerializableLock for better use with distributed scheduling (:pr:`1766`)
- Improved atop support (:pr:`1800`)
- Rechunk optimization (:pr:`1737`), (:pr:`1827`)

Bag
+++

- Avoid wrong results when recomputing the same groupby twice (:pr:`1867`)

DataFrame
+++++++++

- Add ``map_overlap`` for custom rolling operations (:pr:`1769`)
- Add ``shift`` (:pr:`1773`)
- Add Parquet support (:pr:`1782`) (:pr:`1792`) (:pr:`1810`), (:pr:`1843`),
  (:pr:`1859`), (:pr:`1863`)
- Add missing methods combine, abs, autocorr, sem, nsmallest, first, last,
  prod, (:pr:`1787`)
- Approximate nunique (:pr:`1807`), (:pr:`1824`)
- Reductions with multiple output partitions (for operations like
  drop_duplicates) (:pr:`1808`), (:pr:`1823`) (:pr:`1828`)
- Add delitem and copy to DataFrames, increasing mutation support (:pr:`1858`)

Delayed
+++++++

- Changed behaviour for ``delayed(nout=0)`` and ``delayed(nout=1)``:
  ``delayed(nout=1)`` does not default to ``out=None`` anymore, and
  ``delayed(nout=0)`` is also enabled. I.e. functions with return
  tuples of length 1 or 0 can be handled correctly. This is especially
  handy, if functions with a variable amount of outputs are wrapped by
  ``delayed``. E.g. a trivial example:
  ``delayed(lambda *args: args, nout=len(vals))(*vals)``

Core
++++

- Refactor core byte ingest (:pr:`1768`), (:pr:`1774`)
- Improve import time (:pr:`1833`)


0.12.0 / 2016-11-03
-------------------

DataFrame
+++++++++
- Return a series when functions given to ``dataframe.map_partitions`` return
  scalars (:pr:`1515`)
- Fix type size inference for series (:pr:`1513`)
- ``dataframe.DataFrame.categorize`` no longer includes missing values
  in the ``categories``. This is for compatibility with a `pandas change <https://github.com/pydata/pandas/pull/10929>`_ (:pr:`1565`)
- Fix head parser error in ``dataframe.read_csv`` when some lines have quotes
  (:pr:`1495`)
- Add ``dataframe.reduction`` and ``series.reduction`` methods to apply generic
  row-wise reduction to dataframes and series (:pr:`1483`)
- Add ``dataframe.select_dtypes``, which mirrors the `pandas method <http://pandas.pydata.org/pandas-docs/version/0.18.1/generated/pandas.DataFrame.select_dtypes.html>`_ (:pr:`1556`)
- ``dataframe.read_hdf`` now supports reading ``Series`` (:pr:`1564`)
- Support Pandas 0.19.0 (:pr:`1540`)
- Implement ``select_dtypes`` (:pr:`1556`)
- String accessor works with indexes (:pr:`1561`)
- Add pipe method to dask.dataframe (:pr:`1567`)
- Add ``indicator`` keyword to merge (:pr:`1575`)
- Support Series in ``read_hdf`` (:pr:`1575`)
- Support Categories with missing values (:pr:`1578`)
- Support inplace operators like ``df.x += 1`` (:pr:`1585`)
- Str accessor passes through args and kwargs (:pr:`1621`)
- Improved groupby support for single-machine multiprocessing scheduler
  (:pr:`1625`)
- Tree reductions (:pr:`1663`)
- Pivot tables (:pr:`1665`)
- Add clip (:pr:`1667`), align (:pr:`1668`), combine_first (:pr:`1725`), and
  any/all (:pr:`1724`)
- Improved handling of divisions on dask-pandas merges (:pr:`1666`)
- Add ``groupby.aggregate`` method (:pr:`1678`)
- Add ``dd.read_table`` function (:pr:`1682`)
- Improve support for multi-level columns (:pr:`1697`) (:pr:`1712`)
- Support 2d indexing in ``loc`` (:pr:`1726`)
- Extend ``resample`` to include DataFrames (:pr:`1741`)
- Support dask.array ufuncs on dask.dataframe objects (:pr:`1669`)


Array
+++++
- Add information about how ``dask.array`` ``chunks`` argument work (:pr:`1504`)
- Fix field access with non-scalar fields in ``dask.array`` (:pr:`1484`)
- Add concatenate= keyword to atop to concatenate chunks of contracted dimensions
- Optimized slicing performance (:pr:`1539`) (:pr:`1731`)
- Extend ``atop`` with a ``concatenate=`` (:pr:`1609`) ``new_axes=``
  (:pr:`1612`) and ``adjust_chunks=`` (:pr:`1716`) keywords
- Add clip (:pr:`1610`) swapaxes (:pr:`1611`) round (:pr:`1708`) repeat
- Automatically align chunks in ``atop``-backed operations (:pr:`1644`)
- Cull dask.arrays on slicing (:pr:`1709`)

Bag
++++
- Fix issue with callables in ``bag.from_sequence`` being interpreted as
  tasks (:pr:`1491`)
- Avoid non-lazy memory use in reductions (:pr:`1747`)

Administration
++++++++++++++

- Added changelog (:pr:`1526`)
- Create new threadpool when operating from thread (:pr:`1487`)
- Unify example documentation pages into one (:pr:`1520`)
- Add versioneer for git-commit based versions (:pr:`1569`)
- Pass through node_attr and edge_attr keywords in dot visualization
  (:pr:`1614`)
- Add continuous testing for Windows with Appveyor (:pr:`1648`)
- Remove use of multiprocessing.Manager (:pr:`1653`)
- Add global optimizations keyword to compute (:pr:`1675`)
- Micro-optimize get_dependencies (:pr:`1722`)


0.11.0 / 2016-08-24
-------------------

Major Points
++++++++++++

DataFrames now enforce knowing full metadata (columns, dtypes) everywhere.
Previously we would operate in an ambiguous state when functions lost dtype
information (such as ``apply``).  Now all dataframes always know their dtypes
and raise errors asking for information if they are unable to infer (which
they usually can).  Some internal attributes like ``_pd`` and
``_pd_nonempty`` have been moved.

The internals of the distributed scheduler have been refactored to
transition tasks between explicit states.  This improves resilience,
reasoning about scheduling, plugin operation, and logging.  It also makes
the scheduler code easier to understand for newcomers.

Breaking Changes
++++++++++++++++

- The ``distributed.s3`` and ``distributed.hdfs`` namespaces are gone.  Use
  protocols in normal methods like ``read_text('s3://...'`` instead.
- ``Dask.array.reshape`` now errs in some cases where previously it would have
  create a very large number of tasks


0.10.2 / 2016-07-27
-------------------

- More Dataframe shuffles now work in distributed settings, ranging from
  setting-index to hash joins, to sorted joins and groupbys.
- Dask passes the full test suite when run when under in Python's
  optimized-OO mode.
- On-disk shuffles were found to produce wrong results in some
  highly-concurrent situations, especially on Windows.  This has been resolved
  by a fix to the partd library.
- Fixed a growth of open file descriptors that occurred under large data
  communications
- Support ports in the ``--bokeh-whitelist`` option ot dask-scheduler to better
  routing of web interface messages behind non-trivial network settings
- Some improvements to resilience to worker failure (though other known
  failures persist)
- You can now start an IPython kernel on any worker for improved debugging and
  analysis
- Improvements to ``dask.dataframe.read_hdf``, especially when reading from
  multiple files and docs


0.10.0 / 2016-06-13
-------------------

Major Changes
+++++++++++++

- This version drops support for Python 2.6
- Conda packages are built and served from conda-forge
- The ``dask.distributed`` executables have been renamed from dfoo to dask-foo.
  For example dscheduler is renamed to dask-scheduler
- Both Bag and DataFrame include a preliminary distributed shuffle.

Bag
++++

- Add task-based shuffle for distributed groupbys
- Add accumulate for cumulative reductions

DataFrame
+++++++++

- Add a task-based shuffle suitable for distributed joins, groupby-applys, and
  set_index operations.  The single-machine shuffle remains untouched (and
  much more efficient.)
- Add support for new Pandas rolling API with improved communication
  performance on distributed systems.
- Add ``groupby.std/var``
- Pass through S3/HDFS storage options in ``read_csv``
- Improve categorical partitioning
- Add eval, info, isnull, notnull for dataframes

Distributed
+++++++++++

- Rename executables like dscheduler to dask-scheduler
- Improve scheduler performance in the many-fast-tasks case (important for
  shuffling)
- Improve work stealing to be aware of expected function run-times and data
  sizes.  The drastically increases the breadth of algorithms that can be
  efficiently run on the distributed scheduler without significant user
  expertise.
- Support maximum buffer sizes in streaming queues
- Improve Windows support when using the Bokeh diagnostic web interface
- Support compression of very-large-bytestrings in protocol
- Support clean cancellation of submitted futures in Joblib interface

Other
+++++

- All dask-related projects (dask, distributed, s3fs, hdfs, partd) are now
  building conda packages on conda-forge.
- Change credential handling in s3fs to only pass around delegated credentials
  if explicitly given secret/key.  The default now is to rely on managed
  environments.  This can be changed back by explicitly providing a keyword
  argument.  Anonymous mode must be explicitly declared if desired.


0.9.0 / 2016-05-11
------------------

API Changes
+++++++++++

- ``dask.do`` and ``dask.value`` have been renamed to ``dask.delayed``
- ``dask.bag.from_filenames`` has been renamed to ``dask.bag.read_text``
- All S3/HDFS data ingest functions like ``db.from_s3`` or
  ``distributed.s3.read_csv`` have been moved into the plain ``read_text``,
  ``read_csv functions``, which now support protocols, like
  ``dd.read_csv('s3://bucket/keys*.csv')``

Array
+++++

- Add support for ``scipy.LinearOperator``
- Improve optional locking to on-disk data structures
- Change rechunk to expose the intermediate chunks

Bag
++++

- Rename ``from_filename``\ s to ``read_text``
- Remove ``from_s3`` in favor of ``read_text('s3://...')``

DataFrame
+++++++++

- Fixed numerical stability issue for correlation and covariance
- Allow no-hash ``from_pandas`` for speedy round-trips to and from-pandas
  objects
- Generally reengineered ``read_csv`` to be more in line with Pandas behavior
- Support fast ``set_index`` operations for sorted columns

Delayed
+++++++

- Rename ``do/value`` to ``delayed``
- Rename ``to/from_imperative`` to ``to/from_delayed``

Distributed
+++++++++++

- Move s3 and hdfs functionality into the dask repository
- Adaptively oversubscribe workers for very fast tasks
- Improve PyPy support
- Improve work stealing for unbalanced workers
- Scatter data efficiently with tree-scatters

Other
+++++

- Add lzma/xz compression support
- Raise a warning when trying to split unsplittable compression types, like
  gzip or bz2
- Improve hashing for single-machine shuffle operations
- Add new callback method for start state
- General performance tuning


0.8.1 / 2016-03-11
------------------

Array
+++++

- Bugfix for range slicing that could periodically lead to incorrect results.
- Improved support and resiliency of ``arg`` reductions (``argmin``, ``argmax``, etc.)

Bag
++++

- Add ``zip`` function

DataFrame
+++++++++

- Add ``corr`` and ``cov`` functions
- Add ``melt`` function
- Bugfixes for io to bcolz and hdf5


0.8.0 / 2016-02-20
------------------

Array
+++++

- Changed default array reduction split from 32 to 4
- Linear algebra, ``tril``, ``triu``, ``LU``, ``inv``, ``cholesky``,
  ``solve``, ``solve_triangular``, ``eye``, ``lstsq``, ``diag``, ``corrcoef``.

Bag
++++

- Add tree reductions
- Add range function
- drop ``from_hdfs`` function (better functionality now exists in hdfs3 and
  distributed projects)

DataFrame
+++++++++

- Refactor ``dask.dataframe`` to include a full empty pandas dataframe as
  metadata.  Drop the ``.columns`` attribute on Series
- Add Series categorical accessor, series.nunique, drop the ``.columns``
  attribute for series.
- ``read_csv`` fixes (multi-column parse_dates, integer column names, etc. )
- Internal changes to improve graph serialization

Other
+++++

- Documentation updates
- Add from_imperative and to_imperative functions for all collections
- Aesthetic changes to profiler plots
- Moved the dask project to a new dask organization


0.7.6 / 2016-01-05
------------------

Array
+++++
- Improve thread safety
- Tree reductions
- Add ``view``, ``compress``, ``hstack``, ``dstack``, ``vstack`` methods
- ``map_blocks`` can now remove and add dimensions

DataFrame
+++++++++
- Improve thread safety
- Extend sampling to include replacement options

Imperative
++++++++++
- Removed optimization passes that fused results.

Core
++++

- Removed ``dask.distributed``
- Improved performance of blocked file reading
- Serialization improvements
- Test Python 3.5


0.7.4 / 2015-10-23
------------------

This was mostly a bugfix release. Some notable changes:

- Fix minor bugs associated with the release of numpy 1.10 and pandas 0.17
- Fixed a bug with random number generation that would cause repeated blocks
  due to the birthday paradox
- Use locks in ``dask.dataframe.read_hdf`` by default to avoid concurrency
  issues
- Change ``dask.get`` to point to ``dask.async.get_sync`` by default
- Allow visualization functions to accept general graphviz graph options like
  rankdir='LR'
- Add reshape and ravel to ``dask.array``
- Support the creation of ``dask.arrays`` from ``dask.imperative`` objects

Deprecation
+++++++++++

This release also includes a deprecation warning for ``dask.distributed``, which
will be removed in the next version.

Future development in distributed computing for dask is happening here:
https://distributed.readthedocs.io . General feedback on that project is most
welcome from this community.


0.7.3 / 2015-09-25
------------------

Diagnostics
+++++++++++
- A utility for profiling memory and cpu usage has been added to the
  ``dask.diagnostics`` module.

DataFrame
+++++++++
This release improves coverage of the pandas API. Among other things
it includes ``nunique``, ``nlargest``, ``quantile``. Fixes encoding issues
with reading non-ascii csv files. Performance improvements and  bug fixes
with resample. More flexible read_hdf with globbing. And many more. Various
bug fixes in ``dask.imperative`` and ``dask.bag``.


0.7.0 / 2015-08-15
------------------

DataFrame
+++++++++
This release includes significant bugfixes and alignment with the Pandas API.
This has resulted both from use and from recent involvement by Pandas core
developers.

- New operations: query, rolling operations, drop
- Improved operations: quantiles, arithmetic on full dataframes, dropna,
  constructor logic, merge/join, elemwise operations, groupby aggregations

Bag
++++
- Fixed a bug in fold where with a null default argument

Array
+++++
- New operations: da.fft module, da.image.imread

Infrastructure
++++++++++++++
- The array and dataframe collections create graphs with deterministic keys.
  These tend to be longer (hash strings) but should be consistent between
  computations.  This will be useful for caching in the future.
- All collections (Array, Bag, DataFrame) inherit from common subclass


0.6.1 / 2015-07-23
------------------

Distributed
+++++++++++
- Improved (though not yet sufficient) resiliency for ``dask.distributed``
  when workers die

DataFrame
+++++++++
- Improved writing to various formats, including to_hdf, to_castra, and
  to_csv
- Improved creation of dask DataFrames from dask Arrays and Bags
- Improved support for categoricals and various other methods

Array
+++++
- Various bug fixes
- Histogram function

Scheduling
++++++++++
- Added tie-breaking ordering of tasks within parallel workloads to
  better handle and clear intermediate results

Other
+++++
- Added the dask.do function for explicit construction of graphs with
  normal python code
- Traded pydot for graphviz library for graph printing to support Python3
- There is also a gitter chat room and a stackoverflow tag


.. _`Guido Imperiale`: https://github.com/crusaderky
.. _`John A Kirkham`: https://github.com/jakirkham
.. _`Matthew Rocklin`: https://github.com/mrocklin
.. _`Jim Crist`: https://github.com/jcrist
.. _`James Bourbeau`: https://github.com/jrbourbeau
.. _`James Munroe`: https://github.com/jmunroe
.. _`Thomas Caswell`: https://github.com/tacaswell
.. _`Tom Augspurger`: https://github.com/tomaugspurger
.. _`Uwe Korn`: https://github.com/xhochy
.. _`Christopher Prohm`: https://github.com/chmp
.. _`@xwang777`: https://github.com/xwang777
.. _`@fjetter`: https://github.com/fjetter
.. _`@Ced4`: https://github.com/Ced4
.. _`Ian Hopkinson`: https://https://github.com/IanHopkinson
.. _`Stephan Hoyer`: https://github.com/shoyer
.. _`Albert DeFusco`: https://github.com/AlbertDeFusco
.. _`Markus Gonser`: https://github.com/magonser
.. _`Martijn Arts`: https://github.com/mfaafm
.. _`Jon Mease`: https://github.com/jmmease
.. _`Xander Johnson`: https://github.com/metasyn
.. _`Nir`: https://github.com/nirizr
.. _`Keisuke Fujii`: https://github.com/fujiisoup
.. _`Roman Yurchak`: https://github.com/rth
.. _`Max Epstein`: https://github.com/MaxPowerWasTaken
.. _`Simon Perkins`: https://github.com/sjperkins
.. _`Richard Postelnik`: https://github.com/postelrich
.. _`Daniel Collins`: https://github.com/dancollins34
.. _`Gabriele Lanaro`: https://github.com/gabrielelanaro
.. _`Jörg Dietrich`: https://github.com/joergdietrich
.. _`Christopher Ren`: https://github.com/cr458
.. _`Martin Durant`: https://github.com/martindurant
.. _`Thrasibule`: https://github.com/thrasibule
.. _`Dieter Weber`: https://github.com/uellue
.. _`Apostolos Vlachopoulos`: https://github.com/avlahop
.. _`Jesse Vogt`: https://github.com/jessevogt
.. _`Pierre Bartet`: https://github.com/Pierre-Bartet
.. _`Scott Sievert`: https://github.com/stsievert
.. _`Jeremy Chen`: https://github.com/convexset
.. _`Marc Pfister`: https://github.com/drwelby
.. _`Matt Lee`: https://github.com/mathewlee11
.. _`Yu Feng`: https://github.com/rainwoodman
.. _`@andrethrill`: https://github.com/andrethrill
.. _`@beomi`: https://github.com/beomi
.. _`Henrique Ribeiro`: https://github.com/henriqueribeiro
.. _`Marco Rossi`: https://github.com/m-rossi
.. _`Mike Neish`: https://github.com/neishm
.. _`Mark Harfouche`: https://github.com/hmaarrfk
.. _`George Sakkis`: https://github.com/gsakkis
.. _`Ziyao Wei`: https://github.com/ZiyaoWei
.. _`Itamar Turner-Trauring`: https://github.com/itamarst
.. _`Jacob Tomlinson`: https://github.com/jacobtomlinson
.. _`Elliott Sales de Andrade`: https://github.com/QuLogic
.. _`Gerome Pistre`: https://github.com/GPistre
.. _`Cloves Almeida`: https://github.com/cjalmeida
.. _`Tobias de Jong`: https://github.com/tadejong
.. _`Irina Truong`: https://github.com/j-bennet
.. _`Eric Bonfadini`: https://github.com/eric-bonfadini
.. _`Danilo Horta`: https://github.com/horta
.. _`@hugovk`: https://github.com/hugovk
.. _`Jan Margeta`: https://github.com/jmargeta
.. _`John Mrziglod`: https://github.com/JohnMrziglod
.. _`Christoph Moehl`: https://github.com/cmohl2013
.. _`Anderson Banihirwe`: https://github.com/andersy005
.. _`@javad94`: https://github.com/javad94
.. _`Daniel Rothenberg`: https://github.com/darothen
.. _`Hans Moritz Günther`: https://github.com/hamogu
.. _`@rtobar`: https://github.com/rtobar
.. _`Julia Signell`: https://github.com/jsignell
