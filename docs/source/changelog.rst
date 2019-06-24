Changelog
=========

2.0.0 / 2019-06-25
------------------

Array
+++++

-  Support automatic chunking in da.indices (:pr:`4981`) `James Bourbeau`_
-  Err if there are no arrays to stack (:pr:`4975`) `John A Kirkham`_
-  Asymmetrical Array Overlap (:pr:`4863`) `Michael Eaton`_
-  Dispatch concatenate where possible within dask array (:pr:`4669`) `Hameer Abbasi`_
-  Fix tokenization of memmapped numpy arrays on different part of same file (:pr:`4931`) `Henry Pinkard`_
-  Preserve NumPy condition in da.asarray to preserve output shape (:pr:`4945`) `Alistair Miles`_
-  Expand foo_like_safe usage (:pr:`4946`) `Peter Andreas Entschev`_
-  Defer order/casting einsum parameters to NumPy implementation (:pr:`4914`) `Peter Andreas Entschev`_
-  Remove numpy warning in moment calculation (:pr:`4921`) `Matthew Rocklin`_
-  Fix meta_from_array to support Xarray test suite (:pr:`4938`) `Matthew Rocklin`_
-  Cache chunk boundaries for integer slicing (:pr:`4923`) `Bruce Merry`_
-  Drop size 0 arrays in concatenate (:pr:`4167`) `John A Kirkham`_
-  Raise ValueError if concatenate is given no arrays (:pr:`4927`) `John A Kirkham`_
-  Promote types in `concatenate` using `_meta` (:pr:`4925`) `John A Kirkham`_
-  Add chunk type to html repr in Dask array (:pr:`4895`) `Matthew Rocklin`_
-  Add Dask Array._meta attribute (:pr:`4543`) `Peter Andreas Entschev`_
    -  Fix _meta slicing of flexible types (:pr:`4912`) `Peter Andreas Entschev`_
    -  Minor meta construction cleanup in concatenate (:pr:`4937`) `Peter Andreas Entschev`_
    -  Further relax Array meta checks for Xarray (:pr:`4944`) `Matthew Rocklin`_
    -  Support meta= keyword in da.from_delayed (:pr:`4972`) `Matthew Rocklin`_
    -  Concatenate meta along axis (:pr:`4977`) `John A Kirkham`_
    -  Use meta in stack (:pr:`4976`) `John A Kirkham`_
    -  Move blockwise_meta to more general compute_meta function (:pr:`4954`) `Matthew Rocklin`_
-  Alias .partitions to .blocks attribute of dask arrays (:pr:`4853`) `Genevieve Buckley`_
-  Drop outdated `numpy_compat` functions (:pr:`4850`) `John A Kirkham`_
-  Allow da.eye to support arbitrary chunking sizes with chunks='auto'  (:pr:`4834`) `Anderson Banihirwe`_
-  Fix CI warnings in dask.array tests (:pr:`4805`) `Tom Augspurger`_
-  Make map_blocks work with drop_axis + block_info (:pr:`4831`) `Bruce Merry`_
-  Add SVG image and table in Array._repr_html_ (:pr:`4794`) `Matthew Rocklin`_
-  ufunc: avoid __array_wrap__ in favor of __array_function__ (:pr:`4708`) `Peter Andreas Entschev`_
-  Ensure trivial padding returns the original array (:pr:`4990`) `John A Kirkham`_
-  Test ``da.block`` with 0-size arrays (:pr:`4991`) `John A Kirkham`_


Core
++++

-  **Drop Python 2.7** (:pr:`4919`) `Jim Crist`_
-  Quiet dependency installs in CI (:pr:`4960`) `Tom Augspurger`_
-  Raise on warnings in tests (:pr:`4916`) `Tom Augspurger`_
-  Add a diagnostics extra to setup.py (includes bokeh) (:pr:`4924`) `John A Kirkham`_
-  Add newline delimter keyword to OpenFile (:pr:`4935`) `btw08`_
-  Overload HighLevelGraphs values method (:pr:`4918`) `James Bourbeau`_
-  Add __await__ method to Dask collections (:pr:`4901`) `Matthew Rocklin`_
-  Also ignore AttributeErrors which may occur if snappy (not python-snappy) is installed (:pr:`4908`) `Mark Bell`_
-  Canonicalize key names in config.rename (:pr:`4903`) `Ian Bolliger`_
-  Bump minimum partd to 0.3.10 (:pr:`4890`) `Tom Augspurger`_
-  Catch async def SyntaxError (:pr:`4836`) `James Bourbeau`_
-  catch IOError in ensure_file (:pr:`4806`) `Justin Poehnelt`_
-  Cleanup CI warnings (:pr:`4798`) `Tom Augspurger`_
-  Move distributed's parse and format functions to dask.utils (:pr:`4793`) `Matthew Rocklin`_
-  Apply black formatting (:pr:`4983`) `James Bourbeau`_
-  Package license file in wheels (:pr:`4988`) `John A Kirkham`_


DataFrame
+++++++++

-  Add an optional partition_size parameter to repartition (:pr:`4416`) `George Sakkis`_
-  merge_asof and prefix_reduction (:pr:`4877`) `Cody Johnson`_
-  Allow dataframes to be indexed by dask arrays (:pr:`4882`) `Endre Mark Borza`_
-  Avoid deprecated message parameter in pytest.raises (:pr:`4962`) `James Bourbeau`_
-  Update test_to_records to test with lengths argument(:pr:`4515`) `asmith26`_
-  Remove pandas pinning in Dataframe accessors (:pr:`4955`) `Matthew Rocklin`_
-  Fix correlation of series with same names (:pr:`4934`) `Philipp S. Sommer`_
-  Map Dask Series to Dask Series (:pr:`4872`) `Justin Waugh`_
-  Warn in dd.merge on dtype warning (:pr:`4917`) `mcsoini`_
-  Add groupby Covariance/Correlation (:pr:`4889`) `Ben Zaitlen`_
-  keep index name with to_datetime (:pr:`4905`) `Ian Bolliger`_
-  Add Parallel variance computation for dataframes (:pr:`4865`) `Ksenia Bobrova`_
-  Add divmod implementation to arrays and dataframes (:pr:`4884`) `Henrique Ribeiro`_
-  Add documentation for dataframe reshape methods (:pr:`4896`) `tpanza`_
-  Avoid use of pandas.compat (:pr:`4881`) `Tom Augspurger`_
-  Added accessor registration for Series, DataFrame, and Index (:pr:`4829`) `Tom Augspurger`_
-  Add read_function keyword to read_json (:pr:`4810`) `Richard J Zamora`_
-  Provide full type name in check_meta (:pr:`4819`) `Matthew Rocklin`_
-  Correctly estimate bytes per row in read_sql_table (:pr:`4807`) `Lijo Jose`_
-  Adding support of non-numeric data to describe() (:pr:`4791`) `Ksenia Bobrova`_
-  Scalars for extension dtypes. (:pr:`4459`) `Tom Augspurger`_
-  Call head before compute in dd.from_delayed (:pr:`4802`) `Matthew Rocklin`_
-  Add support for rolling operations with larger window that partition size in DataFrames with Time-based index (:pr:`4796`) `Jorge Pessoa`_
-  Update groupby-apply doc with warning (:pr:`4800`) `Tom Augspurger`_
-  Change groupby-ness tests in `_maybe_slice` (:pr:`4786`) `Ben Zaitlen`_
-  Add master best practices document (:pr:`4745`) `Matthew Rocklin`_
-  Add document for how Dask works with GPUs (:pr:`4792`) `Matthew Rocklin`_
-  Add cli API docs (:pr:`4788`) `James Bourbeau`_
-  Ensure concat output has coherent dtypes (:pr:`4692`) `Guillaume Lemaitre`_
-  Fixes pandas_datareader dependencies installation (:pr:`4989`) `James Bourbeau`_
-  Accept pathlib.Path as pattern in read_hdf (:pr:`3335`) `Jörg Dietrich`_


Documentation
+++++++++++++

-  Move CLI API docs to relavant pages (:pr:`4980`) `James Bourbeau`_
-  Add to_datetime function to dataframe API docs `Matthew Rocklin`_
-  Add documentation entry for dask.array.ma.average (:pr:`4970`) `Bouwe Andela`_
-  Add bag.read_avro to bag API docs (:pr:`4969`) `James Bourbeau`_
-  Fix typo (:pr:`4968`) `mbarkhau`_
-  Docs: Drop support for Python 2.7 (:pr:`4932`) `Hugo`_
-  Remove requirement to modify changelog (:pr:`4915`) `Matthew Rocklin`_
-  Add documentation about meta column order (:pr:`4887`) `Tom Augspurger`_
-  Add documentation note in DataFrame.shift (:pr:`4886`) `Tom Augspurger`_
-  Docs: Fix typo (:pr:`4868`) `Paweł Kordek`_
-  Put do/don't into boxes for delayed best practice docs (:pr:`3821`) `Martin Durant`_
-  Doc fixups (:pr:`2528`) `Tom Augspurger`_
-  Add quansight to paid support doc section (:pr:`4838`) `Martin Durant`_
-  Add document for custom startup (:pr:`4833`) `Matthew Rocklin`_
-  Allow `utils.derive_from` to accept functions, apply across array (:pr:`4804`) `Martin Durant`_
-  Add "Avoid Large Partitions" section to best practices (:pr:`4808`) `Matthew Rocklin`_
-  Update URL for joblib to new website hosting their doc (:pr:`4816`) `Christian Hudon`_

1.2.2 / 2019-05-08
------------------

Array
+++++

- Clarify regions kwarg to array.store (:pr:`4759`) `Martin Durant`_
- Add dtype= parameter to da.random.randint (:pr:`4753`) `Matthew Rocklin`_
- Use "row major" rather than "C order" in docstring (:pr:`4452`) `@asmith26`_
- Normalize Xarray datasets to Dask arrays (:pr:`4756`) `Matthew Rocklin`_
- Remove normed keyword in da.histogram (:pr:`4755`) `Matthew Rocklin`_

Bag
+++

- Add key argument to Bag.distinct (:pr:`4423`) `Daniel Severo`_

Core
++++

- Add core dask config file (:pr:`4774`) `Matthew Rocklin`_
- Add core dask config file to MANIFEST.in (:pr:`4780`) `James Bourbeau`_
- Enabling glob with HTTP file-system (:pr:`3926`) `Martin Durant`_
- HTTPFile.seek with whence=1 (:pr:`4751`) `Martin Durant`_
- Remove config key normalization (:pr:`4742`) `Jim Crist`_

DataFrame
+++++++++

- Remove explicit references to Pandas in dask.dataframe.groupby (:pr:`4778`) `Matthew Rocklin`_
- Add support for group_keys kwarg in DataFrame.groupby() (:pr:`4771`) `Brian Chu`_
- Describe doc (:pr:`4762`) `Martin Durant`_
- Remove explicit pandas check in cumulative aggregations (:pr:`4765`) `Nick Becker`_
- Added meta for read_json and test (:pr:`4588`) `Abhinav Ralhan`_
- Add test for dtype casting (:pr:`4760`) `Martin Durant`_
- Document alignment in map_partitions (:pr:`4757`) `Jim Crist`_
- Implement Series.str.split(expand=True) (:pr:`4744`) `Matthew Rocklin`_

Documentation
+++++++++++++

- Tweaks to develop.rst from trying to run tests (:pr:`4772`) `Christian Hudon`_
- Add document describing phases of computation (:pr:`4766`) `Matthew Rocklin`_
- Point users to Dask-Yarn from spark documentation (:pr:`4770`) `Matthew Rocklin`_
- Update images in delayed doc to remove labels (:pr:`4768`) `Martin Durant`_
- Explain intermediate storage for dask arrays (:pr:`4025`) `John A Kirkham`_
- Specify bash code-block in array best practices (:pr:`4764`) `James Bourbeau`_
- Add array best practices doc (:pr:`4705`) `Matthew Rocklin`_
- Update optimization docs now that cull is not automatic (:pr:`4752`) `Matthew Rocklin`_


1.2.1 / 2019-04-29
------------------

Array
+++++

-  Fix map_blocks with block_info and broadcasting (:pr:`4737`) `Bruce Merry`_
-  Make 'minlength' keyword argument optional in da.bincount (:pr:`4684`) `Genevieve Buckley`_
-  Add support for map_blocks with no array arguments (:pr:`4713`) `Bruce Merry`_
-  Add dask.array.trace (:pr:`4717`) `Danilo Horta`_
-  Add sizeof support for cupy.ndarray (:pr:`4715`) `Peter Andreas Entschev`_
-  Add name kwarg to from_zarr (:pr:`4663`) `Michael Eaton`_
-  Add chunks='auto' to from_array (:pr:`4704`) `Matthew Rocklin`_
-  Raise TypeError if dask array is given as shape for da.ones, zeros, empty or full (:pr:`4707`) `Genevieve Buckley`_
-  Add TileDB backend (:pr:`4679`) `Isaiah Norton`_

Core
++++

-  Delay long list arguments (:pr:`4735`) `Matthew Rocklin`_
-  Bump to numpy >= 1.13, pandas >= 0.21.0 (:pr:`4720`) `Jim Crist`_
-  Remove file "test" (:pr:`4710`) `James Bourbeau`_
-  Reenable development build, uses upstream libraries (:pr:`4696`) `Peter Andreas Entschev`_
-  Remove assertion in HighLevelGraph constructor (:pr:`4699`) `Matthew Rocklin`_

DataFrame
+++++++++

-  Change cum-aggregation last-nonnull-value algorithm (:pr:`4736`) `Nick Becker`_
-  Fixup series-groupby-apply (:pr:`4738`) `Jim Crist`_
-  Refactor array.percentile and dataframe.quantile to use t-digest (:pr:`4677`) `Janne Vuorela`_
-  Allow naive concatenation of sorted dataframes (:pr:`4725`) `Matthew Rocklin`_
-  Fix perf issue in dd.Series.isin (:pr:`4727`) `Jim Crist`_
-  Remove hard pandas dependency for melt by using methodcaller (:pr:`4719`) `Nick Becker`_
-  A few dataframe metadata fixes (:pr:`4695`) `Jim Crist`_
-  Add Dataframe.replace (:pr:`4714`) `Matthew Rocklin`_
-  Add 'threshold' parameter to pd.DataFrame.dropna (:pr:`4625`) `Nathan Matare`_

Documentation
+++++++++++++

-   Add warning about derived docstrings early in the docstring (:pr:`4716`) `Matthew Rocklin`_
-   Create dataframe best practices doc (:pr:`4703`) `Matthew Rocklin`_
-   Uncomment dask_sphinx_theme (:pr:`4728`) `James Bourbeau`_
-   Fix minor typo fix in a Queue/fire_and_forget example (:pr:`4709`) `Matthew Rocklin`_
-   Update from_pandas docstring to match signature (:pr:`4698`) `James Bourbeau`_

1.2.0 / 2019-04-12
------------------

Array
+++++

-  Fixed mean() and moment() on sparse arrays (:pr:`4525`) `Peter Andreas Entschev`_
-  Add test for NEP-18. (:pr:`4675`) `Hameer Abbasi`_
-  Allow None to say "no chunking" in normalize_chunks (:pr:`4656`) `Matthew Rocklin`_
-  Fix limit value in auto_chunks (:pr:`4645`) `Matthew Rocklin`_

Core
++++

-  Updated diagnostic bokeh test for compatibility with bokeh>=1.1.0 (:pr:`4680`) `Philipp Rudiger`_
-  Adjusts codecov's target/threshold, disable patch (:pr:`4671`) `Peter Andreas Entschev`_
-  Always start with empty http buffer, not None (:pr:`4673`) `Martin Durant`_

DataFrame
+++++++++

-  Propagate index dtype and name when create dask dataframe from array (:pr:`4686`) `Henrique Ribeiro`_
-  Fix ordering of quantiles in describe (:pr:`4647`) `gregrf`_
-  Clean up and document rearrange_column_by_tasks (:pr:`4674`) `Matthew Rocklin`_
-  Mark some parquet tests xfail (:pr:`4667`) `Peter Andreas Entschev`_
-  Fix parquet breakages with arrow 0.13.0 (:pr:`4668`) `Martin Durant`_
-  Allow sample to be False when reading CSV from a remote URL (:pr:`4634`) `Ian Rose`_
-  Fix timezone metadata inference on parquet load (:pr:`4655`) `Martin Durant`_
-  Use is_dataframe/index_like in dd.utils (:pr:`4657`) `Matthew Rocklin`_
-  Add min_count parameter to groupby sum method (:pr:`4648`) `Henrique Ribeiro`_
-  Correct quantile to handle unsorted quantiles (:pr:`4650`) `gregrf`_

Documentation
+++++++++++++

-  Add delayed extra dependencies to install docs (:pr:`4660`) `James Bourbeau`_


1.1.5 / 2019-03-29
------------------

Array
+++++

-  Ensure that we use the dtype keyword in normalize_chunks (:pr:`4646`) `Matthew Rocklin`_

Core
++++

-  Use recursive glob in LocalFileSystem (:pr:`4186`) `Brett Naul`_
-  Avoid YAML deprecation (:pr:`4603`)
-  Fix CI and add set -e (:pr:`4605`) `James Bourbeau`_
-  Support builtin sequence types in dask.visualize (:pr:`4602`)
-  unpack/repack orderedDict (:pr:`4623`) `Justin Poehnelt`_
-  Add da.random.randint to API docs (:pr:`4628`) `James Bourbeau`_
-  Add zarr to CI environment (:pr:`4604`) `James Bourbeau`_
-  Enable codecov (:pr:`4631`) `Peter Andreas Entschev`_

DataFrame
+++++++++

-  Support setting the index (:pr:`4565`)
-  DataFrame.itertuples accepts index, name kwargs (:pr:`4593`) `Dan O'Donovan`_
-  Support non-Pandas series in dd.Series.unique (:pr:`4599`) `Ben Zaitlen`_
-  Replace use of explicit type check with ._is_partition_type predicate (:pr:`4533`)
-  Remove additional pandas warnings in tests (:pr:`4576`)
-  Check object for name/dtype attributes rather than type (:pr:`4606`)
-  Fix comparison against pd.Series (:pr:`4613`) `amerkel2`_
-  Fixing warning from setting categorical codes to floats (:pr:`4624`) `Julia Signell`_
-  Fix renaming on index to_frame method (:pr:`4498`) `Henrique Ribeiro`_
-  Fix divisions when joining two single-partition dataframes (:pr:`4636`) `Justin Waugh`_
-  Warn if partitions overlap in compute_divisions (:pr:`4600`) `Brian Chu`_
-  Give informative meta= warning (:pr:`4637`) `Matthew Rocklin`_
-  Add informative error message to Series.__getitem__ (:pr:`4638`) `Matthew Rocklin`_
-  Add clear exception message when using index or index_col in read_csv (:pr:`4651`) `Álvaro Abella Bascarán`_

Documentation
+++++++++++++

-  Add documentation for custom groupby aggregations (:pr:`4571`)
-  Docs dataframe joins (:pr:`4569`)
-  Specify fork-based contributions  (:pr:`4619`) `James Bourbeau`_
-  correct to_parquet example in docs (:pr:`4641`) `Aaron Fowles`_
-  Update and secure several references (:pr:`4649`) `Søren Fuglede Jørgensen`_


1.1.4 / 2019-03-08
------------------

Array
+++++

-  Use mask selection in compress (:pr:`4548`) `John A Kirkham`_
-  Use `asarray` in `extract` (:pr:`4549`) `John A Kirkham`_
-  Use correct dtype when test concatenation. (:pr:`4539`) `Elliott Sales de Andrade`_
-  Fix CuPy tests or properly marks as xfail (:pr:`4564`) `Peter Andreas Entschev`_

Core
++++

-  Fix local scheduler callback to deal with custom caching (:pr:`4542`) `Yu Feng`_
-  Use parse_bytes in read_bytes(sample=...) (:pr:`4554`) `Matthew Rocklin`_

DataFrame
+++++++++

-  Fix up groupby-standard deviation again on object dtype keys (:pr:`4541`) `Matthew Rocklin`_
-  TST/CI: Updates for pandas 0.24.1 (:pr:`4551`) `Tom Augspurger`_
-  Add ability to control number of unique elements in timeseries (:pr:`4557`) `Matthew Rocklin`_
-  Add support in read_csv for parameter skiprows for other iterables (:pr:`4560`) `@JulianWgs`_

Documentation
+++++++++++++

-  DataFrame to Array conversion and unknown chunks (:pr:`4516`) `Scott Sievert`_
-  Add docs for random array creation (:pr:`4566`) `Matthew Rocklin`_
-  Fix typo in docstring (:pr:`4572`) `Shyam Saladi`_


1.1.3 / 2019-03-01
------------------

Array
+++++

-  Modify mean chunk functions to return dicts rather than arrays (:pr:`4513`) `Matthew Rocklin`_
-  Change sparse installation in CI for NumPy/Python2 compatibility (:pr:`4537`) `Matthew Rocklin`_

DataFrame
+++++++++

-  Make merge dispatchable on pandas/other dataframe types (:pr:`4522`) `Matthew Rocklin`_
-  read_sql_table - datetime index fix and  index type checking (:pr:`4474`) `Joe Corbett`_
-  Use generalized form of index checking (is_index_like) (:pr:`4531`) `Ben Zaitlen`_
-  Add tests for groupby reductions with object dtypes (:pr:`4535`) `Matthew Rocklin`_
-  Fixes #4467 : Updates time_series for pandas deprecation (:pr:`4530`) `@HSR05`_

Documentation
+++++++++++++

-  Add missing method to documentation index (:pr:`4528`) `Bart Broere`_


1.1.2 / 2019-02-25
------------------

Array
+++++

-  Fix another unicode/mixed-type edge case in normalize_array (:pr:`4489`) `Marco Neumann`_
-  Add dask.array.diagonal (:pr:`4431`) `Danilo Horta`_
-  Call asanyarray in unify_chunks (:pr:`4506`) `Jim Crist`_
-  Modify moment chunk functions to return dicts (:pr:`4519`) `Peter Andreas Entschev`_


Bag
+++

-  Don't inline output keys in dask.bag (:pr:`4464`) `Jim Crist`_
-  Ensure that bag.from_sequence always includes at least one partition (:pr:`4475`) `Anderson Banihirwe`_
-  Implement out_type for bag.fold (:pr:`4502`) `Matthew Rocklin`_
-  Remove map from bag keynames (:pr:`4500`) `Matthew Rocklin`_
-  Avoid itertools.repeat in map_partitions (:pr:`4507`) `Matthew Rocklin`_


DataFrame
+++++++++

-  Fix relative path parsing on windows when using fastparquet (:pr:`4445`) `Janne Vuorela`_
-  Fix bug in pyarrow and hdfs (:pr:`4453`) (:pr:`4455`) `Michał Jastrzębski`_
-  df getitem with integer slices is not implemented (:pr:`4466`) `Jim Crist`_
-  Replace cudf-specific code with dask-cudf import (:pr:`4470`) `Matthew Rocklin`_
-  Avoid groupby.agg(callable) in groupby-var (:pr:`4482`) `Matthew Rocklin`_
-  Consider uint types as numerical in check_meta (:pr:`4485`) `Marco Neumann`_
-  Fix some typos in groupby comments (:pr:`4494`) `Daniel Saxton`_
-  Add error message around set_index(inplace=True) (:pr:`4501`) `Matthew Rocklin`_
-  meta_nonempty works with categorical index (:pr:`4505`) `Jim Crist`_
-  Add module name to expected meta error message (:pr:`4499`) `Matthew Rocklin`_
-  groupby-nunique works on empty chunk (:pr:`4504`) `Jim Crist`_
-  Propogate index metadata if not specified (:pr:`4509`) `Jim Crist`_

Documentation
+++++++++++++

-  Update docs to use ``from_zarr`` (:pr:`4472`) `John A Kirkham`_
-  DOC: add section of `Using Other S3-Compatible Services` for remote-data-services (:pr:`4405`) `Aploium`_
-  Fix header level of section in changelog (:pr:`4483`) `Bruce Merry`_
-  Add quotes to pip install [skip-ci] (:pr:`4508`) `James Bourbeau`_

Core
++++

-  Extend started_cbs AFTER state is initialized (:pr:`4460`) `Marco Neumann`_
-  Fix bug in HTTPFile._fetch_range with headers (:pr:`4479`) (:pr:`4480`) `Ross Petchler`_
-  Repeat optimize_blockwise for diamond fusion (:pr:`4492`) `Matthew Rocklin`_


1.1.1 / 2019-01-31
------------------

Array
+++++

-  Add support for cupy.einsum (:pr:`4402`) `Johnnie Gray`_
-  Provide byte size in chunks keyword (:pr:`4434`) `Adam Beberg`_
-  Raise more informative error for histogram bins and range (:pr:`4430`) `James Bourbeau`_

DataFrame
+++++++++

-  Lazily register more cudf functions and move to backends file (:pr:`4396`) `Matthew Rocklin`_
-  Fix ORC tests for pyarrow 0.12.0 (:pr:`4413`) `Jim Crist`_
-  rearrange_by_column: ensure that shuffle arg defaults to 'disk' if it's None in dask.config (:pr:`4414`) `George Sakkis`_
-  Implement filters for _read_pyarrow (:pr:`4415`) `George Sakkis`_
-  Avoid checking against types in is_dataframe_like (:pr:`4418`) `Matthew Rocklin`_
-  Pass username as 'user' when using pyarrow (:pr:`4438`) `Roma Sokolov`_

Delayed
+++++++

-  Fix DelayedAttr return value (:pr:`4440`) `Matthew Rocklin`_

Documentation
+++++++++++++

-  Use SVG for pipeline graphic (:pr:`4406`) `John A Kirkham`_
-  Add doctest-modules to py.test documentation (:pr:`4427`) `Daniel Severo`_

Core
++++

-  Work around psutil 5.5.0 not allowing pickling Process objects `Janne Vuorela`_


1.1.0 / 2019-01-18
------------------

Array
+++++

-  Fix the average function when there is a masked array (:pr:`4236`) `Damien Garaud`_
-  Add allow_unknown_chunksizes to hstack and vstack (:pr:`4287`) `Paul Vecchio`_
-  Fix tensordot for 27+ dimensions (:pr:`4304`) `Johnnie Gray`_
-  Fixed block_info with axes. (:pr:`4301`) `Tom Augspurger`_
-  Use safe_wraps for matmul (:pr:`4346`) `Mark Harfouche`_
-  Use chunks="auto" in array creation routines (:pr:`4354`) `Matthew Rocklin`_
-  Fix np.matmul in dask.array.Array.__array_ufunc__ (:pr:`4363`) `Stephan Hoyer`_
-  COMPAT: Re-enable multifield copy->view change (:pr:`4357`) `Diane Trout`_
-  Calling np.dtype on a delayed object works (:pr:`4387`) `Jim Crist`_
-  Rework normalize_array for numpy data (:pr:`4312`) `Marco Neumann`_

DataFrame
+++++++++

-  Add fill_value support for series comparisons (:pr:`4250`) `James Bourbeau`_
-  Add schema name in read_sql_table for empty tables (:pr:`4268`) `Mina Farid`_
-  Adjust check for bad chunks in map_blocks (:pr:`4308`) `Tom Augspurger`_
-  Add dask.dataframe.read_fwf (:pr:`4316`) `@slnguyen`_
-  Use atop fusion in dask dataframe (:pr:`4229`) `Matthew Rocklin`_
-  Use parallel_types() in from_pandas (:pr:`4331`) `Matthew Rocklin`_
-  Change DataFrame._repr_data to method (:pr:`4330`) `Matthew Rocklin`_
-  Install pyarrow fastparquet for Appveyor (:pr:`4338`) `Gábor Lipták`_
-  Remove explicit pandas checks and provide cudf lazy registration (:pr:`4359`) `Matthew Rocklin`_
-  Replace isinstance(..., pandas) with is_dataframe_like (:pr:`4375`) `Matthew Rocklin`_
-  ENH: Support 3rd-party ExtensionArrays (:pr:`4379`) `Tom Augspurger`_
-  Pandas 0.24.0 compat (:pr:`4374`) `Tom Augspurger`_

Documentation
+++++++++++++

-  Fix link to 'map_blocks' function in array api docs (:pr:`4258`) `David Hoese`_
-  Add a paragraph on Dask-Yarn in the cloud docs (:pr:`4260`) `Jim Crist`_
-  Copy edit documentation (:pr:`4267`), (:pr:`4263`), (:pr:`4262`), (:pr:`4277`), (:pr:`4271`), (:pr:`4279`), (:pr:`4265`), (:pr:`4295`), (:pr:`4293`), (:pr:`4296`), (:pr:`4302`), (:pr:`4306`), (:pr:`4318`), (:pr:`4314`), (:pr:`4309`), (:pr:`4317`), (:pr:`4326`), (:pr:`4325`), (:pr:`4322`), (:pr:`4332`), (:pr:`4333`), `Miguel Farrajota`_
-  Fix typo in code example (:pr:`4272`) `Daniel Li`_
-  Doc: Update array-api.rst (:pr:`4259`) (:pr:`4282`) `Prabakaran Kumaresshan`_
-  Update hpc doc (:pr:`4266`) `Guillaume Eynard-Bontemps`_
-  Doc: Replace from_avro with read_avro in documents (:pr:`4313`) `Prabakaran Kumaresshan`_
-  Remove reference to "get" scheduler functions in docs (:pr:`4350`) `Matthew Rocklin`_
-  Fix typo in docstring (:pr:`4376`) `Daniel Saxton`_
-  Added documentation for dask.dataframe.merge (:pr:`4382`) `Jendrik Jördening`_

Core
++++

-  Avoid recursion in dask.core.get (:pr:`4219`) `Matthew Rocklin`_
-  Remove verbose flag from pytest setup.cfg (:pr:`4281`) `Matthew Rocklin`_
-  Support Pytest 4.0 by specifying marks explicitly (:pr:`4280`) `Takahiro Kojima`_
-  Add High Level Graphs (:pr:`4092`) `Matthew Rocklin`_
-  Fix SerializableLock locked and acquire methods (:pr:`4294`) `Stephan Hoyer`_
-  Pin boto3 to earlier version in tests to avoid moto conflict (:pr:`4276`) `Martin Durant`_
-  Treat None as missing in config when updating (:pr:`4324`) `Matthew Rocklin`_
-  Update Appveyor to Python 3.6 (:pr:`4337`) `Gábor Lipták`_
-  Use parse_bytes more liberally in dask.dataframe/bytes/bag (:pr:`4339`) `Matthew Rocklin`_
-  Add a better error message when cloudpickle is missing (:pr:`4342`) `Mark Harfouche`_
-  Support pool= keyword argument in threaded/multiprocessing get functions (:pr:`4351`) `Matthew Rocklin`_
-  Allow updates from arbitrary Mappings in config.update, not only dicts. (:pr:`4356`) `Stuart Berg`_
-  Move dask/array/top.py code to dask/blockwise.py (:pr:`4348`) `Matthew Rocklin`_
-  Add has_parallel_type (:pr:`4395`) `Matthew Rocklin`_
-  CI: Update Appveyor (:pr:`4381`) `Tom Augspurger`_
-  Ignore non-readable config files (:pr:`4388`) `Jim Crist`_


1.0.0 / 2018-11-28
------------------

Array
+++++

-  Add nancumsum/nancumprod unit tests (:pr:`4215`) `Guido Imperiale`_

DataFrame
+++++++++

-  Add index to to_dask_dataframe docstring (:pr:`4232`) `James Bourbeau`_
-  Text and fix when appending categoricals with fastparquet (:pr:`4245`) `Martin Durant`_
-  Don't reread metadata when passing ParquetFile to read_parquet (:pr:`4247`) `Martin Durant`_

Documentation
+++++++++++++

-  Copy edit documentation (:pr:`4222`) (:pr:`4224`) (:pr:`4228`) (:pr:`4231`) (:pr:`4230`) (:pr:`4234`) (:pr:`4235`) (:pr:`4254`) `Miguel Farrajota`_
-  Updated doc for the new scheduler keyword (:pr:`4251`) `@milesial`_


Core
++++

-  Avoid a few warnings (:pr:`4223`) `Matthew Rocklin`_
-  Remove dask.store module (:pr:`4221`) `Matthew Rocklin`_
-  Remove AUTHORS.md `Jim Crist`_


0.20.2 / 2018-11-15
-------------------

Array
+++++

-  Avoid fusing dependencies of atop reductions (:pr:`4207`) `Matthew Rocklin`_

Dataframe
+++++++++

-  Improve memory footprint for dataframe correlation (:pr:`4193`) `Damien Garaud`_
-  Add empty DataFrame check to boundary_slice (:pr:`4212`) `James Bourbeau`_


Documentation
+++++++++++++

-  Copy edit documentation (:pr:`4197`) (:pr:`4204`) (:pr:`4198`) (:pr:`4199`) (:pr:`4200`) (:pr:`4202`) (:pr:`4209`) `Miguel Farrajota`_
-  Add stats module namespace (:pr:`4206`) `James Bourbeau`_
-  Fix link in dataframe documentation (:pr:`4208`) `James Bourbeau`_


0.20.1 / 2018-11-09
-------------------

Array
+++++

-  Only allocate the result space in wrapped_pad_func (:pr:`4153`) `John A Kirkham`_
-  Generalize expand_pad_width to expand_pad_value (:pr:`4150`) `John A Kirkham`_
-  Test da.pad with 2D linear_ramp case (:pr:`4162`) `John A Kirkham`_
-  Fix import for broadcast_to. (:pr:`4168`) `samc0de`_
-  Rewrite Dask Array's `pad` to add only new chunks (:pr:`4152`) `John A Kirkham`_
-  Validate index inputs to atop (:pr:`4182`) `Matthew Rocklin`_

Core
++++

-  Dask.config set and get normalize underscores and hyphens (:pr:`4143`) `James Bourbeau`_
-  Only subs on core collections, not subclasses (:pr:`4159`) `Matthew Rocklin`_
-  Add block_size=0 option to HTTPFileSystem. (:pr:`4171`) `Martin Durant`_
-  Add traverse support for dataclasses (:pr:`4165`) `Armin Berres`_
-  Avoid optimization on sharedicts without dependencies (:pr:`4181`) `Matthew Rocklin`_
-  Update the pytest version for TravisCI (:pr:`4189`) `Damien Garaud`_
-  Use key_split rather than funcname in visualize names (:pr:`4160`) `Matthew Rocklin`_

Dataframe
+++++++++

-  Add fix for  DataFrame.__setitem__ for index (:pr:`4151`) `Anderson Banihirwe`_
-  Fix column choice when passing list of files to fastparquet (:pr:`4174`) `Martin Durant`_
-  Pass engine_kwargs from read_sql_table to sqlalchemy (:pr:`4187`) `Damien Garaud`_

Documentation
+++++++++++++

-  Fix documentation in Delayed best practices example that returned an empty list (:pr:`4147`) `Jonathan Fraine`_
-  Copy edit documentation (:pr:`4164`) (:pr:`4175`) (:pr:`4185`) (:pr:`4192`) (:pr:`4191`) (:pr:`4190`) (:pr:`4180`) `Miguel Farrajota`_
-  Fix typo in docstring (:pr:`4183`) `Carlos Valiente`_


0.20.0 / 2018-10-26
-------------------

Array
+++++

-  Fuse Atop operations (:pr:`3998`), (:pr:`4081`) `Matthew Rocklin`_
-  Support da.asanyarray on dask dataframes (:pr:`4080`) `Matthew Rocklin`_
-  Remove unnecessary endianness check in datetime test (:pr:`4113`) `Elliott Sales de Andrade`_
-  Set name=False in array foo_like functions (:pr:`4116`) `Matthew Rocklin`_
-  Remove dask.array.ghost module (:pr:`4121`) `Matthew Rocklin`_
-  Fix use of getargspec in dask array (:pr:`4125`) `Stephan Hoyer`_
-  Adds dask.array.invert (:pr:`4127`), (:pr:`4131`) `Anderson Banihirwe`_
-  Raise informative error on arg-reduction on unknown chunksize (:pr:`4128`), (:pr:`4135`) `Matthew Rocklin`_
-  Normalize reversed slices in dask array (:pr:`4126`) `Matthew Rocklin`_

Bag
+++

-  Add bag.to_avro (:pr:`4076`) `Martin Durant`_

Core
++++

-  Pull num_workers from config.get (:pr:`4086`), (:pr:`4093`) `James Bourbeau`_
-  Fix invalid escape sequences with raw strings (:pr:`4112`) `Elliott Sales de Andrade`_
-  Raise an error on the use of the get= keyword and set_options (:pr:`4077`) `Matthew Rocklin`_
-  Add import for Azure DataLake storage, and add docs (:pr:`4132`) `Martin Durant`_
-  Avoid collections.Mapping/Sequence (:pr:`4138`)  `Matthew Rocklin`_

Dataframe
+++++++++

-  Include index keyword in to_dask_dataframe (:pr:`4071`) `Matthew Rocklin`_
-  add support for duplicate column names (:pr:`4087`) `Jan Koch`_
-  Implement min_count for the DataFrame methods sum and prod (:pr:`4090`) `Bart Broere`_
-  Remove pandas warnings in concat (:pr:`4095`) `Matthew Rocklin`_
-  DataFrame.to_csv header option to only output headers in the first chunk (:pr:`3909`) `Rahul Vaidya`_
-  Remove Series.to_parquet (:pr:`4104`) `Justin Dennison`_
-  Avoid warnings and deprecated pandas methods (:pr:`4115`) `Matthew Rocklin`_
-  Swap 'old' and 'previous' when reporting append error (:pr:`4130`) `Martin Durant`_

Documentation
+++++++++++++

-  Copy edit documentation (:pr:`4073`), (:pr:`4074`), (:pr:`4094`), (:pr:`4097`), (:pr:`4107`), (:pr:`4124`), (:pr:`4133`), (:pr:`4139`) `Miguel Farrajota`_
-  Fix typo in code example (:pr:`4089`) `Antonino Ingargiola`_
-  Add pycon 2018 presentation (:pr:`4102`) `Javad`_
-  Quick description for gcsfs (:pr:`4109`) `Martin Durant`_
-  Fixed typo in docstrings of read_sql_table method (:pr:`4114`) `TakaakiFuruse`_
-  Make target directories in redirects if they don't exist (:pr:`4136`) `Matthew Rocklin`_



0.19.4 / 2018-10-09
-------------------

Array
+++++

-  Implement ``apply_gufunc(..., axes=..., keepdims=...)`` (:pr:`3985`) `Markus Gonser`_

Bag
+++

-  Fix typo in datasets.make_people (:pr:`4069`) `Matthew Rocklin`_

Dataframe
+++++++++

-  Added `percentiles` options for `dask.dataframe.describe` method (:pr:`4067`) `Zhenqing Li`_
-  Add DataFrame.partitions accessor similar to Array.blocks (:pr:`4066`) `Matthew Rocklin`_

Core
++++

-  Pass get functions and Clients through scheduler keyword (:pr:`4062`) `Matthew Rocklin`_

Documentation
+++++++++++++

-  Fix Typo on hpc example. (missing `=` in kwarg). (:pr:`4068`) `Matthias Bussonier`_
-  Extensive copy-editing: (:pr:`4065`), (:pr:`4064`), (:pr:`4063`) `Miguel Farrajota`_


0.19.3 / 2018-10-05
-------------------

Array
+++++

-   Make da.RandomState extensible to other modules (:pr:`4041`) `Matthew Rocklin`_
-   Support unknown dims in ravel no-op case (:pr:`4055`) `Jim Crist`_
-   Add basic infrastructure for cupy (:pr:`4019`) `Matthew Rocklin`_
-   Avoid asarray and lock arguments for from_array(getitem) (:pr:`4044`) `Matthew Rocklin`_
-   Move local imports in `corrcoef` to global imports (:pr:`4030`) `John A Kirkham`_
-   Move local `indices` import to global import (:pr:`4029`) `John A Kirkham`_
-   Fix-up Dask Array's fromfunction w.r.t. dtype and kwargs (:pr:`4028`) `John A Kirkham`_
-   Don't use dummy expansion for trim_internal in overlapped (:pr:`3964`) `Mark Harfouche`_
-   Add unravel_index (:pr:`3958`) `John A Kirkham`_

Bag
+++

-   Sort result in Bag.frequencies (:pr:`4033`) `Matthew Rocklin`_
-   Add support for npartitions=1 edge case in groupby (:pr:`4050`) `James Bourbeau`_
-   Add new random dataset for people (:pr:`4018`) `Matthew Rocklin`_
-   Improve performance of bag.read_text on small files (:pr:`4013`) `Eric Wolak`_
-   Add bag.read_avro (:pr:`4000`) (:pr:`4007`) `Martin Durant`_

Dataframe
+++++++++

-   Added an ``index`` parameter to :meth:`dask.dataframe.from_dask_array` for creating a dask DataFrame from a dask Array with a given index. (:pr:`3991`) `Tom Augspurger`_
-   Improve sub-classability of dask dataframe (:pr:`4015`) `Matthew Rocklin`_
-   Fix failing hdfs test [test-hdfs] (:pr:`4046`) `Jim Crist`_
-   fuse_subgraphs works without normal fuse (:pr:`4042`) `Jim Crist`_
-   Make path for reading many parquet files without prescan (:pr:`3978`) `Martin Durant`_
-   Index in dd.from_dask_array (:pr:`3991`) `Tom Augspurger`_
-   Making skiprows accept lists (:pr:`3975`) `Julia Signell`_
-   Fail early in fastparquet read for nonexistent column (:pr:`3989`) `Martin Durant`_

Core
++++

-   Add support for npartitions=1 edge case in groupby (:pr:`4050`) `James Bourbeau`_
-   Automatically wrap large arguments with dask.delayed in map_blocks/partitions (:pr:`4002`) `Matthew Rocklin`_
-   Fuse linear chains of subgraphs (:pr:`3979`) `Jim Crist`_
-   Make multiprocessing context configurable (:pr:`3763`) `Itamar Turner-Trauring`_

Documentation
+++++++++++++

-   Extensive copy-editing  (:pr:`4049`), (:pr:`4034`),  (:pr:`4031`), (:pr:`4020`), (:pr:`4021`), (:pr:`4022`), (:pr:`4023`), (:pr:`4016`), (:pr:`4017`), (:pr:`4010`), (:pr:`3997`), (:pr:`3996`), `Miguel Farrajota`_
-   Update shuffle method selection docs (:pr:`4048`) `James Bourbeau`_
-   Remove docs/source/examples, point to examples.dask.org (:pr:`4014`) `Matthew Rocklin`_
-   Replace readthedocs links with dask.org (:pr:`4008`) `Matthew Rocklin`_
-   Updates DataFrame.to_hdf docstring for returned values (:pr:`3992`) `James Bourbeau`_


0.19.2 / 2018-09-17
-------------------

Array
+++++

-  ``apply_gufunc`` implements automatic infer of functions output dtypes (:pr:`3936`) `Markus Gonser`_
-  Fix array histogram range error when array has nans (:pr:`3980`) `James Bourbeau`_
-  Issue 3937 follow up, int type checks. (:pr:`3956`) `Yu Feng`_
-  from_array: add @martindurant's explaining of how hashing is done for an array. (:pr:`3965`) `Mark Harfouche`_
-  Support gradient with coordinate (:pr:`3949`) `Keisuke Fujii`_

Core
++++

-  Fix use of has_keyword with partial in Python 2.7 (:pr:`3966`) `Mark Harfouche`_
-  Set pyarrow as default for HDFS (:pr:`3957`) `Matthew Rocklin`_

Documentation
+++++++++++++

-  Use dask_sphinx_theme (:pr:`3963`) `Matthew Rocklin`_
-  Use JupyterLab in Binder links from main page `Matthew Rocklin`_
-  DOC: fixed sphinx syntax (:pr:`3960`) `Tom Augspurger`_


0.19.1 / 2018-09-06
-------------------

Array
+++++

-  Don't enforce dtype if result has no dtype (:pr:`3928`) `Matthew Rocklin`_
-  Fix NumPy issubtype deprecation warning (:pr:`3939`) `Bruce Merry`_
-  Fix arg reduction tokens to be unique with different arguments (:pr:`3955`) `Tobias de Jong`_
-  Coerce numpy integers to ints in slicing code (:pr:`3944`) `Yu Feng`_
-  Linalg.norm ndim along axis partial fix (:pr:`3933`) `Tobias de Jong`_

Dataframe
+++++++++

-  Deterministic DataFrame.set_index (:pr:`3867`) `George Sakkis`_
-  Fix divisions in read_parquet when dealing with filters #3831 #3930 (:pr:`3923`) (:pr:`3931`)  `@andrethrill`_
-  Fixing returning type in categorical.as_known  (:pr:`3888`) `Sriharsha Hatwar`_
-  Fix DataFrame.assign for callables (:pr:`3919`) `Tom Augspurger`_
-  Include partitions with no width in repartition (:pr:`3941`) `Matthew Rocklin`_
-  Don't constrict stage/k dtype in dataframe shuffle (:pr:`3942`) `Matthew Rocklin`_

Documentation
+++++++++++++

-  DOC: Add hint on how to render task graphs horizontally (:pr:`3922`) `Uwe Korn`_
-  Add try-now button to main landing page (:pr:`3924`) `Matthew Rocklin`_


0.19.0 / 2018-08-29
-------------------

Array
+++++

-  Support coordinate in gradient (:pr:`3949`) `Keisuke Fujii`_
-  Fix argtopk split_every bug (:pr:`3810`) `Guido Imperiale`_
-  Ensure result computing dask.array.isnull() always gives a numpy array (:pr:`3825`) `Stephan Hoyer`_
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
-  Add new presentations (:pr:`3880`) `Javad`_
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

- Add to/from_zarr for Zarr-format datasets and arrays (:pr:`3460`) `Martin Durant`_
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
- Add ``dataframe.select_dtypes``, which mirrors the `pandas method <https://pandas.pydata.org/pandas-docs/version/0.18.1/generated/pandas.DataFrame.select_dtypes.html>`_ (:pr:`1556`)
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
https://distributed.dask.org . General feedback on that project is most
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
.. _`Ian Hopkinson`: https://github.com/IanHopkinson
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
.. _`Itamar Turner-Trauring`: https://github.com/itamarst
.. _`Mike Neish`: https://github.com/neishm
.. _`Mark Harfouche`: https://github.com/hmaarrfk
.. _`George Sakkis`: https://github.com/gsakkis
.. _`Ziyao Wei`: https://github.com/ZiyaoWei
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
.. _`Javad`: https://github.com/javad94
.. _`Daniel Rothenberg`: https://github.com/darothen
.. _`Hans Moritz Günther`: https://github.com/hamogu
.. _`@rtobar`: https://github.com/rtobar
.. _`Julia Signell`: https://github.com/jsignell
.. _`Sriharsha Hatwar`: https://github.com/Sriharsha-hatwar
.. _`Bruce Merry`: https://github.com/bmerry
.. _`Joe Hamman`: https://github.com/jhamman
.. _`Robert Sare`: https://github.com/rmsare
.. _`Jeremy Chan`: https://github.com/convexset
.. _`Eric Wolak`: https://github.com/epall
.. _`Miguel Farrajota`: https://github.com/farrajota
.. _`Zhenqing Li`: https://github.com/DigitalPig
.. _`Matthias Bussonier`: https://github.com/Carreau
.. _`Jan Koch`: https://github.com/datajanko
.. _`Bart Broere`: https://github.com/bartbroere
.. _`Rahul Vaidya`: https://github.com/rvaidya
.. _`Justin Dennison`: https://github.com/justin1dennison
.. _`Antonino Ingargiola`: https://github.com/tritemio
.. _`TakaakiFuruse`: https://github.com/TakaakiFuruse
.. _`samc0de`: https://github.com/samc0de
.. _`Armin Berres`: https://github.com/aberres
.. _`Damien Garaud`: https://github.com/geraud
.. _`Jonathan Fraine`: https://github.com/exowanderer
.. _`Carlos Valiente`: https://github.com/carletes
.. _`@milesial`: https://github.com/milesial
.. _`Paul Vecchio`: https://github.com/vecchp
.. _`Johnnie Gray`: https://github.com/jcmgray
.. _`Diane Trout`: https://github.com/detrout
.. _`Marco Neumann`: https://github.com/crepererum
.. _`Mina Farid`: https://github.com/minafarid
.. _`@slnguyen`: https://github.com/slnguyen
.. _`Gábor Lipták`: https://github.com/gliptak
.. _`David Hoese`: https://github.com/djhoese
.. _`Daniel Li`: https://github.com/li-dan
.. _`Prabakaran Kumaresshan`: https://github.com/nixphix
.. _`Daniel Saxton`: https://github.com/dsaxton
.. _`Jendrik Jördening`: https://github.com/jendrikjoe
.. _`Takahiro Kojima`: https://github.com/515hikaru
.. _`Stuart Berg`: https://github.com/stuarteberg
.. _`Guillaume Eynard-Bontemps`: https://github.com/guillaumeeb
.. _`Adam Beberg`: https://github.com/beberg
.. _`Johnnie Gray`: https://github.com/jcmgray
.. _`Roma Sokolov`: https://github.com/little-arhat
.. _`Daniel Severo`: https://github.com/daniel-severo
.. _`Michał Jastrzębski`: https://github.com/inc0
.. _`Janne Vuorela`: https://github.com/Dimplexion
.. _`Ross Petchler`: https://github.com/rpetchler
.. _`Aploium`: https://github.com/aploium
.. _`Peter Andreas Entschev`: https://github.com/pentschev
.. _`@JulianWgs`: https://github.com/JulianWgs
.. _`Shyam Saladi`: https://github.com/smsaladi
.. _`Joe Corbett`: https://github.com/jcorb
.. _`@HSR05`: https://github.com/HSR05
.. _`Ben Zaitlen`: https://github.com/quasiben
.. _`Brett Naul`: https://github.com/bnaul
.. _`Justin Poehnelt`: https://github.com/justinwp
.. _`Dan O'Donovan`: https://github.com/danodonovan
.. _`amerkel2`: https://github.com/amerkel2
.. _`Justin Waugh`: https://github.com/bluecoconut
.. _`Brian Chu`: https://github.com/bchu
.. _`Álvaro Abella Bascarán`: https://github.com/alvaroabascar
.. _`Aaron Fowles`: https://github.com/aaronfowles
.. _`Søren Fuglede Jørgensen`: https://github.com/fuglede
.. _`Hameer Abbasi`: https://github.com/hameerabbasi
.. _`Philipp Rudiger`: https://github.com/philippjfr
.. _`gregrf`: https://github.com/gregrf
.. _`Ian Rose`: https://github.com/ian-r-rose
.. _`Genevieve Buckley`: https://github.com/GenevieveBuckley
.. _`Michael Eaton`: https://github.com/mpeaton
.. _`Isaiah Norton`: https://github.com/hnorton
.. _`Nick Becker`: https://github.com/beckernick
.. _`Nathan Matare`: https://github.com/nmatare
.. _`@asmith26`: https://github.com/asmith26
.. _`Abhinav Ralhan`: https://github.com/abhinavralhan
.. _`Christian Hudon`: https://github.com/chrish42
.. _`Alistair Miles`: https://github.com/alimanfoo
.. _`Henry Pinkard`: https://github.com/
.. _`Ian Bolliger`: https://github.com/bolliger32
.. _`Mark Bell`: https://github.com/MarkCBell
.. _`Cody Johnson`: https://github.com/codercody
.. _`Endre Mark Borza`: https://github.com/endremborza
.. _`asmith26`: https://github.com/asmith26
.. _`Philipp S. Sommer`: https://github.com/Chilipp
.. _`mcsoini`: https://github.com/mcsoini
.. _`Ksenia Bobrova`: https://github.com/almaleksia
.. _`tpanza`: https://github.com/tpanza
.. _`Richard J Zamora`: https://github.com/rjzamora
.. _`Lijo Jose`: https://github.com/lijose
.. _`btw08`: https://github.com/btw08
.. _`Jorge Pessoa`: https://github.com/jorge-pessoa
.. _`Guillaume Lemaitre`: https://github.com/glemaitre
.. _`Bouwe Andela`: https://github.com/bouweandela
.. _`mbarkhau`: https://github.com/mbarkhau
.. _`Hugo`: https://github.com/hugovk
.. _`Paweł Kordek`: https://github.com/kordek
