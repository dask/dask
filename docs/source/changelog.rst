Changelog
=========

2.30.0 / 2020-10-06
-------------------

Array
+++++

- Allow ``rechunk`` to evenly split into N chunks (:pr:`6420`) `Scott Sievert`_


2.29.0 / 2020-10-02
-------------------

Array
+++++

- ``_repr_html_``: color sides darker instead of drawing all the lines (:pr:`6683`) `Julia Signell`_
- Removes warning from ``nanstd`` and ``nanvar`` (:pr:`6667`) `Thomas J Fan`_
- Get shape of output from original array - ``map_overlap`` (:pr:`6682`) `Julia Signell`_
- Replace ``np.searchsorted`` with ``bisect`` in indexing (:pr:`6669`) `Joachim B Haga`_

Bag
+++

- Make sure subprocesses have a consistent hash for bag ``groupby`` (:pr:`6660`) `Itamar Turner-Trauring`_

Core
++++

- Revert "Use ``HighLevelGraph`` layers everywhere in collections (:pr:`6510`)" (:pr:`6697`) `Tom Augspurger`_
- Use ``pandas.testing`` (:pr:`6687`) `John A Kirkham`_
- Improve 128-bit floating-point skip in tests (:pr:`6676`) `Elliott Sales de Andrade`_

DataFrame
+++++++++

- Allow setting dataframe items using a bool dataframe (:pr:`6608`) `Julia Signell`_

Documentation
+++++++++++++

- Fix typo (:pr:`6692`) `garanews`_
- Fix a few typos (:pr:`6678`) `Pav A`_


2.28.0 / 2020-09-25
-------------------

Array
+++++

- Partially reverted changes to ``Array`` indexing that produces large changes.
  This restores the behavior from Dask 2.25.0 and earlier, with a warning
  when large chunks are produced. A configuration option is provided
  to avoid creating the large chunks, see :ref:`array.slicing.efficiency`.
  (:pr:`6665`) `Tom Augspurger`_
- Add ``meta`` to ``to_dask_array`` (:pr:`6651`) `Kyle Nicholson`_
- Fix :pr:`6631` and :pr:`6611` (:pr:`6632`) `Rafal Wojdyla`_
- Infer object in array reductions (:pr:`6629`) `Daniel Saxton`_
- Adding ``v_based`` flag for ``svd_flip`` (:pr:`6658`) `Eric Czech`_
- Fix flakey array ``mean`` (:pr:`6656`) `Sam Grayson`_

Core
++++

- Removed ``dsk`` equality check from ``SubgraphCallable.__eq__`` (:pr:`6666`) `Mads R. B. Kristensen`_
- Use ``HighLevelGraph`` layers everywhere in collections (:pr:`6510`) `Mads R. B. Kristensen`_
- Adds hash dunder method to ``SubgraphCallable`` for caching purposes (:pr:`6424`) `Andrew Fulton`_
- Stop writing commented out config files by default (:pr:`6647`) `Matthew Rocklin`_

DataFrame
+++++++++

- Add support for collect list aggregation via ``agg`` API (:pr:`6655`) `Madhur Tandon`_
- Slightly better error message (:pr:`6657`) `Julia Signell`_


2.27.0 / 2020-09-18
-------------------

Array
+++++

- Preserve ``dtype`` in ``svd`` (:pr:`6643`) `Eric Czech`_

Core
++++

- ``store()``: create a single HLG layer (:pr:`6601`) `Mads R. B. Kristensen`_
- Add pre-commit CI build (:pr:`6645`) `James Bourbeau`_
- Update ``.pre-commit-config`` to latest black. (:pr:`6641`) `Julia Signell`_
- Update super usage to remove Python 2 compatibility (:pr:`6630`) `Poruri Sai Rahul`_
- Remove u string prefixes (:pr:`6633`) `Poruri Sai Rahul`_

DataFrame
+++++++++

- Improve error message for ``to_sql`` (:pr:`6638`) `Julia Signell`_
- Use empty list as categories (:pr:`6626`) `Julia Signell`_

Documentation
+++++++++++++

- Add ``autofunction`` to array api docs for more ufuncs (:pr:`6644`) `James Bourbeau`_
- Add a number of missing ufuncs to ``dask.array`` docs (:pr:`6642`) `Ralf Gommers`_
- Add ``HelmCluster`` docs (:pr:`6290`) `Jacob Tomlinson`_


2.26.0 / 2020-09-11
-------------------

Array
+++++

- Backend-aware dtype inference for single-chunk svd (:pr:`6623`) `Eric Czech`_
- Make ``array.reduction`` docstring match for dtype (:pr:`6624`) `Martin Durant`_
- Set lower bound on compression level for ``svd_compressed`` using rows and cols (:pr:`6622`) `Eric Czech`_
- Improve SVD consistency and small array handling (:pr:`6616`) `Eric Czech`_
- Add ``svd_flip`` #6599 (:pr:`6613`) `Eric Czech`_
- Handle sequences containing dask Arrays (:pr:`6595`) `Gabe Joseph`_
- Avoid large chunks from ``getitem`` with lists (:pr:`6514`) `Tom Augspurger`_
- Eagerly slice numpy arrays in ``from_array`` (:pr:`6605`) `Deepak Cherian`_
- Restore ability to pickle dask arrays (:pr:`6594`) `Noah D Brenowitz`_
- Add SVD support for short-and-fat arrays (:pr:`6591`) `Eric Czech`_
- Add simple chunk type registry and defer as appropriate to upcast types (:pr:`6393`) `Jon Thielen`_
- Align coarsen chunks by default (:pr:`6580`) `Deepak Cherian`_
- Fixup reshape on unknown dimensions and other testing fixes (:pr:`6578`) `Ryan Williams`_

Core
++++

- Add validation and fixes for ``HighLevelGraph`` dependencies (:pr:`6588`) `Mads R. B. Kristensen`_
- Fix linting issue (:pr:`6598`) `Tom Augspurger`_
- Skip ``bokeh`` version 2.0.0 (:pr:`6572`) `John A Kirkham`_

DataFrame
+++++++++

- Added bytes/row calculation when using meta (:pr:`6585`) `McToel`_
- Handle ``min_count`` in ``Series.sum`` / ``prod`` (:pr:`6618`) `Daniel Saxton`_
- Update ``DataFrame.set_index`` docstring (:pr:`6549`) `Timost`_
- Always compute 0 and 1 quantiles during quantile calculations (:pr:`6564`) `Erik Welch`_
- Fix wrong path when reading empty csv file (:pr:`6573`) `Abdulelah Bin Mahfoodh`_

Documentation
+++++++++++++

- Doc: Troubleshooting dashboard 404 (:pr:`6215`) `Kilian Lieret`_
- Fixup ``extraConfig`` example (:pr:`6625`) `Tom Augspurger`_
- Update supported Python versions (:pr:`6609`) `Julia Signell`_
- Document dask/daskhub helm chart (:pr:`6560`) `Tom Augspurger`_


2.25.0 / 2020-08-28
-------------------

Core
++++

- Compare key hashes in ``subs()`` (:pr:`6559`) `Mads R. B. Kristensen`_
- Rerun with latest ``black`` release (:pr:`6568`) `James Bourbeau`_
- License update (:pr:`6554`) `Tom Augspurger`_

DataFrame
+++++++++

- Add gs ``read_parquet`` example (:pr:`6548`) `Ray Bell`_

Documentation
+++++++++++++

- Remove version from documentation page names (:pr:`6558`) `James Bourbeau`_
- Update ``kubernetes-helm.rst`` (:pr:`6523`) `David Sheldon`_
- Stop 2020 survey (:pr:`6547`) `Tom Augspurger`_


2.24.0 / 2020-08-22
-------------------

Array
+++++

-   Fix setting random seed in tests. (:pr:`6518`) `Elliott Sales de Andrade`_
-   Support meta in apply gufunc (:pr:`6521`) `joshreback`_
-   Replace `cupy.sparse` with `cupyx.scipy.sparse` (:pr:`6530`) `John A Kirkham`_

Dataframe
+++++++++

-   Bump up tolerance for rolling tests (:pr:`6502`) `Julia Signell`_
-   Implement DatFrame.__len__ (:pr:`6515`) `Tom Augspurger`_
-   Infer arrow schema in to_parquet  (for ArrowEngine`) (:pr:`6490`) `Richard Zamora`_
-   Fix parquet test when no pyarrow (:pr:`6524`) `Martin Durant`_
-   Remove problematic ``filter`` arguments in ArrowEngine (:pr:`6527`) `Richard Zamora`_
-   Avoid schema validation by default in ArrowEngine (:pr:`6536`) `Richard Zamora`_

Core
++++

-   Use unpack_collections in make_blockwise_graph (:pr:`6517`) `Thomas Fan`_
-   Move key_split() from optimization.py to utils.py (:pr:`6529`) `Mads R. B. Kristensen`_
-   Make tests run on moto server (:pr:`6528`) `Martin Durant`_


2.23.0 / 2020-08-14
-------------------

Array
+++++

- Reduce ``np.zeros``, ``ones``, and ``full`` array size with broadcasting (:pr:`6491`) `Matthias Bussonnier`_
- Add missing ``meta=`` for ``trim`` in ``map_overlap`` (:pr:`6494`) `Peter Andreas Entschev`_

Bag
+++

- Bag repartition partition size (:pr:`6371`) `joshreback`_

Core
++++

- ``Scalar.__dask_layers__()`` to return ``self._name`` instead of ``self.key`` (:pr:`6507`) `Mads R. B. Kristensen`_
- Update dependencies correctly in ``fuse_root`` optimization (:pr:`6508`) `Mads R. B. Kristensen`_


DataFrame
+++++++++

- Adds ``items`` to dataframe (:pr:`6503`) `Thomas J Fan`_
- Include compression in ``write_table`` call (:pr:`6499`) `Julia Signell`_
- Fixed warning in ``nonempty_series`` (:pr:`6485`) `Tom Augspurger`_
- Intelligently determine partitions based on type of first arg (:pr:`6479`) `Matthew Rocklin`_
- Fix pyarrow ``mkdirs`` (:pr:`6475`) `Julia Signell`_
- Fix duplicate parquet output in ``to_parquet`` (:pr:`6451`) `michaelnarodovitch`_

Documentation
+++++++++++++

- Fix documentation ``da.histogram`` (:pr:`6439`) `Roberto Panai`_
- Add ``agg`` ``nunique`` example (:pr:`6404`) `Ray Bell`_
- Fixed a few typos in the SQL docs (:pr:`6489`) `Mike McCarty`_
- Docs for SQLing (:pr:`6453`) `Martin Durant`_


2.22.0 / 2020-07-31
-------------------

Array
+++++

- Compatibility for NumPy dtype deprecation (:pr:`6430`) `Tom Augspurger`_

Core
++++

- Implement ``sizeof`` for some ``bytes``-like objects (:pr:`6457`) `John A Kirkham`_
- HTTP error for new ``fsspec`` (:pr:`6446`) `Martin Durant`_
- When ``RecursionError`` is raised, return uuid from ``tokenize`` function (:pr:`6437`) `Julia Signell`_
- Install deps of upstream-dev packages (:pr:`6431`) `Tom Augspurger`_
- Use updated link in ``setup.cfg`` (:pr:`6426`) `Zhengnan`_

DataFrame
+++++++++

- Add single quotes around column names if strings (:pr:`6471`) `Gil Forsyth`_
- Refactor ``ArrowEngine`` for better ``read_parquet`` performance (:pr:`6346`) `Richard (Rick) Zamora`_
- Add ``tolist`` dispatch (:pr:`6444`) `GALI PREM SAGAR`_
- Compatibility with pandas 1.1.0rc0 (:pr:`6429`) `Tom Augspurger`_
- Multi value pivot table (:pr:`6428`) `joshreback`_
- Duplicate argument definitions in ``to_csv`` docstring (:pr:`6411`) `Jun Han (Johnson) Ooi`_

Documentation
+++++++++++++

- Add utility to docs to convert YAML config to env vars and back (:pr:`6472`) `Jacob Tomlinson`_
- Fix parameter server rendering (:pr:`6466`) `Scott Sievert`_
- Fixes broken links (:pr:`6403`) `Jim Circadian`_
- Complete parameter server implementation in docs (:pr:`6449`) `Scott Sievert`_
- Fix typo (:pr:`6436`) `Jack Xiaosong Xu`_


2.21.0 / 2020-07-17
-------------------

Array
+++++

- Correct error message in ``array.routines.gradient()`` (:pr:`6417`) `johnomotani`_
- Fix blockwise concatenate for array with some ``dimension=1`` (:pr:`6342`) `Matthias Bussonnier`_

Bag
+++

- Fix ``bag.take`` example (:pr:`6418`) `Roberto Panai`_

Core
++++

- Groups values in optimization pass should only be graph and keys -- not an optimization + keys (:pr:`6409`) `Ben Zaitlen`_
- Call custom optimizations once, with ``kwargs`` provided (:pr:`6382`) `Clark Zinzow`_
- Include ``pickle5`` for testing on Python 3.7 (:pr:`6379`) `John A Kirkham`_

DataFrame
+++++++++

- Correct typo in error message (:pr:`6422`) `Tom McTiernan`_
- Use ``pytest.warns`` to check for ``UserWarning`` (:pr:`6378`) `Richard (Rick) Zamora`_
- Parse ``bytes_per_chunk keyword`` from string (:pr:`6370`) `Matthew Rocklin`_

Documentation
+++++++++++++

- Numpydoc formatting (:pr:`6421`) `Matthias Bussonnier`_
- Unpin ``numpydoc`` following 1.1 release (:pr:`6407`) `Gil Forsyth`_
- Numpydoc formatting (:pr:`6402`) `Matthias Bussonnier`_
- Add instructions for using conda when installing code for development (:pr:`6399`) `Ray Bell`_
- Update ``visualize`` docstrings (:pr:`6383`) `Zhengnan`_


2.20.0 / 2020-07-02
-------------------

Array
+++++

- Register ``sizeof`` for numpy zero-strided arrays (:pr:`6343`) `Matthias Bussonnier`_
- Use ``concatenate_lookup`` in ``concatenate`` (:pr:`6339`) `John A Kirkham`_
- Fix rechunking of arrays with some zero-length dimensions (:pr:`6335`) `Matthias Bussonnier`_

DataFrame
+++++++++

- Dispatch ``iloc``` calls to ``getitem`` (:pr:`6355`) `Gil Forsyth`_
- Handle unnamed pandas ``RangeIndex`` in fastparquet engine (:pr:`6350`) `Richard (Rick) Zamora`_
- Preserve index when writing partitioned parquet datasets with pyarrow (:pr:`6282`) `Richard (Rick) Zamora`_
- Use ``ignore_index`` for pandas' ``group_split_dispatch`` (:pr:`6251`) `Richard (Rick) Zamora`_

Documentation
+++++++++++++

- Add doc describing argument (:pr:`6318`) `asmith26`_


2.19.0 / 2020-06-19
-------------------

Array
+++++

- Cast chunk sizes to python int ``dtype`` (:pr:`6326`) `Gil Forsyth`_
- Add ``shape=None`` to ``*_like()`` array creation functions (:pr:`6064`) `Anderson Banihirwe`_

Core
++++

- Update expected error msg for protocol difference in fsspec (:pr:`6331`) `Gil Forsyth`_
- Fix for floats < 1 in ``parse_bytes`` (:pr:`6311`) `Gil Forsyth`_
- Fix exception causes all over the codebase (:pr:`6308`) `Ram Rachum`_
- Fix duplicated tests (:pr:`6303`) `James Lamb`_
- Remove unused testing function (:pr:`6304`) `James Lamb`_

DataFrame
+++++++++

- Add high-level CSV Subgraph (:pr:`6262`) `Gil Forsyth`_
- Fix ``ValueError`` when merging an index-only 1-partition dataframe (:pr:`6309`) `Krishan Bhasin`_
- Make ``index.map`` clear divisions. (:pr:`6285`) `Julia Signell`_

Documentation
+++++++++++++

- Add link to 2020 survey (:pr:`6328`) `Tom Augspurger`_
- Update ``bag.rst`` (:pr:`6317`) `Ben Shaver`_


2.18.1 / 2020-06-09
-------------------

Array
+++++

- Don't try to set name on ``full`` (:pr:`6299`) `Julia Signell`_
- Histogram: support lazy values for range/bins (another way) (:pr:`6252`) `Gabe Joseph`_

Core
++++

- Fix exception causes in ``utils.py`` (:pr:`6302`) `Ram Rachum`_
- Improve performance of ``HighLevelGraph`` construction (:pr:`6293`) `Julia Signell`_

Documentation
+++++++++++++

- Now readthedocs builds unrelased features' docstrings (:pr:`6295`) `Antonio Ercole De Luca`_
- Add ``asyncssh`` intersphinx mappings (:pr:`6298`) `Jacob Tomlinson`_


2.18.0 / 2020-06-05
-------------------

Array
+++++

- Cast slicing index to dask array if same shape as original (:pr:`6273`) `Julia Signell`_
- Fix ``stack`` error message (:pr:`6268`) `Stephanie Gott`_
- ``full`` & ``full_like``: error on non-scalar ``fill_value`` (:pr:`6129`) `Huite`_
- Support for multiple arrays in ``map_overlap`` (:pr:`6165`) `Eric Czech`_
- Pad resample divisions so that edges are counted (:pr:`6255`) `Julia Signell`_

Bag
+++

- Random sampling of k elements from a dask bag #4799 (:pr:`6239`) `Antonio Ercole De Luca`_

DataFrame
+++++++++

- Add ``dropna``, ``sort``, and ``ascending`` to ``sort_values`` (:pr:`5880`) `Julia Signell`_
- Generalize ``from_dask_array`` (:pr:`6263`) `GALI PREM SAGAR`_
- Add derived docstring for ``SeriesGroupby.nunique`` (:pr:`6284`) `Julia Signell`_
- Remove ``NotImplementedError`` in resample with rule  (:pr:`6274`) `Abdulelah Bin Mahfoodh`_
- Add ``dd.to_sql`` (:pr:`6038`) `Ryan Williams`_

Documentation
+++++++++++++

- Update remote data section (:pr:`6258`) `Ray Bell`_


2.17.2 / 2020-05-28
-------------------

Core
++++

- Re-add the ``complete`` extra (:pr:`6257`) `Jim Crist-Harif`_

DataFrame
+++++++++

- Raise error if ``resample`` isn't going to give right answer (:pr:`6244`) `Julia Signell`_


2.17.1 / 2020-05-28
-------------------

Array
+++++

- Empty array rechunk (:pr:`6233`) `Andrew Fulton`_

Core
++++

- Make ``pyyaml`` required (:pr:`6250`) `Jim Crist-Harif`_
- Fix install commands from ``ImportError`` (:pr:`6238`) `Gaurav Sheni`_
- Remove issue template (:pr:`6249`) `Jacob Tomlinson`_

DataFrame
+++++++++

- Pass ``ignore_index`` to ``dd_shuffle`` from ``DataFrame.shuffle`` (:pr:`6247`) `Richard (Rick) Zamora`_
- Cope with missing HDF keys (:pr:`6204`) `Martin Durant`_
- Generalize ``describe`` & ``quantile`` apis (:pr:`5137`) `GALI PREM SAGAR`_


2.17.0 / 2020-05-26
-------------------

Array
+++++

- Small improvements to ``da.pad`` (:pr:`6213`) `Mark Boer`_
- Return ``tuple`` if multiple outputs in ``dask.array.apply_gufunc``, add test to check for tuple (:pr:`6207`) `Kai Mühlbauer`_
- Support ``stack`` with unknown chunksizes (:pr:`6195`) `swapna`_

Bag
+++

- Random Choice on Bags (:pr:`6208`) `Antonio Ercole De Luca`_

Core
++++

- Raise warning ``delayed.visualise()`` (:pr:`6216`) `Amol Umbarkar`_
- Ensure other pickle arguments work (:pr:`6229`) `John A Kirkham`_
- Overhaul ``fuse()`` config (:pr:`6198`) `Guido Imperiale`_
- Update ``dask.order.order`` to consider "next" nodes using both FIFO and LIFO (:pr:`5872`) `Erik Welch`_

DataFrame
+++++++++

- Use 0 as ``fill_value`` for more agg methods (:pr:`6245`) `Julia Signell`_
- Generalize ``rearrange_by_column_tasks`` and add ``DataFrame.shuffle`` (:pr:`6066`) `Richard (Rick) Zamora`_
- Xfail ``test_rolling_numba_engine`` for newer numba and older pandas (:pr:`6236`) `James Bourbeau`_
- Generalize ``fix_overlap`` (:pr:`6240`) `GALI PREM SAGAR`_
- Fix ``DataFrame.shape`` with no columns (:pr:`6237`) `noreentry`_
- Avoid shuffle when setting a presorted index with overlapping divisions (:pr:`6226`) `Krishan Bhasin`_
- Adjust the Parquet engine classes to allow more easily subclassing (:pr:`6211`) `Marius van Niekerk`_
- Fix ``dd.merge_asof`` with ``left_on='col'`` & ``right_index=True`` (:pr:`6192`) `noreentry`_
- Disable warning for ``concat`` (:pr:`6210`) `Tung Dang`_
- Move ``AUTO_BLOCKSIZE`` out of ``read_csv`` signature (:pr:`6214`) `Jim Crist-Harif`_
- ``.loc`` indexing with callable (:pr:`6185`) `Endre Mark Borza`_
- Avoid apply in ``_compute_sum_of_squares`` for groupby std agg (:pr:`6186`) `Richard (Rick) Zamora`_
- Minor correction to ``test_parquet`` (:pr:`6190`) `Brian Larsen`_
- Adhering to the passed pat for delimeter join and fix error message (:pr:`6194`) `GALI PREM SAGAR`_
- Skip ``test_to_parquet_with_get`` if no parquet libs available (:pr:`6188`) `Scott Sanderson`_

Documentation
+++++++++++++

- Added documentation for ``distributed.Event`` class (:pr:`6231`) `Nils Braun`_
- Doc write to remote (:pr:`6124`) `Ray Bell`_


2.16.0 / 2020-05-08
-------------------

Array
+++++

- Fix array general-reduction name (:pr:`6176`) `Nick Evans`_
- Replace ``dim`` with ``shape`` in ``unravel_index`` (:pr:`6155`) `Julia Signell`_
- Moment: handle all elements being masked (:pr:`5339`) `Gabe Joseph`_

Core
++++

- Remove Redundant string concatenations in dask code-base (:pr:`6137`) `GALI PREM SAGAR`_
- Upstream compat (:pr:`6159`) `Tom Augspurger`_
- Ensure ``sizeof`` of dict and sequences returns an integer (:pr:`6179`) `James Bourbeau`_
- Estimate python collection sizes with random sampling (:pr:`6154`) `Florian Jetter`_
- Update test upstream (:pr:`6146`) `Tom Augspurger`_
- Skip test for mindeps build (:pr:`6144`) `Tom Augspurger`_
- Switch default multiprocessing context to "spawn" (:pr:`4003`) `Itamar Turner-Trauring`_
- Update manifest to include dask-schema (:pr:`6140`) `Ben Zaitlen`_

DataFrame
+++++++++

- Harden inconsistent-schema handling in pyarrow-based ``read_parquet`` (:pr:`6160`) `Richard (Rick) Zamora`_
- Add compute ``kwargs`` to methods that write data to disk (:pr:`6056`) `Krishan Bhasin`_
- Fix issue where ``unique`` returns an index like result from backends (:pr:`6153`) `GALI PREM SAGAR`_
- Fix internal error in ``map_partitions`` with collections (:pr:`6103`) `Tom Augspurger`_

Documentation
+++++++++++++

- Add phase of computation to index TOC (:pr:`6157`) `Ben Zaitlen`_
- Remove unused imports in scheduling script (:pr:`6138`) `James Lamb`_
- Fix indent (:pr:`6147`) `Martin Durant`_
- Add Tom's log config example (:pr:`6143`) `Martin Durant`_


2.15.0 / 2020-04-24
-------------------

Array
+++++

- Update ``dask.array.from_array`` to warn when passed a Dask collection (:pr:`6122`) `James Bourbeau`_
- Un-numpy like behaviour in ``dask.array.pad`` (:pr:`6042`) `Mark Boer`_
- Add support for ``repeats=0`` in ``da.repeat`` (:pr:`6080`) `James Bourbeau`_

Core
++++

- Fix yaml layout for schema (:pr:`6132`) `Ben Zaitlen`_
- Configuration Reference (:pr:`6069`) `Ben Zaitlen`_
- Add configuration option to turn off task fusion (:pr:`6087`) `Matthew Rocklin`_
- Skip pyarrow on windows (:pr:`6094`) `Tom Augspurger`_
- Set limit to maximum length of fused key (:pr:`6057`) `Lucas Rademaker`_
- Add test against #6062 (:pr:`6072`) `Martin Durant`_
- Bump checkout action to v2 (:pr:`6065`) `James Bourbeau`_

DataFrame
+++++++++

- Generalize categorical calls to support cudf ``Categorical`` (:pr:`6113`) `GALI PREM SAGAR`_
- Avoid reading ``_metadata`` on every worker (:pr:`6017`) `Richard (Rick) Zamora`_
- Use ``group_split_dispatch`` and ``ignore_index`` in ``apply_concat_apply`` (:pr:`6119`) `Richard (Rick) Zamora`_
- Handle new (dtype) pandas metadata with pyarrow (:pr:`6090`) `Richard (Rick) Zamora`_
- Skip ``test_partition_on_cats_pyarrow`` if pyarrow is not installed (:pr:`6112`) `James Bourbeau`_
- Update DataFrame len to handle columns with the same name (:pr:`6111`) `James Bourbeau`_
- ``ArrowEngine`` bug fixes and test coverage (:pr:`6047`) `Richard (Rick) Zamora`_
- Added mode (:pr:`5958`) `Adam Lewis`_

Documentation
+++++++++++++

- Update "helm install" for helm 3 usage (:pr:`6130`) `JulianWgs`_
- Extend preload documentation (:pr:`6077`) `Matthew Rocklin`_
- Fixed small typo in DataFrame ``map_partitions()`` docstring (:pr:`6115`) `Eugene Huang`_
- Fix typo: "double" should be times, not plus (:pr:`6091`) `David Chudzicki`_
- Fix first line of ``array.random.*`` docs (:pr:`6063`) `Martin Durant`_
- Add section about ``Semaphore`` in distributed (:pr:`6053`) `Florian Jetter`_


2.14.0 / 2020-04-03
-------------------

Array
+++++

- Added ``np.iscomplexobj`` implementation (:pr:`6045`) `Tom Augspurger`_

Core
++++

- Update ``test_rearrange_disk_cleanup_with_exception`` to pass without cloudpickle installed (:pr:`6052`) `James Bourbeau`_
- Fixed flaky ``test-rearrange`` (:pr:`5977`) `Tom Augspurger`_

DataFrame
+++++++++

- Use ``_meta_nonempty`` for dtype casting in ``stack_partitions`` (:pr:`6061`) `mlondschien`_
- Fix bugs in ``_metadata`` creation and filtering in parquet ``ArrowEngine`` (:pr:`6023`) `Richard (Rick) Zamora`_

Documentation
+++++++++++++

- DOC: Add name caveats (:pr:`6040`) `Tom Augspurger`_


2.13.0 / 2020-03-25
-------------------

Array
+++++

- Support ``dtype`` and other keyword arguments in ``da.random`` (:pr:`6030`) `Matthew Rocklin`_
- Register support for ``cupy`` sparse ``hstack``/``vstack`` (:pr:`5735`) `Corey J. Nolet`_
- Force ``self.name`` to ``str`` in ``dask.array`` (:pr:`6002`) `Chuanzhu Xu`_

Bag
+++

- Set ``rename_fused_keys`` to ``None`` by default in ``bag.optimize`` (:pr:`6000`) `Lucas Rademaker`_

Core
++++

- Copy dict in ``to_graphviz`` to prevent overwriting (:pr:`5996`) `JulianWgs`_
- Stricter pandas ``xfail`` (:pr:`6024`) `Tom Augspurger`_
- Fix CI failures (:pr:`6013`) `James Bourbeau`_
- Update ``toolz`` to 0.8.2 and use ``tlz`` (:pr:`5997`) `Ryan Grout`_
- Move Windows CI builds to GitHub Actions (:pr:`5862`) `James Bourbeau`_

DataFrame
+++++++++

- Improve path-related exceptions in ``read_hdf`` (:pr:`6032`) `psimaj`_
- Fix ``dtype`` handling in ``dd.concat`` (:pr:`6006`) `mlondschien`_
- Handle cudf's leftsemi and leftanti joins (:pr:`6025`) `Richard J Zamora`_
- Remove unused ``npartitions`` variable in ``dd.from_pandas`` (:pr:`6019`) `Daniel Saxton`_
- Added shuffle to ``DataFrame.random_split`` (:pr:`5980`) `petiop`_

Documentation
+++++++++++++

- Fix indentation in scheduler-overview docs (:pr:`6022`) `Matthew Rocklin`_
- Update task graphs in optimize docs (:pr:`5928`) `Julia Signell`_
- Optionally get rid of intermediary boxes in visualize, and add more labels (:pr:`5976`) `Julia Signell`_


2.12.0 / 2020-03-06
-------------------

Array
+++++

- Improve reuse of temporaries with numpy (:pr:`5933`) `Bruce Merry`_
- Make ``map_blocks`` with ``block_info`` produce a ``Blockwise`` (:pr:`5896`) `Bruce Merry`_
- Optimize ``make_blockwise_graph`` (:pr:`5940`) `Bruce Merry`_
- Fix axes ordering in ``da.tensordot`` (:pr:`5975`) `Gil Forsyth`_
- Adds empty mode to ``array.pad`` (:pr:`5931`) `Thomas J Fan`_

Core
++++

- Remove ``toolz.memoize`` dependency in ``dask.utils`` (:pr:`5978`) `Ryan Grout`_
- Close pool leaking subprocess (:pr:`5979`) `Tom Augspurger`_
- Pin ``numpydoc`` to ``0.8.0`` (fix double autoescape) (:pr:`5961`) `Gil Forsyth`_
- Register deterministic tokenization for ``range`` objects (:pr:`5947`) `James Bourbeau`_
- Unpin ``msgpack`` in CI (:pr:`5930`) `JAmes Bourbeau`_
- Ensure dot results are placed in unique files. (:pr:`5937`) `Elliott Sales de Andrade`_
- Add remaining optional dependencies to Travis 3.8 CI build environment (:pr:`5920`) `James Bourbeau`_

DataFrame
+++++++++

- Skip parquet ``getitem`` optimization for some keys (:pr:`5917`) `Tom Augspurger`_
- Add ``ignore_index`` argument to ``rearrange_by_column`` code path (:pr:`5973`) `Richard J Zamora`_
- Add DataFrame and Series ``memory_usage_per_partition`` methods (:pr:`5971`) `James Bourbeau`_
- ``xfail`` test_describe when using Pandas 0.24.2 (:pr:`5948`) `James Bourbeau`_
- Implement ``dask.dataframe.to_numeric`` (:pr:`5929`) `Julia Signell`_
- Add new error message content when columns are in a different order (:pr:`5927`) `Julia Signell`_
- Use shallow copy for assign operations when possible (:pr:`5740`) `Richard J Zamora`_

Documentation
+++++++++++++

- Changed above to below in ``dask.array.triu`` docs (:pr:`5984`) `Henrik Andersson`_
- Array slicing: fix typo in ``slice_with_int_dask_array`` error message (:pr:`5981`) `Gabe Joseph`_
- Grammar and formatting updates to docstrings (:pr:`5963`) `James Lamb`_
- Update develop doc with conda option (:pr:`5939`) `Ray Bell`_
- Update title of DataFrame extension docs (:pr:`5954`) `James Bourbeau`_
- Fixed typos in documentation (:pr:`5962`) `James Lamb`_
- Add original class or module as a ``kwarg`` on ``_bind_*`` methods (:pr:`5946`) `Julia Signell`_
- Add collect list example (:pr:`5938`) `Ray Bell`_
- Update optimization doc for python 3 (:pr:`5926`) `Julia Signell`_


2.11.0 / 2020-02-19
-------------------

Array
+++++

- Cache result of ``Array.shape`` (:pr:`5916`) `Bruce Merry`_
- Improve accuracy of ``estimate_graph_size`` for ``rechunk`` (:pr:`5907`) `Bruce Merry`_
- Skip rechunk steps that do not alter chunking (:pr:`5909`) `Bruce Merry`_
- Support ``dtype`` and other ``kwargs`` in ``coarsen`` (:pr:`5903`) `Matthew Rocklin`_
- Push chunk override from ``map_blocks`` into blockwise (:pr:`5895`) `Bruce Merry`_
- Avoid using ``rewrite_blockwise`` for a singleton (:pr:`5890`) `Bruce Merry`_
- Optimize ``slices_from_chunks`` (:pr:`5891`) `Bruce Merry`_
- Avoid unnecessary ``__getitem__`` in ``block()`` when chunks have correct dimensionality (:pr:`5884`) `Thomas Robitaille`_

Bag
+++

- Add ``include_path`` option for ``dask.bag.read_text`` (:pr:`5836`) `Yifan Gu`_
- Fixes ``ValueError`` in delayed execution of bagged NumPy array (:pr:`5828`) `Surya Avala`_

Core
++++

- CI: Pin ``msgpack`` (:pr:`5923`) `Tom Augspurger`_
- Rename ``test_inner`` to ``test_outer`` (:pr:`5922`) `Shiva Raisinghani`_
- ``quote`` should quote dicts too (:pr:`5905`) `Bruce Merry`_
- Register a normalizer for literal (:pr:`5898`) `Bruce Merry`_
- Improve layer name synthesis for non-HLGs (:pr:`5888`) `Bruce Merry`_
- Replace flake8 pre-commit-hook with upstream (:pr:`5892`) `Julia Signell`_
- Call pip as a module to avoid warnings (:pr:`5861`) `Cyril Shcherbin`_
- Close ``ThreadPool`` at exit (:pr:`5852`) `Tom Augspurger`_
- Remove ``dask.dataframe`` import in tokenization code (:pr:`5855`) `James Bourbeau`_

DataFrame
+++++++++

- Require ``pandas>=0.23`` (:pr:`5883`) `Tom Augspurger`_
- Remove lambda from dataframe aggregation (:pr:`5901`) `Matthew Rocklin`_
- Fix exception chaining in ``dataframe/__init__.py`` (:pr:`5882`) `Ram Rachum`_
- Add support for reductions on empty dataframes (:pr:`5804`) `Shiva Raisinghani`_
- Expose ``sort=`` argument for groupby (:pr:`5801`) `Richard J Zamora`_
- Add ``df.empty`` property (:pr:`5711`) `rockwellw`_
- Use parquet read speed-ups from ``fastparquet.api.paths_to_cats``. (:pr:`5821`) `Igor Gotlibovych`_

Documentation
+++++++++++++

- Deprecate ``doc_wraps`` (:pr:`5912`) `Tom Augspurger`_
- Update array internal design docs for HighLevelGraph era (:pr:`5889`) `Bruce Merry`_
- Move over dashboard connection docs (:pr:`5877`) `Matthew Rocklin`_
- Move prometheus docs from distributed.dask.org (:pr:`5876`) `Matthew Rocklin`_
- Removing duplicated DO block at the end (:pr:`5878`) `K.-Michael Aye`_
- ``map_blocks`` see also (:pr:`5874`) `Tom Augspurger`_
- More derived from (:pr:`5871`) `Julia Signell`_
- Fix typo (:pr:`5866`) `Yetunde Dada`_
- Fix typo in ``cloud.rst`` (:pr:`5860`) `Andrew Thomas`_
- Add note pointing to code of conduct and diversity statement (:pr:`5844`) `Matthew Rocklin`_


2.10.1 / 2020-01-30
-------------------

- Fix Pandas 1.0 version comparison (:pr:`5851`) `Tom Augspurger`_
- Fix typo in distributed diagnostics documentation (:pr:`5841`) `Gerrit Holl`_


2.10.0 / 2020-01-28
-------------------

- Support for pandas 1.0's new ``BooleanDtype`` and ``StringDtype`` (:pr:`5815`) `Tom Augspurger`_
- Compatibility with pandas 1.0's API breaking changes and deprecations (:pr:`5792`) `Tom Augspurger`_
- Fixed non-deterministic tokenization of some extension-array backed pandas objects (:pr:`5813`) `Tom Augspurger`_
- Fixed handling of dataclass class objects in collections (:pr:`5812`) `Matteo De Wint`_
- Fixed resampling with tz-aware dates when one of the endpoints fell in a non-existent time (:pr:`5807`) `dfonnegra`_
- Delay initial Zarr dataset creation until the computation occurs (:pr:`5797`) `Chris Roat`_
- Use parquet dataset statistics in more cases with the ``pyarrow`` engine (:pr:`5799`) `Richard J Zamora`_
- Fixed exception in ``groupby.std()`` when some of the keys were large integers (:pr:`5737`) `H. Thomson Comer`_


2.9.2 / 2020-01-16
------------------

Array
+++++

- Unify chunks in ``broadcast_arrays`` (:pr:`5765`) `Matthew Rocklin`_

Core
++++

- ``xfail`` CSV encoding tests (:pr:`5791`) `Tom Augspurger`_
- Update order to handle empty dask graph (:pr:`5789`) `James Bourbeau`_
- Redo ``dask.order.order`` (:pr:`5646`) `Erik Welch`_

DataFrame
+++++++++

- Add transparent compression for on-disk shuffle with ``partd`` (:pr:`5786`) `Christian Wesp`_
- Fix ``repr`` for empty dataframes (:pr:`5781`) `Shiva Raisinghani`_
- Pandas 1.0.0RC0 compat (:pr:`5784`) `Tom Augspurger`_
- Remove buggy assertions (:pr:`5783`) `Tom Augspurger`_
- Pandas 1.0 compat (:pr:`5782`) `Tom Augspurger`_
- Fix bug in pyarrow-based ``read_parquet`` on partitioned datasets (:pr:`5777`) `Richard J Zamora`_
- Compat for pandas 1.0 (:pr:`5779`) `Tom Augspurger`_
- Fix groupby/mean error with with categorical index (:pr:`5776`) `Richard J Zamora`_
- Support empty partitions when performing cumulative aggregation (:pr:`5730`) `Matthew Rocklin`_
- ``set_index`` accepts single-item unnested list (:pr:`5760`) `Wes Roach`_
- Fixed partitioning in set index for ordered ``Categorical`` (:pr:`5715`) `Tom Augspurger`_

Documentation
+++++++++++++

- Note additional use case for ``normalize_token.register`` (:pr:`5766`) `Thomas A Caswell`_
- Update bag ``repartition`` docstring (:pr:`5772`) `Timost`_
- Small typos (:pr:`5771`) `Maarten Breddels`_
- Fix typo in Task Expectations docs (:pr:`5767`) `James Bourbeau`_
- Add docs section on task expectations to graph page (:pr:`5764`) `Devin Petersohn`_


2.9.1 / 2019-12-27
------------------

Array
+++++

-  Support Array.view with dtype=None (:pr:`5736`) `Anderson Banihirwe`_
-  Add dask.array.nanmedian (:pr:`5684`) `Deepak Cherian`_

Core
++++

-  xfail test_temporary_directory on Python 3.8 (:pr:`5734`) `James Bourbeau`_
-  Add support for Python 3.8 (:pr:`5603`) `James Bourbeau`_
-  Use id to dedupe constants in rewrite_blockwise (:pr:`5696`) `Jim Crist`_

DataFrame
+++++++++

-  Raise error when converting a dask dataframe scalar to a boolean (:pr:`5743`) `James Bourbeau`_
-  Ensure dataframe groupby-variance is greater than zero (:pr:`5728`) `Matthew Rocklin`_
-  Fix DataFrame.__iter__ (:pr:`5719`) `Tom Augspurger`_
-  Support Parquet filters in disjunctive normal form, like PyArrow (:pr:`5656`) `Matteo De Wint`_
-  Auto-detect categorical columns in ArrowEngine-based read_parquet (:pr:`5690`) `Richard J Zamora`_
-  Skip parquet getitem optimization tests if no engine found (:pr:`5697`) `James Bourbeau`_
-  Fix independent optimization of parquet-getitem (:pr:`5613`) `Tom Augspurger`_

Documentation
+++++++++++++

-  Update helm config doc (:pr:`5750`) `Ray Bell`_
-  Link to examples.dask.org in several places (:pr:`5733`) `Tom Augspurger`_
-  Add missing " in performance report example (:pr:`5724`) `James Bourbeau`_
-  Resolve several documentation build warnings (:pr:`5685`) `James Bourbeau`_
-  add info on performance_report (:pr:`5713`) `Ben Zaitlen`_
-  Add more docs disclaimers (:pr:`5710`) `Julia Signell`_
-  Fix simple typo: wihout -> without (:pr:`5708`) `Tim Gates`_
-  Update numpydoc dependency (:pr:`5694`) `James Bourbeau`_


2.9.0 / 2019-12-06
------------------

Array
+++++
- Fix ``da.std`` to work with NumPy arrays (:pr:`5681`) `James Bourbeau`_

Core
++++
- Register ``sizeof`` functions for Numba and RMM (:pr:`5668`) `John A Kirkham`_
- Update meeting time (:pr:`5682`) `Tom Augspurger`_

DataFrame
+++++++++
- Modify ``dd.DataFrame.drop`` to use shallow copy (:pr:`5675`) `Richard J Zamora`_
- Fix bug in ``_get_md_row_groups`` (:pr:`5673`) `Richard J Zamora`_
- Close sqlalchemy engine after querying DB (:pr:`5629`) `Krishan Bhasin`_
- Allow ``dd.map_partitions`` to not enforce meta (:pr:`5660`) `Matthew Rocklin`_
- Generalize ``concat_unindexed_dataframes`` to support cudf-backend (:pr:`5659`) `Richard J Zamora`_
- Add dataframe resample methods (:pr:`5636`) `Ben Zaitlen`_
- Compute length of dataframe as length of first column (:pr:`5635`) `Matthew Rocklin`_

Documentation
+++++++++++++
- Doc fixup (:pr:`5665`) `James Bourbeau`_
- Update doc build instructions (:pr:`5640`) `James Bourbeau`_
- Fix ADL link (:pr:`5639`) `Ray Bell`_
- Add documentation build (:pr:`5617`) `James Bourbeau`_


2.8.1 / 2019-11-22
------------------

Array
+++++
- Use auto rechunking in ``da.rechunk`` if no value given (:pr:`5605`) `Matthew Rocklin`_

Core
++++
- Add simple action to activate GH actions (:pr:`5619`) `James Bourbeau`_

DataFrame
+++++++++
- Fix "file_path_0" bug in ``aggregate_row_groups`` (:pr:`5627`) `Richard J Zamora`_
- Add ``chunksize`` argument to ``read_parquet`` (:pr:`5607`) `Richard J Zamora`_
- Change ``test_repartition_npartitions`` to support arch64 architecture (:pr:`5620`) `ossdev07`_
- Categories lost after groupby + agg (:pr:`5423`) `Oliver Hofkens`_
- Fixed relative path issue with parquet metadata file (:pr:`5608`) `Nuno Gomes Silva`_
- Enable gpu-backed covariance/correlation in dataframes (:pr:`5597`) `Richard J Zamora`_

Documentation
+++++++++++++
- Fix institutional faq and unknown doc warnings (:pr:`5616`) `James Bourbeau`_
- Add doc for some utils (:pr:`5609`) `Tom Augspurger`_
- Removes ``html_extra_path`` (:pr:`5614`) `James Bourbeau`_
- Fixed See Also referencence (:pr:`5612`) `Tom Augspurger`_


2.8.0 / 2019-11-14
------------------

Array
+++++
-  Implement complete dask.array.tile function (:pr:`5574`) `Bouwe Andela`_
-  Add median along an axis with automatic rechunking (:pr:`5575`) `Matthew Rocklin`_
-  Allow da.asarray to chunk inputs (:pr:`5586`) `Matthew Rocklin`_

Bag
+++

-  Use key_split in Bag name (:pr:`5571`) `Matthew Rocklin`_

Core
++++
-  Switch Doctests to Py3.7 (:pr:`5573`) `Ryan Nazareth`_
-  Relax get_colors test to adapt to new Bokeh release (:pr:`5576`) `Matthew Rocklin`_
-  Add dask.blockwise.fuse_roots optimization (:pr:`5451`) `Matthew Rocklin`_
-  Add sizeof implementation for small dicts (:pr:`5578`) `Matthew Rocklin`_
-  Update fsspec, gcsfs, s3fs (:pr:`5588`) `Tom Augspurger`_

DataFrame
+++++++++
-  Add dropna argument to groupby (:pr:`5579`) `Richard J Zamora`_
-  Revert "Remove import of dask_cudf, which is now a part of cudf (:pr:`5568`)" (:pr:`5590`) `Matthew Rocklin`_

Documentation
+++++++++++++

-  Add best practice for dask.compute function (:pr:`5583`) `Matthew Rocklin`_
-  Create FUNDING.yml (:pr:`5587`) `Gina Helfrich`_
-  Add screencast for coordination primitives (:pr:`5593`) `Matthew Rocklin`_
-  Move funding to .github repo (:pr:`5589`) `Tom Augspurger`_
-  Update calendar link (:pr:`5569`) `Tom Augspurger`_


2.7.0 / 2019-11-08
------------------

This release drops support for Python 3.5

Array
+++++

-  Reuse code for assert_eq util method (:pr:`5496`) `Vijayant`_
-  Update da.array to always return a dask array (:pr:`5510`) `James Bourbeau`_
-  Skip transpose on trivial inputs (:pr:`5523`) `Ryan Abernathey`_
-  Avoid NumPy scalar string representation in tokenize (:pr:`5527`) `James Bourbeau`_
-  Remove unnecessary tiledb shape constraint (:pr:`5545`) `Norman Barker`_
-  Removes bytes from sparse array HTML repr (:pr:`5556`) `James Bourbeau`_

Core
++++

-  Drop Python 3.5 (:pr:`5528`) `James Bourbeau`_
-  Update the use of fixtures in distributed tests (:pr:`5497`) `Matthew Rocklin`_
-  Changed deprecated bokeh-port to dashboard-address (:pr:`5507`) `darindf`_
-  Avoid updating with identical dicts in ensure_dict (:pr:`5501`) `James Bourbeau`_
-  Test Upstream (:pr:`5516`) `Tom Augspurger`_
-  Accelerate reverse_dict (:pr:`5479`) `Ryan Grout`_
-  Update test_imports.sh (:pr:`5534`) `James Bourbeau`_
-  Support cgroups limits on cpu count in multiprocess and threaded schedulers (:pr:`5499`) `Albert DeFusco`_
-  Update minimum pyarrow version on CI (:pr:`5562`) `James Bourbeau`_
-  Make cloudpickle optional (:pr:`5511`) `Guido Imperiale`_

DataFrame
+++++++++

-  Add an example of index_col usage (:pr:`3072`) `Bruno Bonfils`_
-  Explicitly use iloc for row indexing (:pr:`5500`) `Krishan Bhasin`_
-  Accept dask arrays on columns assignemnt (:pr:`5224`) `Henrique Ribeiro`-
-  Implement unique and value_counts for SeriesGroupBy (:pr:`5358`) `Scott Sievert`_
-  Add sizeof definition for pyarrow tables and columns (:pr:`5522`) `Richard J Zamora`_
-  Enable row-group task partitioning in pyarrow-based read_parquet (:pr:`5508`) `Richard J Zamora`_
-  Removes npartitions='auto' from dd.merge docstring (:pr:`5531`) `James Bourbeau`_
-  Apply enforce error message shows non-overlapping columns. (:pr:`5530`) `Tom Augspurger`_
-  Optimize meta_nonempty for repetitive dtypes (:pr:`5553`) `Petio Petrov`_
-  Remove import of dask_cudf, which is now a part of cudf (:pr:`5568`) `Mads R. B. Kristensen`_

Documentation
+++++++++++++

-  Make capitalization more consistent in FAQ docs (:pr:`5512`) `Matthew Rocklin`_
-  Add CONTRIBUTING.md (:pr:`5513`) `Jacob Tomlinson`_
-  Document optional dependencies (:pr:`5456`) `Prithvi MK`_
-  Update helm chart docs to reflect new chart repo (:pr:`5539`) `Jacob Tomlinson`_
-  Add Resampler to API docs (:pr:`5551`) `James Bourbeau`_
-  Fix typo in read_sql_table (:pr:`5554`) `Eric Dill`_
-  Add adaptive deployments screencast [skip ci] (:pr:`5566`) `Matthew Rocklin`_


2.6.0 / 2019-10-15
------------------

Core
++++

- Call ``ensure_dict`` on graphs before entering ``toolz.merge`` (:pr:`5486`) `Matthew Rocklin`_
- Consolidating hash dispatch functions (:pr:`5476`) `Richard J Zamora`_

DataFrame
+++++++++

- Support Python 3.5 in Parquet code (:pr:`5491`) `Ben Zaitlen`_
- Avoid identity check in ``warn_dtype_mismatch`` (:pr:`5489`) `Tom Augspurger`_
- Enable unused groupby tests (:pr:`3480`) `Jörg Dietrich`_
- Remove old parquet and bcolz dataframe optimizations (:pr:`5484`) `Matthew Rocklin`_
- Add getitem optimization for ``read_parquet`` (:pr:`5453`) `Tom Augspurger`_
- Use ``_constructor_sliced`` method to determine Series type (:pr:`5480`) `Richard J Zamora`_
- Fix map(series) for unsorted base series index (:pr:`5459`) `Justin Waugh`_
- Fix ``KeyError`` with Groupby label (:pr:`5467`) `Ryan Nazareth`_

Documentation
+++++++++++++

- Use Zoom meeting instead of appear.in (:pr:`5494`) `Matthew Rocklin`_
- Added curated list of resources (:pr:`5460`) `Javad`_
- Update SSH docs to include ``SSHCluster`` (:pr:`5482`) `Matthew Rocklin`_
- Update "Why Dask?" page (:pr:`5473`) `Matthew Rocklin`_
- Fix typos in docstrings (:pr:`5469`) `garanews`_


2.5.2 / 2019-10-04
------------------

Array
+++++

-  Correct chunk size logic for asymmetric overlaps (:pr:`5449`) `Ben Jeffery`_
-  Make da.unify_chunks public API (:pr:`5443`) `Matthew Rocklin`_

DataFrame
+++++++++

-  Fix dask.dataframe.fillna handling of Scalar object (:pr:`5463`) `Zhenqing Li`_

Documentation
+++++++++++++

-  Remove boxes in Spark comparison page (:pr:`5445`) `Matthew Rocklin`_
-  Add latest presentations (:pr:`5446`) `Javad`_
-  Update cloud documentation (:pr:`5444`) `Matthew Rocklin`_


2.5.0 / 2019-09-27
------------------

Core
++++

-  Add sentinel no_default to get_dependencies task (:pr:`5420`) `James Bourbeau`_
-  Update fsspec version (:pr:`5415`) `Matthew Rocklin`_
-  Remove PY2 checks (:pr:`5400`) `Jim Crist`_

DataFrame
+++++++++

-  Add option to not check meta in dd.from_delayed (:pr:`5436`) `Christopher J. Wright`_
-  Fix test_timeseries_nulls_in_schema failures with pyarrow master (:pr:`5421`) `Richard J Zamora`_
-  Reduce read_metadata output size in pyarrow/parquet (:pr:`5391`) `Richard J Zamora`_
-  Test numeric edge case for repartition with npartitions. (:pr:`5433`) `amerkel2`_
-  Unxfail pandas-datareader test (:pr:`5430`) `Tom Augspurger`_
-  Add DataFrame.pop implementation (:pr:`5422`) `Matthew Rocklin`_
-  Enable merge/set_index for cudf-based dataframes with cupy ``values`` (:pr:`5322`) `Richard J Zamora`_
-  drop_duplicates support for positional subset parameter (:pr:`5410`) `Wes Roach`_

Documentation
+++++++++++++

-  Add screencasts to array, bag, dataframe, delayed, futures and setup  (:pr:`5429`) (:pr:`5424`) `Matthew Rocklin`_
-  Fix delimeter parsing documentation (:pr:`5428`) `Mahmut Bulut`_
-  Update overview image (:pr:`5404`) `James Bourbeau`_


2.4.0 / 2019-09-13
------------------

Array
+++++

- Adds explicit ``h5py.File`` mode (:pr:`5390`) `James Bourbeau`_
- Provides method to compute unknown array chunks sizes (:pr:`5312`) `Scott Sievert`_
- Ignore runtime warning in Array ``compute_meta`` (:pr:`5356`) `estebanag`_
- Add ``_meta`` to ``Array.__dask_postpersist__`` (:pr:`5353`) `Benoit Bovy`_
- Fixup ``da.asarray`` and ``da.asanyarray`` for datetime64 dtype and xarray objects (:pr:`5334`) `Stephan Hoyer`_
- Add shape implementation (:pr:`5293`) `Tom Augspurger`_
- Add chunktype to array text repr (:pr:`5289`) `James Bourbeau`_
- Array.random.choice: handle array-like non-arrays (:pr:`5283`) `Gabe Joseph`_

Core
++++

- Remove deprecated code (:pr:`5401`) `Jim Crist`_
- Fix ``funcname`` when vectorized func has no ``__name__`` (:pr:`5399`) `James Bourbeau`_
- Truncate ``funcname`` to avoid long key names (:pr:`5383`) `Matthew Rocklin`_
- Add support for ``numpy.vectorize`` in ``funcname`` (:pr:`5396`) `James Bourbeau`_
- Fixed HDFS upstream test (:pr:`5395`) `Tom Augspurger`_
- Support numbers and None in ``parse_bytes``/``timedelta`` (:pr:`5384`) `Matthew Rocklin`_
- Fix tokenizing of subindexes on memmapped numpy arrays (:pr:`5351`) `Henry Pinkard`_
- Upstream fixups (:pr:`5300`) `Tom Augspurger`_

DataFrame
+++++++++

- Allow pandas to cast type of statistics (:pr:`5402`) `Richard J Zamora`_
- Preserve index dtype after applying ``dd.pivot_table`` (:pr:`5385`) `therhaag`_
- Implement explode for Series and DataFrame (:pr:`5381`) `Arpit Solanki`_
- ``set_index`` on categorical fails with less categories than partitions (:pr:`5354`) `Oliver Hofkens`_
- Support output to a single CSV file (:pr:`5304`) `Hongjiu Zhang`_
- Add ``groupby().transform()`` (:pr:`5327`) `Oliver Hofkens`_
- Adding filter kwarg to pyarrow dataset call (:pr:`5348`) `Richard J Zamora`_
- Implement and check compression defaults for parquet (:pr:`5335`) `Sarah Bird`_
- Pass sqlalchemy params to delayed objects (:pr:`5332`) `Arpit Solanki`_
- Fixing schema handling in arrow-parquet (:pr:`5307`) `Richard J Zamora`_
- Add support for DF and Series ``groupby().idxmin/max()`` (:pr:`5273`) `Oliver Hofkens`_
- Add correlation calculation and add test (:pr:`5296`) `Ben Zaitlen`_

Documentation
+++++++++++++

- Numpy docstring standard has moved (:pr:`5405`) `Wes Roach`_
- Reference correct NumPy array name (:pr:`5403`) `Wes Roach`_
- Minor edits to Array chunk documentation (:pr:`5372`) `Scott Sievert`_
- Add methods to API docs (:pr:`5387`) `Tom Augspurger`_
- Add namespacing to configuration example (:pr:`5374`) `Matthew Rocklin`_
- Add get_task_stream and profile to the diagnostics page (:pr:`5375`) `Matthew Rocklin`_
- Add best practice to load data with Dask (:pr:`5369`) `Matthew Rocklin`_
- Update ``institutional-faq.rst`` (:pr:`5345`) `DomHudson`_
- Add threads and processes note to the best practices (:pr:`5340`) `Matthew Rocklin`_
- Update cuDF links (:pr:`5328`) `James Bourbeau`_
- Fixed small typo with parentheses placement (:pr:`5311`) `Eugene Huang`_
- Update link in reshape docstring (:pr:`5297`) `James Bourbeau`_


2.3.0 / 2019-08-16
------------------

Array
+++++

- Raise exception when ``from_array`` is given a dask array (:pr:`5280`) `David Hoese`_
- Avoid adjusting gufunc's meta dtype twice (:pr:`5274`) `Peter Andreas Entschev`_
- Add ``meta=`` keyword to map_blocks and add test with sparse (:pr:`5269`) `Matthew Rocklin`_
- Add rollaxis and moveaxis (:pr:`4822`) `Tobias de Jong`_
- Always increment old chunk index (:pr:`5256`) `James Bourbeau`_
- Shuffle dask array (:pr:`3901`) `Tom Augspurger`_
- Fix ordering when indexing a dask array with a bool dask array (:pr:`5151`) `James Bourbeau`_

Bag
+++

- Add workaround for memory leaks in bag generators (:pr:`5208`) `Marco Neumann`_

Core
++++

- Set strict xfail option (:pr:`5220`) `James Bourbeau`_
- test-upstream (:pr:`5267`) `Tom Augspurger`_
- Fixed HDFS CI failure (:pr:`5234`) `Tom Augspurger`_
- Error nicely if no file size inferred (:pr:`5231`) `Jim Crist`_
- A few changes to ``config.set`` (:pr:`5226`) `Jim Crist`_
- Fixup black string normalization (:pr:`5227`) `Jim Crist`_
- Pin NumPy in windows tests (:pr:`5228`) `Jim Crist`_
- Ensure parquet tests are skipped if fastparquet and pyarrow not installed (:pr:`5217`) `James Bourbeau`_
- Add fsspec to readthedocs (:pr:`5207`) `Matthew Rocklin`_
- Bump NumPy and Pandas to 1.17 and 0.25 in CI test (:pr:`5179`) `John A Kirkham`_

DataFrame
+++++++++

- Fix ``DataFrame.query`` docstring (incorrect numexpr API) (:pr:`5271`) `Doug Davis`_
- Parquet metadata-handling improvements (:pr:`5218`) `Richard J Zamora`_
- Improve messaging around sorted parquet columns for index (:pr:`5265`) `Martin Durant`_
- Add ``rearrange_by_divisions`` and ``set_index`` support for cudf (:pr:`5205`) `Richard J Zamora`_
- Fix ``groupby.std()`` with integer colum names (:pr:`5096`) `Nicolas Hug`_
- Add ``Series.__iter__`` (:pr:`5071`) `Blane`_
- Generalize ``hash_pandas_object`` to work for non-pandas backends (:pr:`5184`) `GALI PREM SAGAR`_
- Add rolling cov (:pr:`5154`) `Ivars Geidans`_
- Add columns argument in drop function (:pr:`5223`) `Henrique Ribeiro`_

Documentation
+++++++++++++

- Update institutional FAQ doc (:pr:`5277`) `Matthew Rocklin`_
- Add draft of institutional FAQ (:pr:`5214`) `Matthew Rocklin`_
- Make boxes for dask-spark page (:pr:`5249`) `Martin Durant`_
- Add motivation for shuffle docs (:pr:`5213`) `Matthew Rocklin`_
- Fix links and API entries for best-practices (:pr:`5246`) `Martin Durant`_
- Remove "bytes" (internal data ingestion) doc page (:pr:`5242`) `Martin Durant`_
- Redirect from our local distributed page to distributed.dask.org (:pr:`5248`) `Matthew Rocklin`_
- Cleanup API page (:pr:`5247`) `Matthew Rocklin`_
- Remove excess endlines from install docs (:pr:`5243`) `Matthew Rocklin`_
- Remove item list in phases of computation doc (:pr:`5245`) `Martin Durant`_
- Remove custom graphs from the TOC sidebar (:pr:`5241`) `Matthew Rocklin`_
- Remove experimental status of custom collections (:pr:`5236`) `James Bourbeau`_
- Adds table of contents to Why Dask? (:pr:`5244`) `James Bourbeau`_
- Moves bag overview to top-level bag page (:pr:`5240`) `James Bourbeau`_
- Remove use-cases in favor of stories.dask.org (:pr:`5238`) `Matthew Rocklin`_
- Removes redundant TOC information in index.rst (:pr:`5235`) `James Bourbeau`_
- Elevate dashboard in distributed diagnostics documentation (:pr:`5239`) `Martin Durant`_
- Updates "add" layer in HLG docs example (:pr:`5237`) `James Bourbeau`_
- Update GUFunc documentation (:pr:`5232`) `Matthew Rocklin`_


2.2.0 / 2019-08-01
------------------

Array
+++++

-  Use da.from_array(..., asarray=False) if input follows NEP-18 (:pr:`5074`) `Matthew Rocklin`_
-  Add missing attributes to from_array documentation (:pr:`5108`) `Peter Andreas Entschev`_
-  Fix meta computation for some reduction functions (:pr:`5035`) `Peter Andreas Entschev`_
-  Raise informative error in to_zarr if unknown chunks (:pr:`5148`) `James Bourbeau`_
-  Remove invalid pad tests (:pr:`5122`) `Tom Augspurger`_
-  Ignore NumPy warnings in compute_meta (:pr:`5103`) `Peter Andreas Entschev`_
-  Fix kurtosis calc for single dimension input array (:pr:`5177`) `@andrethrill`_
-  Support Numpy 1.17 in tests (:pr:`5192`) `Matthew Rocklin`_

Bag
+++

-  Supply pool to bag test to resolve intermittent failure (:pr:`5172`) `Tom Augspurger`_

Core
++++

-  Base dask on fsspec (:pr:`5064`) (:pr:`5121`) `Martin Durant`_
-  Various upstream compatibility fixes (:pr:`5056`) `Tom Augspurger`_
-  Make distributed tests optional again. (:pr:`5128`) `Elliott Sales de Andrade`_
-  Fix HDFS in dask (:pr:`5130`) `Martin Durant`_
-  Ignore some more invalid value warnings. (:pr:`5140`) `Elliott Sales de Andrade`_

DataFrame
+++++++++

-  Fix pd.MultiIndex size estimate (:pr:`5066`) `Brett Naul`_
-  Generalizing has_known_categories (:pr:`5090`) `GALI PREM SAGAR`_
-  Refactor Parquet engine (:pr:`4995`) `Richard J Zamora`_
-  Add divide method to series and dataframe (:pr:`5094`) `msbrown47`_
-  fix flaky partd test (:pr:`5111`) `Tom Augspurger`_
-  Adjust is_dataframe_like to adjust for value_counts change (:pr:`5143`) `Tom Augspurger`_
-  Generalize rolling windows to support non-Pandas dataframes (:pr:`5149`) `Nick Becker`_
-  Avoid unnecessary aggregation in pivot_table (:pr:`5173`) `Daniel Saxton`_
-  Add column names to apply_and_enforce error message (:pr:`5180`) `Matthew Rocklin`_
-  Add schema keyword argument to to_parquet (:pr:`5150`) `Sarah Bird`_
-  Remove recursion error in accessors (:pr:`5182`) `Jim Crist`_
-  Allow fastparquet to handle gather_statistics=False for file lists (:pr:`5157`) `Richard J Zamora`_

Documentation
+++++++++++++

-  Adds NumFOCUS badge to the README (:pr:`5086`) `James Bourbeau`_
-  Update developer docs [ci skip] (:pr:`5093`) `Jim Crist`_
-  Document DataFrame.set_index computataion behavior `Natalya Rapstine`_
-  Use pip install . instead of calling setup.py (:pr:`5139`) `Matthias Bussonier`_
-  Close user survey (:pr:`5147`) `Tom Augspurger`_
-  Fix Google Calendar meeting link (:pr:`5155`) `Loïc Estève`_
-  Add docker image customization example (:pr:`5171`) `James Bourbeau`_
-  Update remote-data-services after fsspec (:pr:`5170`) `Martin Durant`_
-  Fix typo in spark.rst (:pr:`5164`) `Xavier Holt`_
-  Update setup/python docs for async/await API (:pr:`5163`) `Matthew Rocklin`_
-  Update Local Storage HPC documentation (:pr:`5165`) `Matthew Rocklin`_



2.1.0 / 2019-07-08
------------------

Array
+++++

- Add ``recompute=`` keyword to ``svd_compressed`` for lower-memory use (:pr:`5041`) `Matthew Rocklin`_
- Change ``__array_function__`` implementation for backwards compatibility (:pr:`5043`) `Ralf Gommers`_
- Added ``dtype`` and ``shape`` kwargs to ``apply_along_axis`` (:pr:`3742`) `Davis Bennett`_
- Fix reduction with empty tuple axis (:pr:`5025`) `Peter Andreas Entschev`_
- Drop size 0 arrays in ``stack`` (:pr:`4978`) `John A Kirkham`_

Core
++++

- Removes index keyword from pandas ``to_parquet`` call (:pr:`5075`) `James Bourbeau`_
- Fixes upstream dev CI build installation (:pr:`5072`) `James Bourbeau`_
- Ensure scalar arrays are not rendered to SVG (:pr:`5058`) `Willi Rath`_
- Environment creation overhaul (:pr:`5038`) `Tom Augspurger`_
- s3fs, moto compatibility (:pr:`5033`) `Tom Augspurger`_
- pytest 5.0 compat (:pr:`5027`) `Tom Augspurger`_

DataFrame
+++++++++

- Fix ``compute_meta`` recursion in blockwise (:pr:`5048`) `Peter Andreas Entschev`_
- Remove hard dependency on pandas in ``get_dummies`` (:pr:`5057`) `GALI PREM SAGAR`_
- Check dtypes unchanged when using ``DataFrame.assign`` (:pr:`5047`) `asmith26`_
- Fix cumulative functions on tables with more than 1 partition (:pr:`5034`) `tshatrov`_
- Handle non-divisible sizes in repartition (:pr:`5013`) `George Sakkis`_
- Handles timestamp and ``preserve_index`` changes in pyarrow (:pr:`5018`) `Richard J Zamora`_
- Fix undefined ``meta`` for ``str.split(expand=False)`` (:pr:`5022`) `Brett Naul`_
- Removed checks used for debugging ``merge_asof`` (:pr:`5011`) `Cody Johnson`_
- Don't use type when getting accessor in dataframes (:pr:`4992`) `Matthew Rocklin`_
- Add ``melt`` as a method of Dask DataFrame (:pr:`4984`) `Dustin Tindall`_
- Adds path-like support to ``to_hdf`` (:pr:`5003`) `James Bourbeau`_

Documentation
+++++++++++++

- Point to latest K8s setup article in JupyterHub docs (:pr:`5065`) `Sean McKenna`_
- Changes vizualize to visualize (:pr:`5061`) `David Brochart`_
- Fix ``from_sequence`` typo in delayed best practices (:pr:`5045`) `James Bourbeau`_
- Add user survey link to docs (:pr:`5026`) `James Bourbeau`_
- Fixes typo in optimization docs (:pr:`5015`) `James Bourbeau`_
- Update community meeting information (:pr:`5006`) `Tom Augspurger`_


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
-  Propagate index metadata if not specified (:pr:`4509`) `Jim Crist`_

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

- Document ``bag.map_paritions`` function may receive either a list or generator. (:pr:`3150`) `Nir`_

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
- Compatibility for changes to structured arrays in the upcoming NumPy 1.14 release (:pr:`2964`) `Tom Augspurger`_
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
- Compatibility for reading Parquet files written by PyArrow 0.8.0 (:pr:`2973`) `Tom Augspurger`_
- Correctly handle the column name (`df.columns.name`) when reading in ``dd.read_parquet`` (:pr:`2973`) `Tom Augspurger`_
- Fixed ``dd.concat`` losing the index dtype when the data contained a categorical (:issue:`2932`) `Tom Augspurger`_
- Add ``dd.Series.rename`` (:pr:`3027`) `Jim Crist`_
- ``DataFrame.merge()`` now supports merging on a combination of columns and the index (:pr:`2960`) `Jon Mease`_
- Removed the deprecated ``dd.rolling*`` methods, in preparation for their removal in the next pandas release (:pr:`2995`) `Tom Augspurger`_
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
-  Compatibility with bokeh 0.12.10 (:pr:`2844`) `Tom Augspurger`_
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
.. _`Jon Mease`: https://github.com/jonmmease
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
.. _`Daniel Severo`: https://github.com/dsevero
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
.. _`Justin Poehnelt`: https://github.com/jpoehnelt
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
.. _`Ralf Gommers`: https://github.com/rgommers
.. _`Davis Bennett`: https://github.com/d-v-b
.. _`Willi Rath`: https://github.com/willirath
.. _`David Brochart`: https://github.com/davidbrochart
.. _`GALI PREM SAGAR`: https://github.com/galipremsagar
.. _`tshatrov`: https://github.com/tshatrov
.. _`Dustin Tindall`: https://github.com/dustindall
.. _`Sean McKenna`: https://github.com/seanmck
.. _`msbrown47`: https://github.com/msbrown47
.. _`Natalya Rapstine`: https://github.com/natalya-patrikeeva
.. _`Loïc Estève`: https://github.com/lesteve
.. _`Xavier Holt`: https://github.com/xavi-ai
.. _`Sarah Bird`: https://github.com/birdsarah
.. _`Doug Davis`: https://github.com/douglasdavis
.. _`Nicolas Hug`: https://github.com/NicolasHug
.. _`Blane`: https://github.com/BlaneG
.. _`Ivars Geidans`: https://github.com/ivarsfg
.. _`Scott Sievert`: https://github.com/stsievert
.. _`estebanag`: https://github.com/estebanag
.. _`Benoit Bovy`: https://github.com/benbovy
.. _`Gabe Joseph`: https://github.com/gjoseph92
.. _`therhaag`: https://github.com/therhaag
.. _`Arpit Solanki`: https://github.com/arpit1997
.. _`Oliver Hofkens`: https://github.com/OliverHofkens
.. _`Hongjiu Zhang`: https://github.com/hongzmsft
.. _`Wes Roach`: https://github.com/WesRoach
.. _`DomHudson`: https://github.com/DomHudson
.. _`Eugene Huang`: https://github.com/eugeneh101
.. _`Christopher J. Wright`: https://github.com/CJ-Wright
.. _`Mahmut Bulut`: https://github.com/vertexclique
.. _`Ben Jeffery`: https://github.com/benjeffery
.. _`Ryan Nazareth`: https://github.com/ryankarlos
.. _`garanews`: https://github.com/garanews
.. _`Vijayant`: https://github.com/VijayantSoni
.. _`Ryan Abernathey`: https://github.com/rabernat
.. _`Norman Barker`: https://github.com/normanb
.. _`darindf`: https://github.com/darindf
.. _`Ryan Grout`: https://github.com/groutr
.. _`Krishan Bhasin`: https://github.com/KrishanBhasin
.. _`Albert DeFusco`: https://github.com/AlbertDeFusco
.. _`Bruno Bonfils`: https://github.com/asyd
.. _`Petio Petrov`: https://github.com/petioptrv
.. _`Mads R. B. Kristensen`: https://github.com/madsbk
.. _`Prithvi MK`: https://github.com/pmk21
.. _`Eric Dill`: https://github.com/ericdill
.. _`Gina Helfrich`: https://github.com/Dr-G
.. _`ossdev07`: https://github.com/ossdev07
.. _`Nuno Gomes Silva`: https://github.com/mgsnuno
.. _`Ray Bell`: https://github.com/raybellwaves
.. _`Deepak Cherian`: https://github.com/dcherian
.. _`Matteo De Wint`: https://github.com/mdwint
.. _`Tim Gates`: https://github.com/timgates42
.. _`Erik Welch`: https://github.com/eriknw
.. _`Christian Wesp`: https://github.com/ChrWesp
.. _`Shiva Raisinghani`: https://github.com/exemplary-citizen
.. _`Thomas A Caswell`: https://github.com/tacaswell
.. _`Timost`: https://github.com/Timost
.. _`Maarten Breddels`: https://github.com/maartenbreddels
.. _`Devin Petersohn`: https://github.com/devin-petersohn
.. _`dfonnegra`: https://github.com/dfonnegra
.. _`Chris Roat`: https://github.com/ChrisRoat
.. _`H. Thomson Comer`: https://github.com/thomcom
.. _`Gerrit Holl`: https://github.com/gerritholl
.. _`Thomas Robitaille`: https://github.com/astrofrog
.. _`Yifan Gu`: https://github.com/gyf304
.. _`Surya Avala`: https://github.com/suryaavala
.. _`Cyril Shcherbin`: https://github.com/shcherbin
.. _`Ram Rachum`: https://github.com/cool-RR
.. _`Igor Gotlibovych`: https://github.com/ig248
.. _`K.-Michael Aye`: https://github.com/michaelaye
.. _`Yetunde Dada`: https://github.com/yetudada
.. _`Andrew Thomas`: https://github.com/amcnicho
.. _`rockwellw`: https://github.com/rockwellw
.. _`Gil Forsyth`: https://github.com/gforsyth
.. _`Thomas J Fan`: https://github.com/thomasjpfan
.. _`Henrik Andersson`: https://github.com/hnra
.. _`James Lamb`: https://github.com/jameslamb
.. _`Corey J. Nolet`: https://github.com/cjnolet
.. _`Chuanzhu Xu`: https://github.com/xcz011
.. _`Lucas Rademaker`: https://github.com/lr4d
.. _`JulianWgs`: https://github.com/JulianWgs
.. _`psimaj`: https://github.com/psimaj
.. _`mlondschien`: https://github.com/mlondschien
.. _`petiop`: https://github.com/petiop
.. _`Richard (Rick) Zamora`: https://github.com/rjzamora
.. _`Mark Boer`: https://github.com/mark-boer
.. _`Florian Jetter`: https://github.com/fjetter
.. _`Adam Lewis`: https://github.com/balast
.. _`David Chudzicki`: https://github.com/dchudz
.. _`Nick Evans`: https://github.com/nre
.. _`Kai Mühlbauer`: https://github.com/kmuehlbauer
.. _`swapna`: https://github.com/swapna-pg
.. _`Antonio Ercole De Luca`: https://github.com/eracle
.. _`Amol Umbarkar`: https://github.com/mindhash
.. _`noreentry`: https://github.com/noreentry
.. _`Marius van Niekerk`: https://github.com/mariusvniekerk
.. _`Tung Dang`: https://github.com/3cham
.. _`Jim Crist-Harif`: https://github.com/jcrist
.. _`Brian Larsen`: https://github.com/brl0
.. _`Nils Braun`: https://github.com/nils-braun
.. _`Scott Sanderson`: https://github.com/ssanderson
.. _`Gaurav Sheni`: https://github.com/gsheni
.. _`Andrew Fulton`: https://github.com/andrewfulton9
.. _`Stephanie Gott`: https://github.com/stephaniegott
.. _`Huite`: https://github.com/Huite
.. _`Ryan Williams`: https://github.com/ryan-williams
.. _`Eric Czech`: https://github.com/eric-czech
.. _`Abdulelah Bin Mahfoodh`: https://github.com/abduhbm
.. _`Ben Shaver`: https://github.com/bpshaver
.. _`Matthias Bussonnier`: https://github.com/Carreau
.. _`johnomotani`: https://github.com/johnomotani
.. _`Roberto Panai`: https://github.com/rpanai
.. _`Clark Zinzow`: https://github.com/clarkzinzow
.. _`Tom McTiernan`: https://github.com/tmct
.. _`Zhengnan`: https://github.com/ZhengnanZhao
.. _`joshreback`: https://github.com/joshreback
.. _`Jun Han (Johnson) Ooi`: https://github.com/tebesfinwo
.. _`Jim Circadian`: https://github.com/JimCircadian
.. _`Jack Xiaosong Xu`: https://github.com/jackxxu
.. _`Mike McCarty`: https://github.com/mmccarty
.. _`michaelnarodovitch`: https://github.com/michaelnarodovitch
.. _`David Sheldon`: https://github.com/davidsmf
.. _`McToel`: https://github.com/McToel
.. _`Kilian Lieret`: https://github.com/klieret
.. _`Noah D Brenowitz`: https://github.com/nbren12
.. _`Jon Thielen`: https://github.com/jthielen
.. _`Poruri Sai Rahul`: https://github.com/rahulporuri
.. _`Kyle Nicholson`: https://github.com/kylejn27
.. _`Rafal Wojdyla`: https://github.com/ravwojdyla
.. _`Sam Grayson`: https://github.com/charmoniumQ
.. _`Madhur Tandon`: https://github.com/madhur-tandon
.. _`Joachim B Haga`: https://github.com/jobh
.. _`Pav A`: https://github.com/rs2
