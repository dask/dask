## Dask-expr

# v1.1.11

- Make split_out for categorical default smarter (:pr:`1124`) `Patrick Hoefler`_
- Avoid calling ``array`` attribute on ``cudf.Series`` (:pr:`1122`) `Richard (Rick) Zamora`_
- Introduce `ToBackend` expression (:pr:`1115`) `Richard (Rick) Zamora`_
- Fix result index of merge (:pr:`1121`) `Patrick Hoefler`_
- Fix projection for Index class in read_parquet (:pr:`1120`) `Patrick Hoefler`_
- Register `read_parquet` and `read_csv` as "dispatchable" (:pr:`1114`) `Richard (Rick) Zamora`_
- Fix merging when index name in meta missmatches actual name (:pr:`1119`) `Patrick Hoefler`_
- Fix tuples as on argument in merge (:pr:`1117`) `Patrick Hoefler`_
- Drop support for Python 3.9 (:pr:`1109`) `Patrick Hoefler`_

# v1.1.10

- Fixup remaining upstream failures (:pr:`1111`) `Patrick Hoefler`_
- Fix some things for pandas 3 (:pr:`1110`) `Patrick Hoefler`_

# v1.1.9

- Patch release for Dask 2024.7.0

# v1.1.8

- Fix shuffle blowing up the task graph (:pr:`1108`) `Patrick Hoefler`_
- Link fix in readme (:pr:`1107`) `Ben`_
- Fix from_pandas with chunksize and empty df (:pr:`1106`) `Patrick Hoefler`_
- Fix deepcopying FromPandas class (:pr:`1105`) `Patrick Hoefler`_
- Skip test if optional xarray cannot be imported (:pr:`1104`) `Sandro`_

# v1.1.7

- Patch release for Dask 2024.7.0

# v1.1.6

# v1.1.5

- Patch release for Dask 2024.6.2

# v1.1.4

# v1.1.3

- Fix resample divisions propagation (:pr:`1075`) `Patrick Hoefler`_
- Fix categorize if columns are dropped (:pr:`1074`) `Patrick Hoefler`_

# v1.1.2

- Fix projection to empty from_pandas (:pr:`1072`) `Patrick Hoefler`_
- Fix meta for string accessors (:pr:`1071`) `Patrick Hoefler`_
- Use `is_categorical_dtype` dispatch for `sort_values` (:pr:`1070`) `Richard (Rick) Zamora`_

# v1.1.1

- Fix read_csv with positional usecols (:pr:`1069`) `Patrick Hoefler`_
- Fix isin for head computation (:pr:`1068`) `Patrick Hoefler`_
- Fix isin with strings (:pr:`1067`) `Patrick Hoefler`_
- Use ensure_deterministic kwarg instead of config (:pr:`1064`) `Florian Jetter`_
- Add cache  argument to ``lower_once`` (:pr:`1059`) `Richard (Rick) Zamora`_
- Fix non-integer divisions in FusedIO (:pr:`1063`) `Patrick Hoefler`_
- Fix dropna before merge (:pr:`1062`) `Patrick Hoefler`_
- Fix sort_values for unordered categories (:pr:`1058`) `Patrick Hoefler`_
- Fix to_parquet in append mode (:pr:`1057`) `Patrick Hoefler`_

# v1.1.0

- Add a bunch of docs (:pr:`1051`) `Patrick Hoefler`_
- reduce pickle size of parquet fragments (:pr:`1050`) `Florian Jetter`_
- Generalize ``get_dummies`` (:pr:`1053`) `Richard (Rick) Zamora`_
- Fixup failing test (:pr:`1052`) `Patrick Hoefler`_
- Add support for ``DataFrame.melt`` (:pr:`1049`) `Richard (Rick) Zamora`_
- Fix default name conversion in `ToFrame` (:pr:`1044`) `Richard (Rick) Zamora`_
- Optimize when from-delayed is called (:pr:`1048`) `Patrick Hoefler`_

# v1.0.14

- Fix delayed in fusing with multipled dependencies (:pr:`1038`) `Patrick Hoefler`_
- Fix ``drop`` with ``set`` (:pr:`1047`) `Patrick Hoefler`_
- Fix ``None`` min/max statistics and missing statistics generally (:pr:`1045`) `Patrick Hoefler`_
- Fix xarray integration with scalar columns (:pr:`1046`) `Patrick Hoefler`_
- Fix ``shape`` returning integer (:pr:`1043`) `Patrick Hoefler`_
- Fix bug in ``Series`` reductions (:pr:`1041`) `Richard (Rick) Zamora`_

# v1.0.13

- Fix shuffle after ``set_index`` from 1 partition df (:pr:`1040`) `Patrick Hoefler`_
- Fix loc slicing with Datetime Index (:pr:`1039`) `Patrick Hoefler`_
- Fix loc accessing index for element wise op (:pr:`1037`) `Patrick Hoefler`_
- Fix backend dispatching for ``read_csv`` (:pr:`1028`) `Richard (Rick) Zamora`_
- Add cudf support to ``to_datetime`` and ``_maybe_from_pandas`` (:pr:`1035`) `Richard (Rick) Zamora`_

# v1.0.12

- Move IO docstrings over (:pr:`1033`) `Patrick Hoefler`_
- Fuse more aggressively if parquet files are tiny (:pr:`1029`) `Patrick Hoefler`_
- Add nr of columns to explain output for projection (:pr:`1030`) `Patrick Hoefler`_
- Fix error in analyze for scalar (:pr:`1027`) `Patrick Hoefler`_
- Fix doc build error (:pr:`1026`) `Patrick Hoefler`_
- Add docs for usefule optimizer methods (:pr:`1025`) `Patrick Hoefler`_
- Rename uniuqe_partition_mapping property and add docs (:pr:`1022`) `Patrick Hoefler`_
- Fix read_parquet if directory is empty (:pr:`1023`) `Patrick Hoefler`_
- Fix assign after set index incorrect projections (:pr:`1020`) `Patrick Hoefler`_
- Use implicit knowledge about divisions for efficient grouping (:pr:`946`) `Florian Jetter`_
- Simplify dtype casting logic for shuffle (:pr:`1012`) `Patrick Hoefler`_
- Fix column projections in merge when suffixes are relevant (:pr:`1019`) `Patrick Hoefler`_

# v1.0.11

- Fix `unique` with numeric columns (:pr:`1017`) `Patrick Hoefler`_
- Fix projection for rename if projection isn't renamed (:pr:`1016`) `Patrick Hoefler`_
- Fix head for npartitions=-1 and optimizer step (:pr:`1014`) `Patrick Hoefler`_
- Deprecate to/from_dask_dataframe API (:pr:`1001`) `Richard (Rick) Zamora`_

# v1.0.10

- Make `setattr` work (:pr:`1011`) `Patrick Hoefler`_
- Adjust version number in changes `Patrick Hoefler`_

# v1.0.9

- Add support for named aggregations in `groupby(...).aggregate()` (:pr:`1009`) `Patrick Hoefler`_

# v1.0.7

- Fix meta calculation in `drop_duplicates` to preserve dtypes (:pr:`1007`) `Patrick Hoefler`_

# v1.0.6

- Fix pyarrow fs reads for list of directories (:pr:`1006`) `Patrick Hoefler`_
- Register json and orc APIs for "pandas" dispatch (:pr:`1004`) `Richard (Rick) Zamora`_
- Rename overloaded `to/from_dask_dataframe` API (:pr:`987`) `Richard (Rick) Zamora`_
- Fix zero division error when reading index from parquet (:pr:`1000`) `Patrick Hoefler`_
- Start building and publishing conda nightlies (:pr:`986`) `Charles Blackmon-Luca`_
- Set divisions with divisions already known (:pr:`997`) `Florian Jetter`_
- Nicer read_parquet prefix (:pr:`998`) `Florian Jetter`_
- Reduce coverage target a little bit (:pr:`999`) `Patrick Hoefler`_

# v1.0.5

- Ensure that repr doesn't raise if an operand is a pandas object (:pr:`996`) `Florian Jetter`_
- Allow passing of boolean index for column index in loc (:pr:`995`) `Florian Jetter`_
- Update pyproject.toml (:pr:`994`) `Florian Jetter`_
- Fix SettingWithCopyWarning in _merge.py (:pr:`990`) `Miles`_
- Ensure drop matches column names exactly (:pr:`992`) `Florian Jetter`_
- Support ``prefix`` argument in  ``from_delayed`` (:pr:`991`) `Richard (Rick) Zamora`_
- Visual ANALYZE (:pr:`889`) `Hendrik Makait`_

# v1.0.4

- Ensure wrapping an array when comparing to Series works if columns are empty (:pr:`984`) `Florian Jetter`_
- Remove keys() (:pr:`983`) `Patrick Hoefler`_
- Fix some reset_index optimization issues (:pr:`982`) `Patrick Hoefler`_
- Fix concat of series objects with column projection (:pr:`981`) `Patrick Hoefler`_
- Raise better error for repartition on divisions with unknown divisions (:pr:`980`) `Patrick Hoefler`_

# v1.0.3
- Support for dask==2023.3.1

# v1.0.2

- Revert enabling pandas cow (:pr:`974`) `Florian Jetter`_
- Fixup predicate pushdown for query 19 (:pr:`973`) `Patrick Hoefler`_
- Fixup set_index with one partition but more divisions by user (:pr:`972`) `Patrick Hoefler`_
- Implement custom reductions (:pr:`970`) `Patrick Hoefler`_
- Fix unique with shuffle and strings (:pr:`971`) `Patrick Hoefler`_
- Fixup filter pushdown through merges with ands and column reuse (:pr:`969`) `Patrick Hoefler`_

# v1.0.0

Initial stable release
