import math
import re
from itertools import chain

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from fsspec.core import get_fs_token_paths
from pyarrow.fs import FileSystem as PaFileSystem

from dask.base import tokenize
from dask.dataframe.io.dataset.core import DatasetEngine, ReadFunction
from dask.delayed import Delayed
from dask.utils import natural_sort_key


class ReadArrowFileFragment(ReadFunction):
    """Wrapper-function class for reading pyarrow fragments"""

    def __init__(self, schema, columns, ds_filters, index, dataset_options):
        self.schema = schema
        self.columns = columns
        self.ds_filters = ds_filters
        self.index = index
        self.dataset_options = dataset_options

    def _make_table(self, fragment):

        if isinstance(fragment, list):
            # fragment is actually a list of fragments
            tables = [self._make_table(frag) for frag in fragment]
            return pa.concat_tables(tables)
        elif isinstance(fragment, str):
            # fragment is a path name
            return ds.dataset(fragment, **self.dataset_options).to_table(
                filter=self.ds_filters,
                columns=self.columns,
            )
        elif isinstance(fragment, tuple):
            # fragment is a (path, row_groups) tuple
            path, row_groups = fragment
            old_frag = list(ds.dataset(path, **self.dataset_options).get_fragments())[0]
            return old_frag.format.make_fragment(
                old_frag.path,
                old_frag.filesystem,
                old_frag.partition_expression,
                row_groups=row_groups,
            ).to_table(
                filter=self.ds_filters,
                columns=self.columns,
            )
        else:
            # fragment is a pyarrow Fragment object
            return fragment.to_table(
                filter=self.ds_filters,
                columns=self.columns,
                schema=self.schema,
            )

    def __call__(self, fragment):
        # Convert fragment to pandas DataFrame
        df = self._make_table(fragment).to_pandas()

        if len(df.index.names) > 1:
            # Dask-DataFrame cannot handle multi-index
            df.reset_index(inplace=True)

        if self.index and self.index != df.index.name:
            if df.index.name is not None:
                df.reset_index(inplace=True)
            df.set_index(self.index, inplace=True)

        if self.columns is not None:
            return df[self.columns]
        return df


class ArrowDataset(DatasetEngine):
    """
    Dataset Engine backed by the ``pyarrow.dataset`` API

    Parameters
    ----------
    format : str or pyarrow.dataset.FileFormat
        File-format label or object (passed to the pyarrow datast API).
    partitioning : str or pyarrow object, default "hive"
        Dataset partitioning option to pass to the pyarrow datast API.
    filesystem : pyarrow.fs.FileSystem or fsspec.AbstractFileSystem, default None
        PyArrow or Fsspec-based filesystem object.
    aggregate_files : int, default 1
        How many adjacent files should be aggregated into the same
        output partition.
    aggregation_boundary : list[str], default None
        List of partitioned-column names that must have matching values in
        adjacent files for aggregation to occur. By default, any two files
        may be aggregated if ``aggregate_files>1``. Note that using this
        option will typically result in re-sorting of input paths. Therefore,
        the order of input path lists will not be preserved.
    sort_paths : bool, default True
        Whether to apply natural sorting order to dataset paths.
    storage_options : dict, default None
        Key/value pairs to be passed on to the file-system backend if
        ``filesystem`` is not specified.
    **dataset_options :
        Other key-value arguments to pass along to the ``pyarrow.dataset`` API.
    """

    def __init__(
        self,
        format=None,
        partitioning="hive",
        filesystem=None,
        aggregate_files=1,
        aggregation_boundary=None,
        sort_paths=True,
        storage_options=None,
        **dataset_options,
    ):
        self.format = format
        self.partitioning = partitioning
        self.filesystem = filesystem
        self.sort_paths = sort_paths
        self.aggregation_boundary = aggregation_boundary or []
        self.storage_options = storage_options or {}
        self.dataset_options = dataset_options
        self.aggregate_files = aggregate_files
        if not isinstance(aggregate_files, int):
            # NOTE: We may convert aggregate_files to a list internally,
            # but only integer input is supported from the user
            raise ValueError(
                f"aggregate_files must be an integer, got {aggregate_files}"
            )

    def get_dataset(self, path, columns, filters, mode="rb"):
        """Returns an engine-specific dataset object"""

        # TODO: Handle glob ordering? Apply sorting only for
        # glob or directory?

        # Check if source is a list of lists
        if isinstance(path, list) and path and isinstance(path[0], list):

            def _get_msg(option):
                return (
                    f"{option} argument not supported when file "
                    f"aggregation is already encoded in the source input. "
                    f"(i.e. When the `path` input is a list of lists)."
                )

            if self.aggregate_files > 1:
                _get_msg("aggregate_files")
            if self.aggregation_boundary:
                _get_msg("aggregation_boundary")
            # Define aggregate_files to be a list and flatten source
            self.aggregate_files = [len(paths) for paths in path]
            path = list(chain(*path))

        # Check if we already have a file-system object
        if self.filesystem is None:
            self.filesystem = get_fs_token_paths(
                path, mode=mode, storage_options=self.storage_options
            )[0]

        return ds.dataset(
            path,
            format=self.format,
            partitioning=self.partitioning,
            filesystem=self.filesystem,
            **self.dataset_options,
        )

    def create_meta(self, dataset, index, columns):
        """Returns an empty-DataFrame meta object"""

        # Start with simple schema -> pandas mapping
        meta = dataset.schema.empty_table().to_pandas()

        # Check if we have a multi-index
        if len(meta.index.names) > 1:
            # Dask-DataFrame cannot handle multi-index
            meta.reset_index(inplace=True)

        # Set index if necessary
        if index and index != meta.index.name:
            if meta.index.name is not None:
                meta.reset_index(inplace=True)
            meta.set_index(index, inplace=True)

        if columns is not None:
            return meta[columns]
        return meta

    def get_read_function(self, schema, columns, ds_filters, index):
        """Returns a function to convert a fragment to a DataFrame partition"""

        return ReadArrowFileFragment(
            schema,
            columns,
            ds_filters,
            index,
            dataset_options={
                "format": self.format,
                "partitioning": self.partitioning,
                "filesystem": self.filesystem,
                **self.dataset_options,
            },
        )

    def _aggregate_files(self, fragments):
        """Aggregate adjacent file fragments into the same output partition"""

        if not fragments:
            return []

        if isinstance(self.aggregate_files, list) and self.aggregate_files:
            # The file-aggregation plan was already encoded in the
            # source input, so we do not need to handle the
            # `aggregation_boundary` option
            cumsum = 0
            aggregated_fragments = []
            for i, agg_size in enumerate(self.aggregate_files):
                aggregated_fragments.append(fragments[cumsum : cumsum + agg_size])
                cumsum += agg_size
            return aggregated_fragments

        elif isinstance(self.aggregate_files, int) and self.aggregate_files > 1:
            # Aggregating files by integer "stride". However, we
            # also need to satisfy the `aggregation_boundary` option
            if self.aggregation_boundary:
                assert not isinstance(fragments[0], (str, tuple))

                def _make_str(frag):
                    expr = ""
                    for field in self.aggregation_boundary:
                        next_expr = re.findall(
                            field + r" == [^{\)}]*",
                            str(frag.partition_expression),
                        )
                        if next_expr:
                            expr += next_expr[0].split(" == ")[-1]

                    return expr

                exprs = [_make_str(frag) for frag in fragments]
            else:
                exprs = [True] * len(fragments)

            frag_df = pd.DataFrame(
                {
                    "fragments": range(len(fragments)),
                    "partition": exprs,
                }
            )
            frag_groups = frag_df.groupby("partition").agg(list)["fragments"]

            aggregated_fragments = []
            for group in frag_groups:
                for i in range(0, len(group), self.aggregate_files):
                    frag_indices = group[i : i + self.aggregate_files]
                    new_item = [fragments[ind] for ind in frag_indices]
                    if new_item:
                        aggregated_fragments.append(new_item)
            return aggregated_fragments

        return fragments

    def get_fragments(self, dataset, ds_filters, meta, index, calculate_divisions):
        """Returns fragments and divisions"""

        if calculate_divisions:
            raise ValueError(f"calculate_divisions=True not supported for {type(self)}")

        if ds_filters is not None or self.aggregation_boundary:
            # Filters are defined - Use real fragments
            fragments = list(dataset.get_fragments(ds_filters))
            # TODO: How to sort paths when .path is not available
            # on the fragment?
        else:
            # Just use list of files
            fragments = dataset.files
            if self.sort_paths:
                fragments = sorted(fragments, key=natural_sort_key)

        # Aggregate files if necessary
        fragments = self._aggregate_files(fragments)

        return fragments, (None,) * (len(fragments) + 1)

    def get_collection_mapping(
        self,
        source,
        columns=None,
        filters=None,
        index=None,
        calculate_divisions=False,
    ):
        """Returns required inputs for a DataFrame collection"""

        # Get dataset
        dataset = self.get_dataset(source, columns, filters)

        # Create meta
        meta = self.create_meta(dataset, index, columns)

        # Get filters
        ds_filters = None
        if filters is not None:
            ds_filters = pq._filters_to_expression(filters)

        # Get fragments and divisions
        fragments, divisions = self.get_fragments(
            dataset, ds_filters, meta, index, calculate_divisions
        )
        divisions = divisions or (None,) * (len(fragments) + 1)

        # Get IO function
        io_func = self.get_read_function(dataset.schema, columns, ds_filters, index)

        # Return all requirements for DataFrame-collection creation
        return fragments, divisions, meta, io_func


class ArrowParquetDataset(ArrowDataset):
    """
    Pyarrow dataset engine for Parquet data

    Parameters
    ----------
    format : str or pyarrow.dataset.FileFormat, default pyarrow.dataset.ParquetFileFormat()
        File-format label or object (passed to the pyarrow datast API).
    partitioning : str or pyarrow object, default "hive"
        Dataset partitioning option to pass to the pyarrow datast API.
    filesystem : pyarrow.fs.FileSystem or fsspec.AbstractFileSystem, default None
        PyArrow or Fsspec-based filesystem object.
    aggregate_files : int, default 1
        How many adjacent files should be aggregated into the same
        output partition.
    aggregation_boundary : list[str], default None
        List of partitioned-column names that must have matching values in
        adjacent files for aggregation to occur. By default, any two files
        may be aggregated if ``aggregate_files>1``.
    sort_paths : bool, default True
        Whether to apply natural sorting order to dataset paths.
    split_row_groups : bool or int, default False
        If True, then each output dataframe partition will correspond to a single
        parquet-file row-group. If False, each partition will correspond to a
        complete file.  If a positive integer value is given, each dataframe
        partition will correspond to that number of parquet row-groups (or fewer).
        This option cannot be combined with ``aggregate_files``, or be used when
        file aggregation is already encoded in the source input.
    max_partition_size : int, default None
        The desired maximum size of each output ``DataFrame`` partition in terms of
        total row count. If specified, row-groups will be aggregated into the same
        output partition until the cumulative row count reaches this value. Use
        ``aggregation_boundary`` to determine if row-groups from different partition
        directories should be agregated together. Note that this option will
        override ``split_row_groups`` and ``aggregate_files``, and is not supported
        when file aggregation is encoded in the ``path`` input.
    ignore_metadata_file : bool, default False
        Whether to ignore the global Parquet _metadata file (if one is present).
    storage_options : dict, default None
        Key/value pairs to be passed on to the file-system backend if
        ``filesystem`` is not specified.
    **dataset_options :
        Other key-value arguments to pass along to the ``pyarrow.dataset`` API.
    """

    def __init__(
        self,
        split_row_groups=False,
        max_partition_size=None,
        ignore_metadata_file=False,
        metadata_task_size=32,
        format=None,
        **dataset_options,
    ):
        super().__init__(format=format or ds.ParquetFileFormat(), **dataset_options)
        self.max_partition_size = max_partition_size
        self.split_row_groups = split_row_groups
        self.ignore_metadata_file = ignore_metadata_file
        self.using_global_metadata = False
        self.metadata_task_size = metadata_task_size
        if bool(self.split_row_groups) and int(self.aggregate_files) > 1:
            raise ValueError(
                "aggregate_files only supported when split_row_groups=False"
            )
        if self.max_partition_size:
            self.split_row_groups = True

    def get_dataset(self, path, columns, filters, mode="rb"):

        # Check if source is a list of lists
        if isinstance(path, list) and path and isinstance(path[0], list):

            def _get_msg(option):
                return (
                    f"{option} argument not supported when file "
                    f"aggregation is already encoded in the source input. "
                    f"(i.e. When the `path` input is a list of lists)."
                )

            if self.aggregate_files > 1:
                _get_msg("aggregate_files")
            if self.aggregation_boundary:
                _get_msg("aggregation_boundary")
            if bool(self.max_partition_size):
                _get_msg("max_partition_size")
            if bool(self.split_row_groups):
                _get_msg("split_row_groups")
            # Define aggregate_files to be a list and flatten source
            self.aggregate_files = [len(paths) for paths in path]
            path = list(chain(*path))

        # Check if we already have a file-system object
        if self.filesystem is None:
            self.filesystem, _, paths = get_fs_token_paths(
                path, mode=mode, storage_options=self.storage_options
            )
        else:
            paths = [path] if isinstance(path, str) else path

        # Check for _metadata file if path is a directory name
        if (
            not self.ignore_metadata_file
            and (len(paths) == 1 and isinstance(paths[0], str))
            and not paths[0].endswith("_metadata")
        ):
            meta_path = "/".join([paths[0], "_metadata"])
            if isinstance(self.filesystem, PaFileSystem):
                if self.filesystem.get_file_info(meta_path).is_file:
                    paths[0] = meta_path
            else:
                if self.filesystem.exists(meta_path):
                    paths[0] = meta_path

        # If path is a _metadata file, use ds.parquet_dataset
        ds_api = ds.dataset
        if (len(paths) == 1 and isinstance(paths[0], str)) and path.endswith("_metadata"):
            self.using_global_metadata = True
            ds_api = ds.parquet_dataset
        else:
            self.using_global_metadata = False

        if len(paths) == 1:
            paths = paths[0]

        return ds_api(
            paths,
            partitioning=self.partitioning,
            filesystem=self.filesystem,
            **self.dataset_options,
        )

    def _calculate_divisions(self, fragments, index):
        divisions = None

        def _stat(rg, kind):
            val = rg.statistics.get(index, {}).get(kind, None)
            if val is None:
                raise ValueError(f"Missing {kind} statistic")
            return val

        def _frag_stats(frag):
            frag_min = _stat(frag.row_groups[0], "min")
            frag_max = _stat(frag.row_groups[0], "max")
            for rg in frag.row_groups[1:]:
                frag_min = min(_stat(rg, "min"), frag_min)
                frag_max = max(_stat(rg, "max"), frag_max)
            return frag_min, frag_max

        try:
            mins, maxes = [], []
            if isinstance(fragments[0], list):
                aggregations = [len(f) for f in fragments]
                use_fragments = list(chain(*fragments))
            else:
                aggregations = [1] * len(fragments)
                use_fragments = fragments
            aggregations = np.cumsum(aggregations)

            divisions = []
            div = 0
            for f, frag in enumerate(use_fragments):
                frag_min, frag_max = _frag_stats(frag)
                mins.append(frag_min)
                maxes.append(frag_max)
                if (f + 1) == aggregations[div]:
                    if not divisions:
                        divisions = [np.min(mins)]
                    elif np.min(mins) < divisions[-1]:
                        return None
                    divisions.append(np.max(maxes))
                    mins, maxes = [], []
                    div += 1

        except ValueError:
            pass
        return divisions

    def _aggregate_files(self, fragments):
        """Aggregate adjacent file fragments into the same output partition"""

        if not fragments:
            return []

        if self.max_partition_size:

            # Aggregating files by integer "stride". However, we
            # also need to satisfy the `aggregation_boundary` option
            if self.aggregation_boundary:

                def _make_str(frag):
                    expr = ""
                    for field in self.aggregation_boundary:
                        next_expr = re.findall(
                            field + r" == [^{\)}]*",
                            str(frag.partition_expression),
                        )
                        if next_expr:
                            expr += next_expr[0].split(" == ")[-1]

                    return expr

                exprs = [_make_str(frag) for frag in fragments]
            else:
                exprs = [True] * len(fragments)

            frag_df = pd.DataFrame(
                {
                    "fragments": range(len(fragments)),
                    "partition": exprs,
                }
            )
            frag_groups = frag_df.groupby("partition").agg(list)["fragments"]

            def row_aggregation(fragments):
                # Aggregating partitions according to total row-count
                nfrags = len(fragments)
                sizes = np.cumsum(
                    [fragment.metadata.num_rows for fragment in fragments],
                    dtype="int64",
                )
                ideal = np.arange(
                    self.max_partition_size,
                    self.max_partition_size * nfrags,
                    self.max_partition_size,
                    dtype="int64",
                )
                offsets = sizes.searchsorted(ideal, side="left")
                offsets = [
                    ind
                    for i, ind in enumerate(offsets)
                    if i == 0 or ind != offsets[i - 1]
                ]
                _aggregated_fragments = []
                for i, offset in enumerate(offsets):
                    last = 0 if i == 0 else offsets[i - 1]
                    _aggregated_fragments.append(fragments[last:offset])
                return _aggregated_fragments

            aggregated_fragments = []
            for group in frag_groups:
                new_items = row_aggregation([fragments[ind] for ind in group])
                aggregated_fragments.extend(new_items)
            return aggregated_fragments

        return super()._aggregate_files(fragments)

    def get_fragments(self, dataset, ds_filters, meta, index, calculate_divisions):

        # Avoid index handling/divisions for default multi-index
        if index is None and len(meta.index.names) > 1:
            index = False

        # Check if we have a "default" index-column name
        if index is None and meta.index.name in dataset.schema.names:
            index = meta.index.name

        # Collect fragments
        if (
            ds_filters is not None
            or self.split_row_groups
            or calculate_divisions
            or self.aggregation_boundary
        ):
            # One or more options require real fragments
            file_fragments = list(dataset.get_fragments(ds_filters))
            if self.sort_paths:
                file_fragments = sorted(
                    file_fragments, key=lambda x: natural_sort_key(x.path)
                )
        else:
            # Just use list of files
            file_fragments = dataset.files
            if self.sort_paths:
                file_fragments = sorted(file_fragments, key=natural_sort_key)

        # Process the parquet metadata in parallel
        if (
            not self.using_global_metadata
            and (self.split_row_groups or calculate_divisions)
            and (len(file_fragments) >= self.metadata_task_size)
            and self.metadata_task_size  # metadata_task_size=0 to skip
        ):
            ntasks = math.ceil(len(file_fragments) / self.metadata_task_size)
            task_size = len(file_fragments) // ntasks

            dsk = {}
            meta_name = "parse-metadata-" + tokenize(
                file_fragments, ds_filters, index, task_size
            )
            for i in range(0, len(file_fragments), task_size):
                dsk[(meta_name, i)] = (
                    self.process_fragments,
                    file_fragments[i : i + task_size],
                    ds_filters,
                    index,
                    calculate_divisions,
                )
            dsk["final-" + meta_name] = (_finalize_plan, list(dsk.keys()))
            return Delayed("final-" + meta_name, dsk).compute()

        # Process on the client
        return self.process_fragments(
            file_fragments,
            ds_filters,
            index,
            calculate_divisions,
        )

    def process_fragments(self, file_fragments, ds_filters, index, calculate_divisions):
        """Returns processed fragments and divisions"""

        def _new_frag(old_frag, row_groups):
            return old_frag.format.make_fragment(
                old_frag.path,
                old_frag.filesystem,
                old_frag.partition_expression,
                row_groups=row_groups,
            )

        # Check if we are splitting the file by row-groups
        split_row_groups = int(self.split_row_groups)
        if split_row_groups:
            fragments = []
            path_fragments = []
            for file_frag in file_fragments:
                row_groups = [rg.id for rg in file_frag.row_groups]
                for i in range(0, len(row_groups), split_row_groups):
                    rgs = row_groups[i : i + split_row_groups]
                    frag = _new_frag(file_frag, rgs)
                    fragments.append(frag)
                    path_fragments.append((frag.path, rgs))
        else:
            # For Parquet, we can convert fragments back to paths
            # to avoid large graph sizes
            fragments = file_fragments
            if fragments and isinstance(fragments[0], str):
                path_fragments = fragments
            else:
                path_fragments = [f.path for f in fragments]

        # Aggregate files
        if fragments and (
            self.max_partition_size
            or (isinstance(self.aggregate_files, int) and self.aggregate_files > 1)
            or (isinstance(self.aggregate_files, list) and self.aggregate_files)
        ):

            def _get_path(frag):
                if isinstance(frag, str):
                    return frag
                elif isinstance(frag, tuple):
                    return _get_path(frag[0])
                elif isinstance(frag, list):
                    return [_get_path(f) for f in frag]
                else:
                    if self.split_row_groups:
                        return frag.path, [rg.id for rg in frag.row_groups]
                    return frag.path

            path_fragments = [_get_path(f) for f in self._aggregate_files(fragments)]

        # Calculate divisions
        divisions = None
        if index and calculate_divisions:
            divisions = self._calculate_divisions(fragments, index)

        return path_fragments, divisions or (None,) * (len(path_fragments) + 1)


def _finalize_plan(plan_list):
    """Finalize fragment/divisions for multiple metadata tasks"""
    fragments, divisions = plan_list[0]

    if None in divisions:
        divisions = None
    else:
        divisions = list(divisions)

    for frags, divs in plan_list[1:]:
        fragments += frags
        if divisions:
            if divisions[-1] is None or divs[0] is None or (divs[0] < divisions[-1]):
                divisions = None
            else:
                divisions += list(divs[1:])

    return fragments, divisions or (None,) * (len(fragments) + 1)
