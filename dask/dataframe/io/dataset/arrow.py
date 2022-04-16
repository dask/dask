from itertools import chain

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from fsspec.core import get_fs_token_paths
from pyarrow.fs import FileSystem as PaFileSystem

from dask.dataframe.io.dataset.core import DatasetEngine, ReadFunction
from dask.utils import natural_sort_key


class ReadArrowFileFragment(ReadFunction):
    """Wrapper-function class for reading pyarrow fragments"""

    def __init__(self, schema, columns, filters, index, dataset_options):
        self.schema = schema
        self.columns = columns
        ds_filters = None
        if filters is not None:
            ds_filters = pq._filters_to_expression(filters)
        self.filters = ds_filters
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
                filter=self.filters,
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
                filter=self.filters,
                columns=self.columns,
            )
        else:
            # fragment is a pyarrow Fragment object
            return fragment.to_table(
                filter=self.filters,
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
    aggregate_files : int, default None
        How many adjacent files should be aggregated into the same
        output partition.
    sort_paths : bool, default True
        Whether to apply natural sorting order to dataset paths.
    storage_options : dict, default None
        Key/value pairs to be passed on to the file-system backend, if any.
    **dataset_options :
        Key-value arguments to pass along to the ``pyarrow.dataset`` API.
    """

    def __init__(
        self,
        format=None,
        partitioning="hive",
        filesystem=None,
        aggregate_files=False,
        sort_paths=True,
        storage_options=None,
        **dataset_options,
    ):
        self.format = format
        self.partitioning = partitioning
        self.filesystem = filesystem
        self.aggregate_files = aggregate_files
        self.sort_paths = sort_paths
        self.storage_options = storage_options or {}
        self.dataset_options = dataset_options

    def get_dataset(self, source, columns, filters, mode="rb"):
        """Returns an engine-specific dataset object"""

        # TODO: Handle glob ordering? Apply sorting only for
        # glob or directory?

        # Convert source to path
        path = source.path
        if source.partition_base_dir:
            self.dataset_options["partition_base_dir"] = source.partition_base_dir

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

    def get_read_function(self, schema, columns, filters, index):
        """Returns a function to convert a fragment to a DataFrame partition"""

        return ReadArrowFileFragment(
            schema,
            columns,
            filters,
            index,
            dataset_options={
                "format": self.format,
                "partitioning": self.partitioning,
                "filesystem": self.filesystem,
                **self.dataset_options,
            },
        )

    def _aggregate_files(self, fragments):
        aggregate_files = int(self.aggregate_files)
        if aggregate_files > 1 and fragments:
            # Assume we can only aggregate files within the same
            # directory. Custom Engines will be required for
            # more-complex logic here (this is just basic aggregation)
            if isinstance(fragments[0], str):
                exprs = ["/".join(frag.split("/")[:-1]) for frag in fragments]
            elif isinstance(fragments[0], tuple):
                exprs = ["/".join(frag[0].split("/")[:-1]) for frag in fragments]
            else:
                exprs = [str(frag.partition_expression) for frag in fragments]
            frag_df = pd.DataFrame(
                {
                    "fragments": range(len(fragments)),
                    "partition": exprs,
                }
            )
            frag_groups = frag_df.groupby("partition").agg(list)["fragments"]

            aggregated_fragments = []
            for group in frag_groups:
                for i in range(0, len(group), aggregate_files):
                    frag_indices = group[i : i + aggregate_files]
                    new_item = [fragments[ind] for ind in frag_indices]
                    if new_item:
                        aggregated_fragments.append(new_item)
            return aggregated_fragments

        return fragments

    def get_fragments(self, dataset, filters, meta, index):
        """Returns dataset fragments and divisions"""

        ds_filters = None
        if filters is not None:
            # Filters are defined - Use real fragments
            ds_filters = pq._filters_to_expression(filters)
            fragments = list(dataset.get_fragments(ds_filters))
            # TODO: How to sort paths when .path is not available
            # on the fragment?
        else:
            # Just use list of files
            fragments = dataset.files
            if self.sort_paths:
                fragments = sorted(fragments, key=natural_sort_key)

        # Check if we are aggregating files
        fragments = self._aggregate_files(fragments)

        return fragments, (None,) * (len(fragments) + 1)

    def get_collection_mapping(
        self,
        source,
        columns=None,
        filters=None,
        index=None,
        full_return=True,
    ):

        # Get dataset
        dataset = self.get_dataset(source, columns, filters)

        # Create meta
        meta = self.create_meta(dataset, index, columns)

        # Get fragments and divisions
        fragments, divisions = self.get_fragments(dataset, filters, meta, index)
        divisions = divisions or (None,) * (len(fragments) + 1)

        if full_return:
            # Get IO function
            io_func = self.get_read_function(dataset.schema, columns, filters, index)
            return fragments, divisions, meta, io_func
        else:
            return fragments, divisions


class ArrowParquetDataset(ArrowDataset):
    """
    Pyarrow dataset engine for Parquet data

    Parameters
    ----------
    split_row_groups : bool or int, default False
        If True, then each output dataframe partition will correspond to a single
        parquet-file row-group. If False, each partition will correspond to a
        complete file.  If a positive integer value is given, each dataframe
        partition will correspond to that number of parquet row-groups (or fewer).
        Cannot be combined with ``aggregate_files``.
    calculate_divisions : bool, default False
        Whether parquet metadata should be used to calculate output divisions.
        This argument will be ignored if there is no active index column.
    ignore_metadata_file : bool, default False
        Whether to ignore the global _metadata file (if one is present).
    """

    def __init__(
        self,
        split_row_groups=False,
        calculate_divisions=False,
        ignore_metadata_file=False,
        format=None,
        **dataset_options,
    ):
        super().__init__(format=format or ds.ParquetFileFormat(), **dataset_options)
        self.split_row_groups = split_row_groups
        self.calculate_divisions = calculate_divisions
        self.ignore_metadata_file = ignore_metadata_file
        if bool(self.split_row_groups) and bool(self.aggregate_files):
            raise ValueError(
                "aggregate_files only supported when split_row_groups=False"
            )

    def get_dataset(self, source, columns, filters, mode="rb"):

        # Convert source to path
        path = source.path
        if source.partition_base_dir:
            self.dataset_options["partition_base_dir"] = source.partition_base_dir

        # Check if we already have a file-system object
        if self.filesystem is None:
            self.filesystem = get_fs_token_paths(
                path, mode=mode, storage_options=self.storage_options
            )[0]

        # Check for _metadata file if path is a directory name
        if (
            not self.ignore_metadata_file
            and isinstance(path, str)
            and not path.endswith("_metadata")
        ):
            meta_path = "/".join([path, "_metadata"])
            if isinstance(self.filesystem, PaFileSystem):
                if self.filesystem.get_file_info(meta_path).is_file:
                    path = meta_path
            else:
                if self.filesystem.exists(meta_path):
                    path = meta_path

        # If path is a _metadata file, use ds.parquet_dataset
        ds_api = ds.dataset
        if isinstance(path, str) and path.endswith("_metadata"):
            ds_api = ds.parquet_dataset

        return ds_api(
            path,
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

    def get_fragments(self, dataset, filters, meta, index):

        # Avoid index handling/divisions for default multi-index
        if index is None and len(meta.index.names) > 1:
            index = False

        # Check if we have a "default" index-column name
        if index is None and meta.index.name in dataset.schema.names:
            index = meta.index.name

        # Collect fragments
        ds_filters = None
        if filters is not None:
            ds_filters = pq._filters_to_expression(filters)
        if ds_filters is not None or self.split_row_groups or self.calculate_divisions:
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
            ds_filters = None
            if filters is not None:
                ds_filters = pq._filters_to_expression(filters)
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
        aggregate_files = int(self.aggregate_files)
        if aggregate_files > 1 and fragments:
            fragments = self._aggregate_files(fragments)
            if isinstance(fragments[0], str):
                path_fragments = fragments
            elif isinstance(fragments[0], list):
                if fragments[0] and not isinstance(fragments[0][0], str):
                    path_fragments = [[f.path for f in frag] for frag in fragments]
                else:
                    path_fragments = fragments
            else:
                path_fragments = [f.path for f in fragments]

        # Calculate divisions
        divisions = None
        if index and self.calculate_divisions:
            divisions = self._calculate_divisions(fragments, index)

        return path_fragments, divisions or (None,) * (len(path_fragments) + 1)
