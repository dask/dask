import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from fsspec.core import get_fs_token_paths

from dask.dataframe.io.dataset.core import DatasetEngine, ReadFunction


class ReadArrowFileFragment(ReadFunction):
    """Wrapper-function class for reading pyarrow fragments"""

    def __init__(self, columns, filters, index, dataset_options):
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
                filter=self.filters, columns=self.columns
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
            ).to_table(filter=self.filters, columns=self.columns)
        else:
            # fragment is a pyarrow Fragment object
            return fragment.to_table(filter=self.filters, columns=self.columns)

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
    calculate_divisions : dict, default False
        Whether file metadata should be used to calculate output divisions.
        This option is not currently supported for the default engine.
    storage_options : dict, default None
        Key/value pairs to be passed on to the file-system backend, if any.
    **dataset_options :
        Key-value arguments to pass along to the ``pyarrow.dataset`` API.
    """

    def __init__(
        self,
        partitioning="hive",
        filesystem=None,
        aggregate_files=False,
        storage_options=None,
        **dataset_options,
    ):
        self.partitioning = partitioning
        self.filesystem = filesystem
        self.aggregate_files = aggregate_files
        self.storage_options = storage_options or {}
        self.dataset_options = dataset_options

    def get_dataset(self, path, columns, filters, mode="rb"):
        """Returns an engine-specific dataset object"""

        # Check if we already have a file-system object
        if self.filesystem is None:
            self.filesystem = get_fs_token_paths(
                path, mode=mode, storage_options=self.storage_options
            )[0]

        return ds.dataset(
            path,
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

    def get_read_function(self, columns, filters, index):
        """Returns a function to convert a fragment to a DataFrame partition"""

        return ReadArrowFileFragment(
            columns,
            filters,
            index,
            dataset_options={
                "partitioning": self.partitioning,
                "filesystem": self.filesystem,
                **self.dataset_options,
            },
        )

    def get_fragments(self, dataset, filters, meta, index):
        """Returns dataset fragments and divisions"""

        ds_filters = None
        if filters is not None:
            # Filters are defined - Use real fragments
            ds_filters = pq._filters_to_expression(filters)
            fragments = list(dataset.get_fragments(ds_filters))
        else:
            # Just use list of files
            fragments = dataset.files

        # Check if we are aggregating files
        aggregate_files = int(self.aggregate_files)
        if aggregate_files:
            aggregated_fragments = []
            for i in range(0, len(fragments), aggregate_files):
                aggregated_fragments.append(fragments[i : i + aggregate_files])
            fragments = aggregated_fragments

        return fragments, None

    def get_collection_mapping(
        self,
        path,
        columns=None,
        filters=None,
        index=None,
    ):

        # Get dataset
        dataset = self.get_dataset(path, columns, filters)

        # Create meta
        meta = self.create_meta(dataset, index, columns)

        # Get fragments and divisions
        fragments, divisions = self.get_fragments(dataset, filters, meta, index)

        # Get IO function
        io_func = self.get_read_function(columns, filters, index)

        return io_func, fragments, meta, divisions or (None,) * (len(fragments) + 1)


class ArrowParquetDataset(ArrowDataset):
    def __init__(self, split_row_groups=False, calculate_divisions=False, **kwargs):
        super().__init__(format="parquet", **kwargs)
        self.split_row_groups = split_row_groups
        self.calculate_divisions = calculate_divisions
        if bool(self.split_row_groups) and bool(self.aggregate_files):
            raise ValueError(
                "aggregate_files only supported when split_row_groups=False"
            )

    def _caclulate_divisions(self, fragments, index):
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
            frag_min, frag_max = _frag_stats(fragments[0])
            mins.append(frag_min)
            maxes.append(frag_max)
            for frag in fragments[1:]:
                frag_min, frag_max = _frag_stats(frag)
                if frag_min < maxes[-1]:
                    return divisions
                mins.append(frag_min)
                maxes.append(frag_max)
            divisions = (mins[0],) + tuple(maxes)
        except ValueError:
            pass
        return divisions

    def get_fragments(self, dataset, filters, meta, index):

        # Avoid index handling/divisions for default multi-index
        if index is None and len(meta.index.names) > 1:
            index = False

        ds_filters = None
        if filters is not None:
            ds_filters = pq._filters_to_expression(filters)
        if ds_filters is not None or self.split_row_groups or self.calculate_divisions:
            # One or more options require real fragments
            file_fragments = list(dataset.get_fragments(ds_filters))
        else:
            # Just use list of files
            file_fragments = dataset.files
        file_divisions = None

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
            divisions = [None] * (len(fragments) + 1)
        else:
            # For Parquet, we can convert fragments back to paths
            # to avoid large graph sizes
            fragments = file_fragments
            divisions = file_divisions
            path_fragments = [f.path for f in fragments]

        # Check if we have an index column
        # to calculate statistics for
        if index is None and meta.index.name in dataset.schema.names:
            index = meta.index.name

        # Calculate divisions if necessary
        if index and self.calculate_divisions:
            divisions = self._caclulate_divisions(fragments, index) or divisions
        divisions = divisions or (None,) * (len(path_fragments) + 1)

        # Check if we are aggregating files
        aggregate_files = int(self.aggregate_files)
        if aggregate_files:
            aggregated_fragments = []
            aggregated_divisions = []
            for i in range(0, len(path_fragments), aggregate_files):
                aggregated_fragments.append(path_fragments[i : i + aggregate_files])
                aggregated_divisions.append(divisions[i])
            aggregated_divisions.append(divisions[-1])
            return aggregated_fragments, aggregated_divisions

        return path_fragments, divisions
