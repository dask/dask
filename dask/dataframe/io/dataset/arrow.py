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

    def __call__(self, fragment):
        if isinstance(fragment, str):
            df = (
                ds.dataset(fragment, **self.dataset_options)
                .to_table(filter=self.filters, columns=self.columns)
                .to_pandas()
            )
        else:
            df = fragment.to_table(
                filter=self.filters, columns=self.columns
            ).to_pandas()

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
    def __init__(
        self,
        partitioning="hive",
        filesystem=None,
        **dataset_options,
    ):
        self.dataset_options = dataset_options
        self.partitioning = partitioning
        self.filesystem = filesystem

    def get_dataset(self, path, columns, filters, storage_options, mode="rb"):
        """Returns an engine-specific dataset object"""

        # Check if we already have a file-system object
        if self.filesystem is None:
            self.filesystem = get_fs_token_paths(
                path, mode=mode, storage_options=storage_options
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

    def get_fragments(self, dataset, filters, meta, index, calculate_divisions):
        ds_filters = None
        if filters is not None:
            ds_filters = pq._filters_to_expression(filters)
        fragments = list(dataset.get_fragments(ds_filters))

        if calculate_divisions:
            raise ValueError(f"calculate_divisions=True not supported for {type(self)}")
        return fragments, None

    def get_collection_mapping(
        self, path, columns, filters, index, calculate_divisions, storage_options
    ):

        # Get dataset
        dataset = self.get_dataset(path, columns, filters, storage_options)

        # Create meta
        meta = self.create_meta(dataset, index, columns)

        # Get fragments and divisions
        fragments, divisions = self.get_fragments(
            dataset, filters, meta, index, calculate_divisions
        )

        # Get IO function
        io_func = self.get_read_function(columns, filters, index)

        return io_func, fragments, meta, divisions or (None,) * (len(fragments) + 1)


class ArrowParquetDataset(ArrowDataset):
    def __init__(self, split_row_groups=False, **kwargs):
        super().__init__(format="parquet", **kwargs)
        self.split_row_groups = split_row_groups

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

    def get_fragments(self, dataset, filters, meta, index, calculate_divisions):

        if index is None and len(meta.index.names) > 1:
            index = False

        file_fragments, file_divisions = super().get_fragments(
            dataset, filters, meta, index, False
        )
        if self.split_row_groups:
            raise ValueError("split_row_groups is not yet supported")
            fragments = []
            ds_filters = None
            if filters is not None:
                ds_filters = pq._filters_to_expression(filters)
            for file_frag in file_fragments:
                for frag in file_frag.split_by_row_group(
                    ds_filters, schema=dataset.schema
                ):
                    fragments.append(frag)
            divisions = [None] * (len(fragments) + 1)
        else:
            fragments = file_fragments
            divisions = file_divisions

        # For Parquet, we can convert fragments back to paths
        # to avoid large graph sizes
        path_fragments = [f.path for f in fragments]

        # Check if we have an index column
        # to calculate statistics for
        if index is None and meta.index.name in dataset.schema.names:
            index = meta.index.name

        if index and calculate_divisions:
            divisions = self._caclulate_divisions(fragments, index) or divisions

        return path_fragments, divisions
