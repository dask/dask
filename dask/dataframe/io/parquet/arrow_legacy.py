from collections import defaultdict

import pandas as pd
import pyarrow.parquet as pq

from .utils import _analyze_paths
from .arrow import ArrowDatasetEngine, _append_row_groups


#
#  PyArrow Legacy API [PyArrow<1.0.0]
#


def _get_dataset_object(paths, fs, filters, dataset_kwargs):
    """Generate a ParquetDataset object"""
    kwargs = dataset_kwargs.copy()
    if "validate_schema" not in kwargs:
        kwargs["validate_schema"] = False
    if len(paths) > 1:
        # This is a list of files
        base, fns = _analyze_paths(paths, fs)
        proxy_metadata = None
        if "_metadata" in fns:
            # We have a _metadata file. PyArrow cannot handle
            #  "_metadata" when `paths` is a list. So, we shuld
            # open "_metadata" separately.
            paths.remove(fs.sep.join([base, "_metadata"]))
            fns.remove("_metadata")
            with fs.open(fs.sep.join([base, "_metadata"]), mode="rb") as fil:
                proxy_metadata = pq.ParquetFile(fil).metadata
        # Create our dataset from the list of data files.
        # Note #1: that this will not parse all the files (yet)
        # Note #2: Cannot pass filters for legacy pyarrow API (see issue#6512).
        #          We can handle partitions + filtering for list input after
        #          adopting new pyarrow.dataset API.
        dataset = pq.ParquetDataset(paths, filesystem=fs, **kwargs)
        if proxy_metadata:
            dataset.metadata = proxy_metadata
    elif fs.isdir(paths[0]):
        # This is a directory.  We can let pyarrow do its thing.
        # Note: In the future, it may be best to avoid listing the
        #       directory if we can get away with checking for the
        #       existence of _metadata.  Listing may be much more
        #       expensive in storage systems like S3.
        allpaths = fs.glob(paths[0] + fs.sep + "*")
        base, fns = _analyze_paths(allpaths, fs)
        dataset = pq.ParquetDataset(paths[0], filesystem=fs, filters=filters, **kwargs)
    else:
        # This is a single file.  No danger in gathering statistics
        # and/or splitting row-groups without a "_metadata" file
        base = paths[0]
        fns = [None]
        dataset = pq.ParquetDataset(paths[0], filesystem=fs, **kwargs)

    return dataset, base, fns


class ArrowLegacyEngine(ArrowDatasetEngine):
    @classmethod
    def _gather_metadata(
        cls, paths, fs, split_row_groups, gather_statistics, filters, dataset_kwargs
    ):
        """Gather parquet metadata into a single data structure.

        Use _metadata or aggregate footer metadata into a single
        object.  Also, collect other information necessary for
        parquet-to-ddf mapping (e.g. schema, partition_info).
        """

        # Step 1: Create a ParquetDataset object
        dataset, base, fns = _get_dataset_object(paths, fs, filters, dataset_kwargs)
        if fns == [None]:
            # This is a single file. No danger in gathering statistics
            # and/or splitting row-groups without a "_metadata" file
            if gather_statistics is None:
                gather_statistics = True
            if split_row_groups is None:
                split_row_groups = True

        # Step 2: Construct necessary (parquet) partitioning information
        partition_info = {
            "partitions": None,
            "partition_keys": {},
            "partition_names": [],
        }
        # The `partition_info` dict summarizes information needed to handle
        # nested-directory (hive) partitioning.
        #
        #    - "partitions" : (ParquetPartitions) PyArrow-specific  object
        #          needed to read in each partition correctly
        #    - "partition_keys" : (dict) The keys and values correspond to
        #          file paths and partition values, respectively. The partition
        #          values (or partition "keys") will be represented as a list
        #          of tuples. E.g. `[("year", 2020), ("state", "CA")]`
        #    - "partition_names" : (list)  This is a list containing the names
        #          of partitioned columns.
        fn_partitioned = False
        if dataset.partitions is not None:
            fn_partitioned = True
            partition_info["partition_names"] = [
                n for n in dataset.partitions.partition_names if n is not None
            ]
            partition_info["partitions"] = dataset.partitions
            for piece in dataset.pieces:
                partition_info["partition_keys"][piece.path] = piece.partition_keys

        # Step 3: Construct a single `metadata` object. We can
        #         directly use dataset.metadata if it is available.
        #         Otherwise, if `gather_statistics` or `split_row_groups`,
        #         we need to gether the footer metadata manually
        metadata = None
        if dataset.metadata:
            # We have a _metadata file.
            # PyArrow already did the work for us
            schema = dataset.metadata.schema.to_arrow_schema()
            if gather_statistics is None:
                gather_statistics = True
            if split_row_groups is None:
                split_row_groups = True
            return (
                schema,
                dataset.metadata,
                base,
                partition_info,
                split_row_groups,
                gather_statistics,
            )
        else:
            # No _metadata file.
            # May need to collect footer metadata manually
            if dataset.schema is not None:
                schema = dataset.schema.to_arrow_schema()
            else:
                schema = None
            if gather_statistics is None:
                gather_statistics = False
            if split_row_groups is None:
                split_row_groups = False
            metadata = None
            if not (split_row_groups or gather_statistics):
                # Don't need to construct real metadata if
                # we are not gathering statistics or splitting
                # by row-group
                metadata = [p.path for p in dataset.pieces]
                if schema is None:
                    schema = dataset.pieces[0].get_metadata().schema.to_arrow_schema()
                return (
                    schema,
                    metadata,
                    base,
                    partition_info,
                    split_row_groups,
                    gather_statistics,
                )
            # We have not detected a _metadata file, and the user has specified
            # that they want to split by row-group and/or gather statistics.
            # This is the only case where we MUST scan all files to collect
            # metadata.
            for piece, fn in zip(dataset.pieces, fns):
                md = piece.get_metadata()
                if schema is None:
                    schema = md.schema.to_arrow_schema()
                if fn_partitioned:
                    md.set_file_path(piece.path.replace(base + fs.sep, ""))
                elif fn:
                    md.set_file_path(fn)
                if metadata:
                    _append_row_groups(metadata, md)
                else:
                    metadata = md
            return (
                schema,
                metadata,
                base,
                partition_info,
                split_row_groups,
                gather_statistics,
            )

    @classmethod
    def _construct_parts(
        cls,
        fs,
        metadata,
        schema,
        filters,
        index_cols,
        data_path,
        partition_info,
        categories,
        split_row_groups,
        gather_statistics,
        fragment_row_groups,
        use_legacy_dataset=True,
    ):
        return super()._construct_parts(
            fs,
            metadata,
            schema,
            filters,
            index_cols,
            data_path,
            partition_info,
            categories,
            split_row_groups,
            gather_statistics,
            fragment_row_groups,
            use_legacy_dataset=True,
        )

    @classmethod
    def _process_metadata(
        cls,
        metadata,
        single_rg_parts,
        gather_statistics,
        stat_col_indices,
        no_filters,
        fragment_row_groups,
    ):
        # Get the number of row groups per file
        file_row_groups = defaultdict(list)
        file_row_group_stats = defaultdict(list)
        file_row_group_column_stats = defaultdict(list)
        cmax_last = {}
        for rg in range(metadata.num_row_groups):
            row_group = metadata.row_group(rg)
            fpath = row_group.column(0).file_path
            if fpath is None:
                raise ValueError(
                    "Global metadata structure is missing a file_path string. "
                    "If the dataset includes a _metadata file, that file may "
                    "have one or more missing file_path fields."
                )
            if file_row_groups[fpath]:
                file_row_groups[fpath].append(file_row_groups[fpath][-1] + 1)
            else:
                file_row_groups[fpath].append(0)
            if gather_statistics:
                if single_rg_parts:
                    s = {
                        "file_path_0": fpath,
                        "num-rows": row_group.num_rows,
                        "total_byte_size": row_group.total_byte_size,
                        "columns": [],
                    }
                else:
                    s = {
                        "num-rows": row_group.num_rows,
                        "total_byte_size": row_group.total_byte_size,
                    }
                cstats = []
                for name, i in stat_col_indices.items():
                    column = row_group.column(i)
                    if column.statistics:
                        cmin = column.statistics.min
                        cmax = column.statistics.max
                        cnull = column.statistics.null_count
                        last = cmax_last.get(name, None)
                        if no_filters:
                            # Only think about bailing if we don't need
                            # stats for filtering
                            if cmin is None or (last and cmin < last):
                                # We are collecting statistics for divisions
                                # only (no filters) - Column isn't sorted, or
                                # we have an all-null partition, so lets bail.
                                #
                                # Note: This assumes ascending order.
                                #
                                gather_statistics = False
                                file_row_group_stats = {}
                                file_row_group_column_stats = {}
                                break

                        if single_rg_parts:
                            to_ts = column.statistics.logical_type.type == "TIMESTAMP"
                            s["columns"].append(
                                {
                                    "name": name,
                                    "min": cmin if not to_ts else pd.Timestamp(cmin),
                                    "max": cmax if not to_ts else pd.Timestamp(cmax),
                                    "null_count": cnull,
                                }
                            )
                        else:
                            cstats += [cmin, cmax, cnull]
                        cmax_last[name] = cmax
                    else:

                        if no_filters and column.num_values > 0:
                            # We are collecting statistics for divisions
                            # only (no filters) - Lets bail.
                            gather_statistics = False
                            file_row_group_stats = {}
                            file_row_group_column_stats = {}
                            break

                        if single_rg_parts:
                            s["columns"].append({"name": name})
                        else:
                            cstats += [None, None, None]
                if gather_statistics:
                    file_row_group_stats[fpath].append(s)
                    if not single_rg_parts:
                        file_row_group_column_stats[fpath].append(tuple(cstats))

        return (
            file_row_groups,
            file_row_group_stats,
            file_row_group_column_stats,
            gather_statistics,
        )
