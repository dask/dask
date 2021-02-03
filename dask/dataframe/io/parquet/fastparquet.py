from distutils.version import LooseVersion
from collections import defaultdict

from collections import OrderedDict
import copy
import json
import pickle
import warnings

import tlz as toolz

import numpy as np
import pandas as pd

try:
    import fastparquet
    from fastparquet import ParquetFile
    from fastparquet.util import get_file_scheme
    from fastparquet.util import ex_from_sep, val_to_num, groupby_types
    from fastparquet.writer import partition_on_columns, make_part_file
except ImportError:
    pass

from .utils import (
    _parse_pandas_metadata,
    _normalize_index_columns,
    _analyze_paths,
    _flatten_filters,
    _row_groups_to_parts,
)
from ..utils import _meta_from_dtypes
from ...utils import UNKNOWN_CATEGORIES
from ...methods import concat


#########################
# Fastparquet interface #
#########################
from .utils import Engine


def _paths_to_cats(paths, file_scheme):
    """
    Extract categorical fields and labels from hive- or drill-style paths.
    FixMe: This has been pasted from https://github.com/dask/fastparquet/pull/471
    Use fastparquet.api.paths_to_cats from fastparquet>0.3.2 instead.

    Parameters
    ----------
    paths (Iterable[str]): file paths relative to root
    file_scheme (str):

    Returns
    -------
    cats (OrderedDict[str, List[Any]]): a dict of field names and their values
    """
    if file_scheme in ["simple", "flat", "other"]:
        cats = {}
        return cats

    cats = OrderedDict()
    raw_cats = OrderedDict()
    s = ex_from_sep("/")
    paths = toolz.unique(paths)
    if file_scheme == "hive":
        partitions = toolz.unique((k, v) for path in paths for k, v in s.findall(path))
        for key, val in partitions:
            cats.setdefault(key, set()).add(val_to_num(val))
            raw_cats.setdefault(key, set()).add(val)
    else:
        i_val = toolz.unique(
            (i, val) for path in paths for i, val in enumerate(path.split("/")[:-1])
        )
        for i, val in i_val:
            key = "dir%i" % i
            cats.setdefault(key, set()).add(val_to_num(val))
            raw_cats.setdefault(key, set()).add(val)

    for key, v in cats.items():
        # Check that no partition names map to the same value after transformation by val_to_num
        raw = raw_cats[key]
        if len(v) != len(raw):
            conflicts_by_value = OrderedDict()
            for raw_val in raw_cats[key]:
                conflicts_by_value.setdefault(val_to_num(raw_val), set()).add(raw_val)
            conflicts = [
                c for k in conflicts_by_value.values() if len(k) > 1 for c in k
            ]
            raise ValueError("Partition names map to the same value: %s" % conflicts)
        vals_by_type = groupby_types(v)

        # Check that all partition names map to the same type after transformation by val_to_num
        if len(vals_by_type) > 1:
            examples = [x[0] for x in vals_by_type.values()]
            warnings.warn(
                "Partition names coerce to values of different types, e.g. %s"
                % examples
            )

    cats = OrderedDict([(key, list(v)) for key, v in cats.items()])
    return cats


paths_to_cats = (
    _paths_to_cats  # FixMe: use fastparquet.api.paths_to_cats for fastparquet>0.3.2
)


def _determine_pf_parts(fs, paths, gather_statistics, **kwargs):
    """Determine how to access metadata and break read into ``parts``

    This logic is mostly to handle `gather_statistics=False` cases,
    because this also means we should avoid scanning every file in the
    dataset.  If _metadata is available, set `gather_statistics=True`
    (if `gather_statistics=None`).
    """
    parts = []
    if len(paths) > 1:
        base, fns = _analyze_paths(paths, fs)
        if gather_statistics is not False:
            # This scans all the files, allowing index/divisions
            # and filtering
            if "_metadata" not in fns:
                paths_use = paths
            else:
                paths_use = base + fs.sep + "_metadata"
            pf = ParquetFile(
                paths_use, open_with=fs.open, sep=fs.sep, **kwargs.get("file", {})
            )
        else:
            if "_metadata" in fns:
                # We have a _metadata file, lets use it
                pf = ParquetFile(
                    base + fs.sep + "_metadata",
                    open_with=fs.open,
                    sep=fs.sep,
                    **kwargs.get("file", {}),
                )
            else:
                # Rely on metadata for 0th file.
                # Will need to pass a list of paths to read_partition
                scheme = get_file_scheme(fns)
                pf = ParquetFile(paths[0], open_with=fs.open, **kwargs.get("file", {}))
                pf.file_scheme = scheme
                pf.cats = paths_to_cats(fns, scheme)
                parts = paths.copy()
    elif fs.isdir(paths[0]):
        # This is a directory, check for _metadata, then _common_metadata
        paths = fs.glob(paths[0] + fs.sep + "*")
        base, fns = _analyze_paths(paths, fs)
        if "_metadata" in fns:
            # Using _metadata file (best-case scenario)
            pf = ParquetFile(
                base + fs.sep + "_metadata",
                open_with=fs.open,
                sep=fs.sep,
                **kwargs.get("file", {}),
            )
            if gather_statistics is None:
                gather_statistics = True

        elif gather_statistics is not False:
            # Scan every file
            pf = ParquetFile(paths, open_with=fs.open, **kwargs.get("file", {}))
        else:
            # Use _common_metadata file if it is available.
            # Otherwise, just use 0th file
            if "_common_metadata" in fns:
                pf = ParquetFile(
                    base + fs.sep + "_common_metadata",
                    open_with=fs.open,
                    **kwargs.get("file", {}),
                )
                fns.remove("_common_metadata")
            else:
                pf = ParquetFile(paths[0], open_with=fs.open, **kwargs.get("file", {}))
            scheme = get_file_scheme(fns)
            pf.file_scheme = scheme
            pf.cats = paths_to_cats(fns, scheme)
            parts = [fs.sep.join([base, fn]) for fn in fns]
    else:
        # There is only one file to read
        base = None
        pf = ParquetFile(
            paths[0], open_with=fs.open, sep=fs.sep, **kwargs.get("file", {})
        )

    return parts, pf, gather_statistics, base


class FastParquetEngine(Engine):
    @classmethod
    def _generate_dd_meta(cls, pf, index, categories):
        columns = None
        if pf.fmd.key_value_metadata:
            pandas_md = [
                x.value for x in pf.fmd.key_value_metadata if x.key == "pandas"
            ]
        else:
            pandas_md = []

        if len(pandas_md) == 0:
            index_names = []
            column_names = pf.columns + list(pf.cats)
            storage_name_mapping = {k: k for k in column_names}
            column_index_names = [None]

        elif len(pandas_md) == 1:
            (
                index_names,
                column_names,
                storage_name_mapping,
                column_index_names,
            ) = _parse_pandas_metadata(json.loads(pandas_md[0]))
            #  auto-ranges should not be created by fastparquet
            column_names.extend(pf.cats)

        else:
            raise ValueError("File has multiple entries for 'pandas' metadata")

        if index is None and len(index_names) > 0:
            if len(index_names) == 1 and index_names[0] is not None:
                index = index_names[0]
            else:
                index = index_names

        # Normalize user inputs
        column_names, index_names = _normalize_index_columns(
            columns, column_names, index, index_names
        )

        all_columns = index_names + column_names

        categories_dict = None
        if isinstance(categories, dict):
            categories_dict = categories

        if categories is None:
            categories = pf.categories
        elif isinstance(categories, str):
            categories = [categories]
        else:
            categories = list(categories)

        # Check that categories are included in columns
        if categories and not set(categories).intersection(all_columns):
            raise ValueError(
                "categories not in available columns.\n"
                "categories: {} | columns: {}".format(categories, list(all_columns))
            )

        dtypes = pf._dtypes(categories)
        dtypes = {storage_name_mapping.get(k, k): v for k, v in dtypes.items()}

        index_cols = index or ()
        meta = _meta_from_dtypes(all_columns, dtypes, index_cols, column_index_names)
        if isinstance(index_cols, str):
            index_cols = [index_cols]

        # fastparquet doesn't handle multiindex
        if len(index_names) > 1:
            raise ValueError("Cannot read DataFrame with MultiIndex.")

        for cat in categories:
            if cat in meta:
                meta[cat] = pd.Series(
                    pd.Categorical([], categories=[UNKNOWN_CATEGORIES]),
                    index=meta.index,
                )

        for catcol in pf.cats:
            if catcol in meta.columns:
                meta[catcol] = meta[catcol].cat.set_categories(pf.cats[catcol])
            elif meta.index.name == catcol:
                meta.index = meta.index.set_categories(pf.cats[catcol])

        return meta, dtypes, index_cols, categories_dict, categories, index

    @classmethod
    def _update_metadata_options(
        cls,
        pf,
        parts,
        gather_statistics,
        split_row_groups,
        index_cols,
        filters,
    ):
        # Cannot gather_statistics if our `parts` is alreadey a list
        # of paths, or if we are building a multiindex (for now).
        # We also don't "need" to gather statistics if we don't
        # want to apply any filters or calculate divisions.
        if (
            isinstance(parts, list) and len(parts) and isinstance(parts[0], str)
        ) or len(index_cols) > 1:
            gather_statistics = False
        elif filters is None and len(index_cols) == 0:
            gather_statistics = False

        # Make sure gather_statistics allows filtering
        # (if filters are desired)
        if filters:
            # Filters may require us to gather statistics
            if gather_statistics is False and pf.info.get("partitions", None):
                warnings.warn(
                    "Filtering with gather_statistics=False. "
                    "Only partition columns will be filtered correctly."
                )
            elif gather_statistics is False:
                raise ValueError("Cannot apply filters with gather_statistics=False")
            elif not gather_statistics:
                gather_statistics = True

        # Determine which columns need statistics.
        flat_filters = _flatten_filters(filters)
        stat_col_indices = {}
        for i, name in enumerate(pf.columns):
            if name in index_cols or name in flat_filters:
                stat_col_indices[name] = i

        # If the user has not specified `gather_statistics`,
        # we will only do so if there are specific columns in
        # need of statistics.
        # NOTE: We cannot change `gather_statistics` from True
        # to False (even if `stat_col_indices` is empty), in
        # case a `chunksize` was specified, and the row-group
        # statistics are needed for part aggregation.
        if gather_statistics is None:
            gather_statistics = bool(stat_col_indices)
        if split_row_groups is None:
            split_row_groups = False

        return (
            gather_statistics,
            split_row_groups,
            stat_col_indices,
        )

    @classmethod
    def _organize_row_groups(
        cls,
        pf,
        split_row_groups,
        gather_statistics,
        stat_col_indices,
        filters,
        dtypes,
        base_path,
        paths,
    ):
        """Organize row-groups by file."""

        # Get partitioning metadata
        pqpartitions = pf.info.get("partitions", None)

        # Store types specified in pandas metadata
        pandas_type = {}
        if pf.row_groups and pf.pandas_metadata:
            for c in pf.pandas_metadata.get("columns", []):
                if "field_name" in c:
                    pandas_type[c["field_name"]] = c.get("pandas_type", None)

        # Get the number of row groups per file
        single_rg_parts = int(split_row_groups) == 1
        file_row_groups = defaultdict(list)
        file_row_group_stats = defaultdict(list)
        file_row_group_column_stats = defaultdict(list)
        cmax_last = {}
        for rg, row_group in enumerate(pf.row_groups):

            # We can filter partition columns here without dealing
            # with statistics
            if (
                pqpartitions
                and filters
                and fastparquet.api.filter_out_cats(row_group, filters)
            ):
                continue

            # NOTE: Here we assume that all column chunks are stored
            # in the same file. This is not strictly required by the
            # parquet spec.
            fpath = row_group.columns[0].file_path
            if fpath is None:
                if paths and pf.fn in paths:
                    # There doesn't need to be a file_path if the
                    # row group is in the same file as the metadata.
                    # Assume this is a single-file dataset.
                    fpath = pf.fn
                    base_path = base_path or ""
                else:
                    raise ValueError(
                        "Global metadata structure is missing a file_path string. "
                        "If the dataset includes a _metadata file, that file may "
                        "have one or more missing file_path fields."
                    )

            # Append a tuple to file_row_groups. This tuple will
            # be structured as: `(<local-row-group-id>, <global-row-group-id>)`
            if file_row_groups[fpath]:
                file_row_groups[fpath].append((file_row_groups[fpath][-1][0] + 1, rg))
            else:
                file_row_groups[fpath].append((0, rg))

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
                    column = row_group.columns[i]
                    if column.meta_data.statistics:
                        cmin = None
                        cmax = None
                        # TODO: Avoid use of `pf.statistics`
                        if pf.statistics["min"][name][0] is not None:
                            cmin = pf.statistics["min"][name][rg]
                            cmax = pf.statistics["max"][name][rg]
                        elif dtypes[name] == "object":
                            cmin = column.meta_data.statistics.min_value
                            cmax = column.meta_data.statistics.max_value
                            # Older versions may not have cmin/cmax_value
                            if cmin is None:
                                cmin = column.meta_data.statistics.min
                            if cmax is None:
                                cmax = column.meta_data.statistics.max
                            # Decode bytes as long as "bytes" is not the
                            # expected `pandas_type` for this column
                            if (
                                isinstance(cmin, (bytes, bytearray))
                                and pandas_type.get(name, None) != "bytes"
                            ):
                                cmin = cmin.decode("utf-8")
                                cmax = cmax.decode("utf-8")
                        if isinstance(cmin, np.datetime64):
                            tz = getattr(dtypes[name], "tz", None)
                            cmin = pd.Timestamp(cmin, tz=tz)
                            cmax = pd.Timestamp(cmax, tz=tz)
                        last = cmax_last.get(name, None)

                        if not filters:
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
                            s["columns"].append(
                                {
                                    "name": name,
                                    "min": cmin,
                                    "max": cmax,
                                }
                            )
                        else:
                            cstats += [cmin, cmax]
                        cmax_last[name] = cmax
                    else:
                        if not filters and column.meta_data.num_values > 0:
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
            base_path,
        )

    @classmethod
    def _get_thrift_row_groups(
        cls,
        pf,
        filename,
        row_groups,
    ):
        """Turn a set of row-groups into bytes-serialized form
        using thrift via pickle.
        """

        real_row_groups = []
        for rg, rg_global in row_groups:
            row_group = pf.row_groups[rg_global]
            row_group.statistics = None
            row_group.helper = None
            for c, col in enumerate(row_group.columns):
                if c:
                    col.file_path = None
                col.meta_data.key_value_metadata = None
                # NOTE: Fastparquet may need the null count in the
                # statistics, so we cannot just set statistics
                # to none.  Set attributes separately:
                if col.meta_data.statistics:
                    col.meta_data.statistics.distinct_count = None
                    col.meta_data.statistics.max = None
                    col.meta_data.statistics.min = None
                    col.meta_data.statistics.max_value = None
                    col.meta_data.statistics.min_value = None
                col.meta_data.encodings = None
                col.meta_data.total_uncompressed_size = None
                col.meta_data.encoding_stats = None
            real_row_groups.append(row_group)
        return pickle.dumps(real_row_groups)

    @classmethod
    def _make_part(
        cls,
        filename,
        rg_list,
        fs=None,
        pf=None,
        base_path=None,
        partitions=None,
    ):
        """Generate a partition-specific element of `parts`."""

        if partitions:
            real_row_groups = cls._get_thrift_row_groups(
                pf,
                filename,
                rg_list,
            )
            part = {"piece": (real_row_groups,)}
        else:
            # Get full path (empty strings should be ignored)
            full_path = fs.sep.join([p for p in [base_path, filename] if p != ""])
            row_groups = [rg[0] for rg in rg_list]  # Don't need global IDs
            part = {"piece": (full_path, row_groups)}

        return part

    @classmethod
    def _process_metadata(
        cls,
        pf,
        dtypes,
        split_row_groups,
        gather_statistics,
        stat_col_indices,
        filters,
        categories,
        base_path,
        paths,
        fs,
    ):

        # Organize row-groups by file
        (
            file_row_groups,
            file_row_group_stats,
            file_row_group_column_stats,
            gather_statistics,
            base_path,
        ) = cls._organize_row_groups(
            pf,
            split_row_groups,
            gather_statistics,
            stat_col_indices,
            filters,
            dtypes,
            base_path,
            paths,
        )

        # Convert organized row-groups to parts
        parts, stats = _row_groups_to_parts(
            gather_statistics,
            split_row_groups,
            file_row_groups,
            file_row_group_stats,
            file_row_group_column_stats,
            stat_col_indices,
            cls._make_part,
            make_part_kwargs={
                "fs": fs,
                "pf": pf,
                "base_path": base_path,
                "partitions": pf.info.get("partitions", None),
            },
        )

        return parts, stats

    @classmethod
    def _construct_parts(
        cls,
        fs,
        pf,
        paths,
        parts,
        dtypes,
        base_path,
        filters,
        index_cols,
        categories,
        split_row_groups,
        gather_statistics,
    ):

        # Check if `parts` is just a list of paths
        # (not splitting by row-group or collecting statistics)
        if isinstance(parts, list) and len(parts) and isinstance(parts[0], str):
            stats = []
            return [{"piece": (full_path, None)} for full_path in parts], stats

        # Update `gather_statistics` and `split_row_groups`
        # before constructing `parts`
        (
            gather_statistics,
            split_row_groups,
            stat_col_indices,
        ) = cls._update_metadata_options(
            pf,
            parts,
            gather_statistics,
            split_row_groups,
            index_cols,
            filters,
        )

        # Process row-groups and return `(parts, stats)`
        return cls._process_metadata(
            pf,
            dtypes,
            split_row_groups,
            gather_statistics,
            stat_col_indices,
            filters,
            categories,
            base_path,
            paths,
            fs,
        )

    @classmethod
    def read_metadata(
        cls,
        fs,
        paths,
        categories=None,
        index=None,
        gather_statistics=None,
        filters=None,
        split_row_groups=True,
        **kwargs,
    ):
        # Define the parquet-file (pf) object to use for metadata,
        # Also, initialize `parts`.  If `parts` is populated here,
        # then each part will correspond to a file.  Otherwise, each part will
        # correspond to a row group (populated below).
        parts, pf, gather_statistics, base_path = _determine_pf_parts(
            fs, paths, gather_statistics, **kwargs
        )

        # Process metadata to define `meta` and `index_cols`
        (
            meta,
            dtypes,
            index_cols,
            categories_dict,
            categories,
            index,
        ) = cls._generate_dd_meta(pf, index, categories)

        # Break `pf` into a list of `parts`
        parts, stats = cls._construct_parts(
            fs,
            pf,
            paths,
            parts,
            dtypes,
            base_path,
            filters,
            index_cols,
            categories,
            split_row_groups,
            gather_statistics,
        )

        # Cannot allow `None` in columns if the user has specified index=False
        if index is False and None in meta.columns:
            meta.drop(columns=[None], inplace=True)

        # Add `common_kwargs` to the first element of `parts`.
        # We can return as a separate element in the future, but
        # should avoid breaking the API for now.
        if len(parts):
            parts[0]["common_kwargs"] = {"categories": categories_dict or categories}

        if len(parts) and len(parts[0]["piece"]) == 1:

            # Strip all partition-dependent or unnecessary
            # data from the `ParquetFile` object
            pf.row_groups = None
            pf.fmd.row_groups = None
            pf._statistics = None
            parts[0]["common_kwargs"]["parquet_file"] = pf

        return (meta, stats, parts, index)

    @classmethod
    def read_partition(cls, fs, piece, columns, index, categories=(), **kwargs):

        null_index_name = False
        if isinstance(index, list):
            if index == [None]:
                # Handling a None-labeled index...
                # The pandas metadata told us to read in an index
                # labeled `None`. If this corresponds to a `RangeIndex`,
                # fastparquet will need use the pandas metadata to
                # construct the index. Otherwise, the index will correspond
                # to a column named "__index_level_0__".  We will need to
                # check the `ParquetFile` object for this column below.
                index = []
                null_index_name = True
            columns += index

        # Use global `parquet_file` object.  Need to reattach
        # the desired row_group
        parquet_file = kwargs.pop("parquet_file", None)

        if isinstance(piece, tuple):
            if isinstance(piece[0], str):
                # We have a path to read from
                assert parquet_file is None
                parquet_file = ParquetFile(
                    piece[0], open_with=fs.open, sep=fs.sep, **kwargs.get("file", {})
                )
                rg_indices = piece[1] or list(range(len(parquet_file.row_groups)))

                # `piece[1]` will contain row-group indices
                row_groups = [parquet_file.row_groups[rg] for rg in rg_indices]
            elif parquet_file:
                # `piece[1]` will contain actual row-group objects,
                # but they may be pickled
                row_groups = piece[0]
                if isinstance(row_groups, bytes):
                    row_groups = pickle.loads(row_groups)
                parquet_file.fmd.row_groups = row_groups
                # NOTE: May lose cats after `_set_attrs` call
                save_cats = parquet_file.cats
                parquet_file._set_attrs()
                parquet_file.cats = save_cats
            else:
                raise ValueError("Neither path nor ParquetFile detected!")

            if null_index_name:
                if "__index_level_0__" in parquet_file.columns:
                    # See "Handling a None-labeled index" comment above
                    index = ["__index_level_0__"]
                    columns += index

            parquet_file._dtypes = (
                lambda *args: parquet_file.dtypes
            )  # ugly patch, could be fixed

            # Read necessary row-groups and concatenate
            dfs = []
            for row_group in row_groups:
                dfs.append(
                    parquet_file.read_row_group_file(
                        row_group,
                        columns,
                        categories,
                        index=index,
                        **kwargs.get("read", {}),
                    )
                )
            return concat(dfs, axis=0) if len(dfs) > 1 else dfs[0]

        else:
            # `piece` is NOT a tuple
            raise ValueError(f"Expected tuple, got {type(piece)}")

    @classmethod
    def initialize_write(
        cls,
        df,
        fs,
        path,
        append=False,
        partition_on=None,
        ignore_divisions=False,
        division_info=None,
        schema=None,
        **kwargs,
    ):
        if append and division_info is None:
            ignore_divisions = True
        fs.mkdirs(path, exist_ok=True)
        object_encoding = kwargs.pop("object_encoding", "utf8")
        index_cols = kwargs.pop("index_cols", [])
        if object_encoding == "infer" or (
            isinstance(object_encoding, dict) and "infer" in object_encoding.values()
        ):
            raise ValueError(
                '"infer" not allowed as object encoding, '
                "because this required data in memory."
            )

        if append:
            try:
                # to append to a dataset without _metadata, need to load
                # _common_metadata or any data file here
                pf = fastparquet.api.ParquetFile(path, open_with=fs.open, sep=fs.sep)
            except (IOError, ValueError):
                # append for create
                append = False
        if append:
            if pf.file_scheme not in ["hive", "empty", "flat"]:
                raise ValueError(
                    "Requested file scheme is hive, but existing file scheme is not."
                )
            elif (set(pf.columns) != set(df.columns) - set(partition_on)) or (
                set(partition_on) != set(pf.cats)
            ):
                raise ValueError(
                    "Appended columns not the same.\n"
                    "Previous: {} | New: {}".format(pf.columns, list(df.columns))
                )
            elif (pd.Series(pf.dtypes).loc[pf.columns] != df[pf.columns].dtypes).any():
                raise ValueError(
                    "Appended dtypes differ.\n{}".format(
                        set(pf.dtypes.items()) ^ set(df.dtypes.iteritems())
                    )
                )
            else:
                df = df[pf.columns + partition_on]

            fmd = pf.fmd
            i_offset = fastparquet.writer.find_max_part(fmd.row_groups)
            if not ignore_divisions:
                if not set(index_cols).intersection([division_info["name"]]):
                    ignore_divisions = True
            if not ignore_divisions:
                minmax = fastparquet.api.sorted_partitioned_columns(pf)
                old_end = minmax[index_cols[0]]["max"][-1]
                divisions = division_info["divisions"]
                if divisions[0] < old_end:
                    raise ValueError(
                        "Appended divisions overlapping with previous ones."
                        "\n"
                        "Previous: {} | New: {}".format(old_end, divisions[0])
                    )
        else:
            fmd = fastparquet.writer.make_metadata(
                df._meta,
                object_encoding=object_encoding,
                index_cols=index_cols,
                ignore_columns=partition_on,
                **kwargs,
            )
            i_offset = 0

        schema = None  # ArrowEngine compatibility
        return (fmd, schema, i_offset)

    @classmethod
    def write_partition(
        cls,
        df,
        path,
        fs,
        filename,
        partition_on,
        return_metadata,
        fmd=None,
        compression=None,
        **kwargs,
    ):
        fmd = copy.copy(fmd)
        if not len(df):
            # Write nothing for empty partitions
            rgs = []
        elif partition_on:
            mkdirs = lambda x: fs.mkdirs(x, exist_ok=True)
            if LooseVersion(fastparquet.__version__) >= "0.1.4":
                rgs = partition_on_columns(
                    df, partition_on, path, filename, fmd, compression, fs.open, mkdirs
                )
            else:
                rgs = partition_on_columns(
                    df,
                    partition_on,
                    path,
                    filename,
                    fmd,
                    fs.sep,
                    compression,
                    fs.open,
                    mkdirs,
                )
        else:
            with fs.open(fs.sep.join([path, filename]), "wb") as fil:
                fmd.num_rows = len(df)
                rg = make_part_file(
                    fil, df, fmd.schema, compression=compression, fmd=fmd
                )
            for chunk in rg.columns:
                chunk.file_path = filename
            rgs = [rg]
        if return_metadata:
            return rgs
        else:
            return []

    @classmethod
    def write_metadata(cls, parts, fmd, fs, path, append=False, **kwargs):
        _meta = copy.copy(fmd)
        if parts:
            for rg in parts:
                if rg is not None:
                    if isinstance(rg, list):
                        for r in rg:
                            _meta.row_groups.append(r)
                    else:
                        _meta.row_groups.append(rg)
            fn = fs.sep.join([path, "_metadata"])
            fastparquet.writer.write_common_metadata(
                fn, _meta, open_with=fs.open, no_row_groups=False
            )

        # if appending, could skip this, but would need to check existence
        fn = fs.sep.join([path, "_common_metadata"])
        fastparquet.writer.write_common_metadata(fn, _meta, open_with=fs.open)
