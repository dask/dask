from distutils.version import LooseVersion

from collections import OrderedDict
import copy
import json
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

from .utils import _parse_pandas_metadata, _normalize_index_columns, _analyze_paths
from ..utils import _meta_from_dtypes
from ...utils import UNKNOWN_CATEGORIES

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

    The `fast_metadata` output specifies that ParquetFile metadata parsing
    is fast enough for each worker to perform during `read_partition`. The
    value will be set to True if: (1) The path is a directory containing
    _metadta, (2) the path is a list of files containing _metadata, (3)
    there is only one file to read, or (4) `gather_statistics` is False.
    In other cases, the ParquetFile object will need to be stored in the
    task graph, because metadata parsing is too expensive.
    """
    parts = []
    fast_metadata = True
    if len(paths) > 1:
        base, fns = _analyze_paths(paths, fs)
        if gather_statistics is not False:
            # This scans all the files, allowing index/divisions
            # and filtering
            if "_metadata" not in fns:
                paths_use = paths
                fast_metadata = False
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
                    **kwargs.get("file", {})
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
                **kwargs.get("file", {})
            )
            if gather_statistics is None:
                gather_statistics = True

        elif gather_statistics is not False:
            # Scan every file
            pf = ParquetFile(paths, open_with=fs.open, **kwargs.get("file", {}))
            fast_metadata = False
        else:
            # Use _common_metadata file if it is available.
            # Otherwise, just use 0th file
            if "_common_metadata" in fns:
                pf = ParquetFile(
                    base + fs.sep + "_common_metadata",
                    open_with=fs.open,
                    **kwargs.get("file", {})
                )
            else:
                pf = ParquetFile(paths[0], open_with=fs.open, **kwargs.get("file", {}))
            scheme = get_file_scheme(fns)
            pf.file_scheme = scheme
            pf.cats = paths_to_cats(fns, scheme)
            parts = paths.copy()
    else:
        # There is only one file to read
        base = None
        pf = ParquetFile(
            paths[0], open_with=fs.open, sep=fs.sep, **kwargs.get("file", {})
        )

    return parts, pf, gather_statistics, fast_metadata, base


class FastParquetEngine(Engine):
    @classmethod
    def read_metadata(
        cls,
        fs,
        paths,
        categories=None,
        index=None,
        gather_statistics=None,
        filters=None,
        **kwargs
    ):
        # Define the parquet-file (pf) object to use for metadata,
        # Also, initialize `parts`.  If `parts` is populated here,
        # then each part will correspond to a file.  Otherwise, each part will
        # correspond to a row group (populated below).
        parts, pf, gather_statistics, fast_metadata, base_path = _determine_pf_parts(
            fs, paths, gather_statistics, **kwargs
        )

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
        if gather_statistics and pf.row_groups:
            stats = []
            if filters is None:
                filters = []

            skip_cols = set()  # Columns with min/max = None detected
            # make statistics conform in layout
            for (i, row_group) in enumerate(pf.row_groups):
                s = {"num-rows": row_group.num_rows, "columns": []}
                for i_col, col in enumerate(pf.columns):
                    if col not in skip_cols:
                        d = {"name": col}
                        cs_min = None
                        cs_max = None
                        if pf.statistics["min"][col][0] is not None:
                            cs_min = pf.statistics["min"][col][i]
                            cs_max = pf.statistics["max"][col][i]
                        elif (
                            dtypes[col] == "object"
                            and row_group.columns[i_col].meta_data.statistics
                        ):
                            cs_min = row_group.columns[
                                i_col
                            ].meta_data.statistics.min_value
                            cs_max = row_group.columns[
                                i_col
                            ].meta_data.statistics.max_value
                            if isinstance(cs_min, (bytes, bytearray)):
                                cs_min = cs_min.decode("utf-8")
                                cs_max = cs_max.decode("utf-8")
                        if None in [cs_min, cs_max] and i == 0:
                            skip_cols.add(col)
                            continue
                        if isinstance(cs_min, np.datetime64):
                            cs_min = pd.Timestamp(cs_min)
                            cs_max = pd.Timestamp(cs_max)
                        d.update(
                            {
                                "min": cs_min,
                                "max": cs_max,
                                "null_count": pf.statistics["null_count"][col][i],
                            }
                        )
                        s["columns"].append(d)
                # Need this to filter out partitioned-on categorical columns
                s["filter"] = fastparquet.api.filter_out_cats(row_group, filters)
                s["total_byte_size"] = row_group.total_byte_size
                s["file_path_0"] = row_group.columns[0].file_path  # 0th column only
                stats.append(s)

        else:
            stats = None

        pf._dtypes = lambda *args: pf.dtypes  # ugly patch, could be fixed
        pf.fmd.row_groups = None

        # Constructing "piece" and "pf" for each dask partition. We will
        # need to consider the following output scenarios:
        #
        #  1) Each "piece" is a file path, and "pf" is `None`
        #      - `gather_statistics==False` and no "_metadata" available
        #  2) Each "piece" is a row-group index, and "pf" is ParquetFile object
        #      - We have parquet partitions and no "_metadata" available
        #  3) Each "piece" is a row-group index, and "pf" is a `tuple`
        #      - The value of the 0th tuple element depends on the following:
        #      A) Dataset is partitioned and "_metadata" exists
        #          - 0th tuple element will be the path to "_metadata"
        #      B) Dataset is not partitioned
        #          - 0th tuple element will be the path to the data
        #      C) Dataset is partitioned and "_metadata" does not exist
        #          - 0th tuple element will be the original `paths` argument
        #            (We will let the workers use `_determine_pf_parts`)

        # Create `parts`
        # This is a list of row-group-descriptor dicts, or file-paths
        # if we have a list of files and gather_statistics=False
        base_path = (base_path or "") + fs.sep
        if parts:
            # Case (1)
            # `parts` is just a list of path names.
            pqpartitions = None
            pf_deps = None
            partsin = parts
        else:
            # Populate `partsin` with dataset row-groups
            partsin = pf.row_groups
            pqpartitions = pf.info.get("partitions", None)
            if pqpartitions and not fast_metadata:
                # Case (2)
                # We have parquet partitions, and do not have
                # a "_metadata" file for the worker to read.
                # Therefore, we need to pass the pf object in
                # the task graph
                pf_deps = pf
            else:
                # Case (3)
                # We don't need to pass a pf object in the task graph.
                # Instead, we can try to pass the path for each part.
                pf_deps = "tuple"

        parts = []
        i_path = 0
        path_last = None

        # Loop over DataFrame partitions.
        # Each `part` will be a row-group or path ()
        for i, part in enumerate(partsin):
            if pqpartitions and fast_metadata:
                # Case (3A)
                # We can pass a "_metadata" path
                file_path = base_path + "_metadata"
                i_path = i
            elif (
                pf_deps
                and isinstance(part.columns[0].file_path, str)
                and not pqpartitions
            ):
                # Case (3B)
                # We can pass a specific file/part path
                path_curr = part.columns[0].file_path
                if path_last and path_curr == path_last:
                    i_path += 1
                else:
                    i_path = 0
                    path_last = path_curr
                file_path = base_path + path_curr
            else:
                # Case (3C)
                # We cannot pass a specific file/part path
                file_path = paths
                i_path = i

            # Strip down pf object
            if pf_deps and pf_deps != "tuple":
                # Case (2)
                for col in part.columns:
                    col.meta_data.statistics = None
                    col.meta_data.encoding_stats = None

            # Final definition of "piece" and "pf" for this output partition
            piece = i_path if pf_deps else part
            pf_piece = (file_path, gather_statistics) if pf_deps == "tuple" else pf_deps
            part_item = {
                "piece": piece,
                "kwargs": {"pf": pf_piece, "categories": categories_dict or categories},
            }
            parts.append(part_item)

        # Cannot allow `None` in columns if the user has specified index=False
        if index is False and None in meta.columns:
            meta.drop(columns=[None], inplace=True)

        return (meta, stats, parts, index)

    @classmethod
    def read_partition(
        cls, fs, piece, columns, index, categories=(), pf=None, **kwargs
    ):

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

        if pf is None:
            base, fns = _analyze_paths([piece], fs)
            scheme = get_file_scheme(fns)
            pf = ParquetFile(piece, open_with=fs.open)
            relpath = piece.replace(base, "").lstrip("/")
            for rg in pf.row_groups:
                for ch in rg.columns:
                    ch.file_path = relpath
            pf.file_scheme = scheme
            pf.cats = paths_to_cats(fns, scheme)
            pf.fn = base
            if null_index_name and "__index_level_0__" in pf.columns:
                # See "Handling a None-labeled index" comment above
                index = ["__index_level_0__"]
                columns += index
            return pf.to_pandas(columns, categories, index=index)
        else:
            if isinstance(pf, tuple):
                if isinstance(pf[0], list):
                    pf = _determine_pf_parts(fs, pf[0], pf[1], **kwargs)[1]
                else:
                    pf = ParquetFile(
                        pf[0], open_with=fs.open, sep=fs.sep, **kwargs.get("file", {})
                    )
                pf._dtypes = lambda *args: pf.dtypes  # ugly patch, could be fixed
                pf.fmd.row_groups = None
            rg_piece = pf.row_groups[piece]
            if null_index_name:
                if "__index_level_0__" in pf.columns:
                    # See "Handling a None-labeled index" comment above
                    index = ["__index_level_0__"]
                    columns += index
                    pf.fmd.key_value_metadata = None
            else:
                pf.fmd.key_value_metadata = None
            return pf.read_row_group_file(
                rg_piece, columns, categories, index=index, **kwargs.get("read", {})
            )

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
        **kwargs
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
                **kwargs
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
        **kwargs
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
