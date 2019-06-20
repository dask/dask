from collections import OrderedDict
import copy
from distutils.version import LooseVersion
import json
import os
import warnings

import numpy as np
import pandas as pd

from ....compatibility import string_types
from ....base import tokenize
from ....delayed import delayed
from ...core import new_dd_object

import fastparquet
from fastparquet.util import check_column_names
from fastparquet import ParquetFile
from fastparquet.util import analyse_paths, get_file_scheme, join_path
from fastparquet.util import ex_from_sep, val_to_num, groupby_types
from fastparquet.api import _pre_allocate
from fastparquet.core import read_row_group_file
from fastparquet.writer import partition_on_columns, make_part_file

from .utils import _parse_pandas_metadata, _normalize_index_columns
from ..utils import _meta_from_dtypes
from ...utils import UNKNOWN_CATEGORIES

#########################
# Fastparquet interface #
#########################
from .utils import Engine


def _paths_to_cats(paths, scheme):
    """Extract out fields and labels from directory names"""
    # can be factored out in fastparquet
    cats = OrderedDict()
    raw_cats = OrderedDict()

    for path in paths:
        s = ex_from_sep("/")
        if scheme == "hive":
            partitions = s.findall(path)
            for (key, val) in partitions:
                cats.setdefault(key, set()).add(val_to_num(val))
                raw_cats.setdefault(key, set()).add(val)
        else:
            for (i, val) in enumerate(path.split("/")[:-1]):
                key = "dir%i" % i
                cats.setdefault(key, set()).add(val_to_num(val))
                raw_cats.setdefault(key, set()).add(val)

    for (key, v) in cats.items():
        # Check that no partition names map to the same value after
        # transformation by val_to_num
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

        # Check that all partition names map to the same type after
        # transformation by val_to_num
        if len(vals_by_type) > 1:
            examples = [x[0] for x in vals_by_type.values()]
            warnings.warn(
                "Partition names coerce to values of different"
                " types, e.g. %s" % examples
            )
    return {k: list(v) for k, v in cats.items()}


class FastParquetEngine(Engine):
    """ The API necessary to provide a new Parquet reader/writer """

    @staticmethod
    def read_metadata(
        fs, paths, categories=None, index=None, gather_statistics=None, filters=[]
    ):
        """ Gather metadata about a Parquet Dataset to prepare for a read

        Parameters
        ----------
        fs: FileSystem
        paths: List[str]
            A list of paths to files (or their equivalents)
        categories: list, dict or None
            Column(s) containing categorical data.
        index: str, List[str], or False
            The column name(s) to be used as the index.
            If set to ``None``, pandas metadata (if available) can be used
            to reset the value in this function
        gather_statistics: bool
            Whether or not to gather statistics data.  If ``None``, we only
            gather statistics data if there is a _metadata file available to
            query (cheaply)
        filters: list
            List of filters to apply, like ``[('x', '>', 0), ...]``. This
            implements row-group (partition) -level filtering only, i.e., to
            prevent the loading of some chunks of the data, and only if
            relevant statistics have been included in the metadata.

        Returns
        -------
        index: str, List[str], or False
            The column name(s) to be used as the index.
        meta: pandas.DataFrame
            An empty DataFrame object to use for metadata.
            Should have appropriate column names and dtypes but need not have
            any actual data
        statistics: Optional[List[Dict]]
            Either none, if no statistics were found, or a list of dictionaries
            of statistics data, one dict for every partition (see the next
            return value).  The statistics should look like the following:

            [
                {'num-rows': 1000, 'columns': [
                    {'name': 'id', 'min': 0, 'max': 100, 'null-count': 0},
                    {'name': 'x', 'min': 0.0, 'max': 1.0, 'null-count': 5},
                    ]},
                ...
            ]
        parts: List[object]
            A list of row-group-describing dictionary objects to be passed to
            ``fastparquet.read_partition``.
        """
        if len(paths) > 1:
            if gather_statistics is not False:
                # this scans all the files, allowing index/divisions
                # and filtering
                pf = fastparquet.ParquetFile(paths, open_with=fs.open, sep=fs.sep)
            else:
                base, fns = analyse_paths(paths)
                scheme = get_file_scheme(fns)
                pf = ParquetFile(paths[0], open_with=fs.open)
                pf.file_scheme = scheme
                pf.cats = _paths_to_cats(fns, scheme)
                relpath = paths.replace(base, "").lstrip("/")
                for rg in pf.row_groups:
                    rg.cats = pf.cats
                    rg.schema = pf.schema
                    for ch in rg.columns:
                        ch.file_path = relpath
        else:
            try:
                pf = fastparquet.ParquetFile(
                    paths[0] + fs.sep + "_metadata", open_with=fs.open, sep=fs.sep
                )
                if gather_statistics is None:
                    gather_statistics = True
            except Exception:
                pf = fastparquet.ParquetFile(paths[0], open_with=fs.open, sep=fs.sep)

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
            index_names = [n for n in index_names if n is not None]
            column_names.extend(pf.cats)

        else:
            raise ValueError("File has multiple entries for 'pandas' metadata")

        if index is None and len(index_names) > 0:
            if len(index_names) == 1:
                index = index_names[0]
            else:
                index = index_names

        # Normalize user inputs
        column_names, index_names = _normalize_index_columns(
            columns, column_names, index, index_names
        )

        all_columns = index_names + column_names

        if categories is None:
            categories = pf.categories
        elif isinstance(categories, string_types):
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

        meta = _meta_from_dtypes(
            all_columns, dtypes, index_cols=(), column_index_names=column_index_names
        )

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
            # make statistics conform in layout
            for (i, row_group) in enumerate(pf.row_groups):
                s = {"num-rows": row_group.num_rows, "columns": []}
                for col in pf.columns:
                    d = {"name": col}
                    if pf.statistics["min"][col][0] is not None:
                        cs_min = pf.statistics["min"][col][i]
                        cs_max = pf.statistics["max"][col][i]
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
                stats.append(s)

        else:
            stats = None

        pf._dtypes = lambda *args: pf.dtypes  # ugly patch, could be fixed
        pf.fmd.row_groups = None

        # Create `parts` (list of row-group-descriptor dicts)
        parts = [
            {"piece": rg, "kwargs": {"pf": pf, "categories": categories}}
            for rg in pf.row_groups
        ]

        return (index, meta, stats, parts)

    @staticmethod
    def read_partition(fs, piece, columns, **kwargs):
        """ Read a single piece of a Parquet dataset into a Pandas DataFrame

        This function is called many times in individual tasks

        Parameters
        ----------
        fs: FileSystem
        piece: object
            This is some token that is returned by Engine.read_metadata.
            Typically it represents a row group in a Parquet dataset
        columns: List[str]
            List of column names to pull out of that row group
        **kwargs:
            Includes the index (`kwargs["index"]`), and other
            `"kwargs"` values stored within the `parts` output of
            `fastparquet.read_metadata`: `pf` and `categories`

        Returns
        -------
        A Pandas DataFrame
        """
        pf = kwargs["pf"]
        categories = kwargs["categories"]
        index = kwargs["index"]
        if index is False:
            ind = False
        else:
            ind = None
        return pf.read_row_group_file(piece, columns, categories, index=ind)

    @staticmethod
    def initialize_write(
        df,
        fs,
        path,
        append=False,
        partition_on=None,
        ignore_divisions=False,
        division_info=None,
        **kwargs
    ):
        """Perform engine-specific initialization steps for this dataset

        Parameters
        ----------
        df: dask.dataframe.DataFrame
        fs: FileSystem
        path : string
            Destination directory for data.  Prepend with protocol like ``s3://``
            or ``hdfs://`` for remote data.
        append: bool
            If True, may use existing metadata (if any) and perform checks
            against the new data being stored.
        partition_on: List(str)
            Column(s) to use for dataset partitioning in parquet.
        ignore_divisions: bool
            Whether or not to ignore old divisions when appending.  Otherwise,
            overlapping divisions will lead to an error being raised.
        division_info: dict
            Dictionary containing the divisions and corresponding column name.
        **kwargs:
            Uses `object_encoding` and `index_cols`, if supplied.
            Remaining values are passed to `fastparquet.writer.make_metadata`

        Returns
        -------
        fmd: Parquet-file metadata object
        i_offset: int
            Initial offset to user for new-file labeling
        """
        if append and division_info is None:
            ignore_divisions = True
        fs.mkdirs(path)
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
                    "Requested file scheme is hive, " "but existing file scheme is not."
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

        return (fmd, i_offset)

    @staticmethod
    def write_metadata(parts, fmd, fs, path, append=False, **kwargs):
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

    @staticmethod
    def write_partition(
        df,
        path,
        fs,
        filename,
        partition_on,
        fmd=None,
        compression=None,
        return_metadata=True,
        index_cols=[],
        **kwargs
    ):
        fmd = copy.copy(fmd)
        if not len(df):
            # Write nothing for empty partitions
            rgs = []
        elif partition_on:
            rgs = partition_on_columns(
                df, partition_on, path, filename, fmd, compression, fs.open, fs.mkdirs
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
