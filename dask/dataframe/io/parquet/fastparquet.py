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
            for key, val in partitions:
                cats.setdefault(key, set()).add(val_to_num(val))
                raw_cats.setdefault(key, set()).add(val)
        else:
            for i, val in enumerate(path.split("/")[:-1]):
                key = "dir%i" % i
                cats.setdefault(key, set()).add(val_to_num(val))
                raw_cats.setdefault(key, set()).add(val)

    for key, v in cats.items():
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
        fs, paths, categories=None, index=None, gather_statistics=None
    ):
        """

        Returns
        -------
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
                        {'name': 'value', 'min': 0.0, 'max': 1.0, 'null-count': 5},
                        ]},  # TODO: we might want to rethink this oranization
                    ...
                ]
        parts: List[object]
            A list of objects to be passed to ``Engine.read_partition``.
            Each object should represent a row group of data.
            We don't care about the type of this object, as long as the
            read_partition function knows how to interpret it.
        """
        import fastparquet
        if len(paths) > 1:
            if gather_statistics is not False:
                # this scans all the files, allowing index/divisions
                # and filtering
                pf = fastparquet.ParquetFile(paths, open_with=fs.open,
                                             sep=fs.sep)
            else:
                base, fns = analyse_paths(paths)
                scheme = get_file_scheme(fns)
                pf = ParquetFile(paths[0], open_with=fs.open)
                pf.file_scheme = scheme
                pf.cats = _paths_to_cats(fns, scheme)
                relpath = path.replace(base, "").lstrip("/")
                for rg in pf.row_groups:
                    rg.cats = pf.cats
                    rg.scheme = pf.scheme
                    for ch in rg.columns:
                        ch.file_path = relpath
        else:
            try:
                pf = fastparquet.ParquetFile(
                    paths[0] + fs.sep + "_metadata", open_with=fs.open,
                    sep=fs.sep
                )
            except Exception:
                pf = fastparquet.ParquetFile(paths[0], open_with=fs.open,
                                             sep=fs.sep)
        stats = []
        # make statistics conform in layout
        for i, rg in enumerate(pf.row_groups):
            cols = []
            part = {"columns": cols, "num-rows": rg.num_rows}
            for col in pf.columns:
                if pf.statistics['min'][col][0] is not None:
                    mi = pf.statistics['min'][col][i]
                    ma = pf.statistics['max'][col][i]
                    nc = pf.statistics['null_count'][col][i]
                    bit = {"max": ma, "min": mi, "null_count": nc, "name": col}
                    cols.append(bit)
            stats.append(part)
        meta = pf.pre_allocate(0, pf.columns, categories, index)[0]
        pieces = [{'piece': rg,
                   'kwargs': {'pf': pf, 'categories': categories}}
                  for rg in pf.row_groups]
        pf._dtypes = lambda *args: pf.dtypes  # ugly patch, could be fixed
        pf.fmd.row_groups = None
        del pf._schema, pf.row_groups
        return meta, stats, pieces

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
        partitions:
        categories:

        Returns
        -------
        A Pandas DataFrame
        """
        pf = kwargs['pf']
        categories = kwargs['categories']
        return pf.read_row_group_file(piece, columns, categories)

    @staticmethod
    def initialize_write(df, fs, path, append=False, partition_on=None,
                        ignore_divisions=False, division_info=None, **kwargs):
        if append:
            try:
                # to append to a dataset without _metadata, need to load
                # _common_metadata or any data file here
                pf = fastparquet.api.ParquetFile(path, open_with=fs.open,
                                                 sep=fs.sep)
            except (IOError, ValueError):
                # append for create
                append = False
        if append:
            if pf.file_scheme not in ["hive", "empty", "flat"]:
                raise ValueError(
                    "Requested file scheme is hive, "
                    "but existing file scheme is not."
                )
            elif (set(pf.columns) != set(df.columns) - set(partition_on)) or (
                    set(partition_on) != set(pf.cats)
            ):
                raise ValueError(
                    "Appended columns not the same.\n"
                    "Previous: {} | New: {}".format(pf.columns,
                                                    list(df.columns))
                )
            elif (pd.Series(pf.dtypes).loc[pf.columns] != df[
                    pf.columns].dtypes).any():
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
                minmax = fastparquet.api.sorted_partitioned_columns(pf)
                old_end = minmax[index_cols[0]]["max"][-1]
                if df.divisions[0] < old_end:
                    raise ValueError(
                        "Appended divisions overlapping with the previous ones."
                        "\n"
                        "Previous: {} | New: {}".format(old_end, df.divisions[0])
                    )
        else:
            fmd = fastparquet.writer.make_metadata(
                df._meta,
                object_encoding=object_encoding,
                index_cols=[],
                ignore_columns=partition_on,
            )
            i_offset = 0

        return fmd, i_offset

    @staticmethod
    def write_metadata(parts, fmd, fs, path, append=False, **kwargs):
        fmd = copy.copy(fmd)
        if parts:
            if all(isinstance(part, list) for part in parts):
                parts = sum(parts, [])
            fmd.row_groups = parts
            fn = fs.sep.join([path, "_metadata"])
            fastparquet.writer.write_common_metadata(
                fn, fmd, open_with=fs.open, no_row_groups=False
            )

        # if appending, could skip this, but would need to check existence
        fn = fs.sep.join([path, "_common_metadata"])
        fastparquet.writer.write_common_metadata(fn, fmd, open_with=fs.open)

    @staticmethod
    def write_partition(
        df, path, fs, filename, partition_on, fmd=None, compression=None,
        return_metadata=True, **kwargs
    ):
        fmd = copy.copy(fmd)
        if not len(df):
            # Write nothing for empty partitions
            rgs = []
        elif partition_on:
            rgs = partition_on_columns(
                df, partition_on, path, filename, fmd, compression, fs.open,
                fs.mkdirs
            )
        else:
            with fs.open(fs.sep.join([path, filename]), "wb") as fil:
                rg = make_part_file(fil, df, fmd.schema,
                                    compression=compression, fmd=fmd)
            for chunk in rg.columns:
                chunk.file_path = filename
            rgs = [rg]
        if return_metadata:
            return rgs
        else:
            return []
