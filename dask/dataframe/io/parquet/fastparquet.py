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


def _read_fastparquet(
    fs,
    fs_token,
    paths,
    columns=None,
    filters=None,
    categories=None,
    index=None,
    infer_divisions=None,
):

    if isinstance(paths, fastparquet.api.ParquetFile):
        pf = paths
    elif len(paths) > 1:
        if infer_divisions is not False:
            # this scans all the files, allowing index/divisions and filtering
            pf = fastparquet.ParquetFile(paths, open_with=fs.open, sep=fs.sep)
        else:
            return _read_fp_multifile(
                fs, fs_token, paths, columns=columns, categories=categories, index=index
            )
    else:
        try:
            pf = fastparquet.ParquetFile(
                paths[0] + fs.sep + "_metadata", open_with=fs.open, sep=fs.sep
            )
        except Exception:
            pf = fastparquet.ParquetFile(paths[0], open_with=fs.open, sep=fs.sep)

    # Validate infer_divisions
    if os.path.split(pf.fn)[-1] != "_metadata" and infer_divisions is True:
        raise NotImplementedError(
            "infer_divisions=True is not supported by the "
            "fastparquet engine for datasets that "
            "do not contain a global '_metadata' file"
        )

    (
        meta,
        filters,
        index_name,
        all_columns,
        index_names,
        storage_name_mapping,
    ) = _pf_validation(pf, columns, index, categories, filters)
    rgs = [
        rg
        for rg in pf.row_groups
        if not (fastparquet.api.filter_out_stats(rg, filters, pf.schema))
        and not (fastparquet.api.filter_out_cats(rg, filters))
    ]

    name = "read-parquet-" + tokenize(fs_token, paths, all_columns, filters, categories)
    dsk = {
        (name, i): (
            _read_parquet_row_group,
            fs,
            pf.row_group_filename(rg),
            index_names,
            all_columns,
            rg,
            categories,
            pf.schema,
            pf.cats,
            pf.dtypes,
            pf.file_scheme,
            storage_name_mapping,
        )
        for i, rg in enumerate(rgs)
    }
    if not dsk:
        # empty dataframe
        dsk = {(name, 0): meta}
        divisions = (None, None)
        return new_dd_object(dsk, name, meta, divisions)

    if index_names and infer_divisions is not False:
        index_name = meta.index.name
        try:
            # is https://github.com/dask/fastparquet/pull/371 available in
            # current fastparquet installation?
            minmax = fastparquet.api.sorted_partitioned_columns(pf, filters)
        except TypeError:
            minmax = fastparquet.api.sorted_partitioned_columns(pf)
        if index_name in minmax:
            divisions = minmax[index_name]
            divisions = divisions["min"] + [divisions["max"][-1]]
        else:
            if infer_divisions is True:
                raise ValueError(
                    (
                        "Unable to infer divisions for index of '{index_name}'"
                        " because it is not known to be "
                        "sorted across partitions"
                    ).format(index_name=index_name)
                )

            divisions = (None,) * (len(rgs) + 1)
    else:
        if infer_divisions is True:
            raise ValueError(
                "Unable to infer divisions for because no index column"
                " was discovered"
            )

        divisions = (None,) * (len(rgs) + 1)

    if isinstance(divisions[0], np.datetime64):
        divisions = [pd.Timestamp(d) for d in divisions]

    return new_dd_object(dsk, name, meta, divisions)


def _pf_validation(pf, columns, index, categories, filters):
    """Validate user options against metadata in dataset

     columns, index and categories must be in the list of columns available
     (both data columns and path-based partitioning - subject to possible
     renaming, if pandas metadata is present). The output index will
     be inferred from any available pandas metadata, if not given.
     """
    check_column_names(pf.columns, categories)
    check_column_names(pf.columns + list(pf.cats or []), columns)
    if isinstance(columns, tuple):
        # ensure they tokenize the same
        columns = list(columns)

    if pf.fmd.key_value_metadata:
        pandas_md = [x.value for x in pf.fmd.key_value_metadata if x.key == "pandas"]
    else:
        pandas_md = []

    if len(pandas_md) == 0:
        # Fall back to the storage information
        index_names = pf._get_index()
        if not isinstance(index_names, list):
            index_names = [index_names]
        column_names = pf.columns + list(pf.cats)
        storage_name_mapping = {k: k for k in column_names}
    elif len(pandas_md) == 1:
        index_names, column_names, storage_name_mapping, column_index_names = _parse_pandas_metadata(
            json.loads(pandas_md[0])
        )
        column_names.extend(pf.cats)
    else:
        raise ValueError("File has multiple entries for 'pandas' metadata")

    # Normalize user inputs

    if filters is None:
        filters = []

    column_names, index_names = _normalize_index_columns(
        columns, column_names, index, index_names
    )

    if categories is None:
        categories = pf.categories
    elif isinstance(categories, string_types):
        categories = [categories]
    else:
        categories = list(categories)

    # TODO: write partition_on to pandas metadata...
    all_columns = list(column_names)
    all_columns.extend(x for x in index_names if x not in column_names)

    dtypes = pf._dtypes(categories)
    dtypes = {storage_name_mapping.get(k, k): v for k, v in dtypes.items()}

    meta = _meta_from_dtypes(all_columns, dtypes, index_names, [None])

    # fastparquet doesn't handle multiindex
    if len(index_names) > 1:
        raise ValueError("Cannot read DataFrame with MultiIndex.")
    elif len(index_names) == 0:
        index_names = None

    for cat in categories:
        if cat in meta:
            meta[cat] = pd.Series(
                pd.Categorical([], categories=[UNKNOWN_CATEGORIES]), index=meta.index
            )

    for catcol in pf.cats:
        if catcol in meta.columns:
            meta[catcol] = meta[catcol].cat.set_categories(pf.cats[catcol])
        elif meta.index.name == catcol:
            meta.index = meta.index.set_categories(pf.cats[catcol])

    return (meta, filters, index_names, all_columns, index_names, storage_name_mapping)


def _read_fp_multifile(fs, fs_token, paths, columns=None, categories=None, index=None):
    """Read dataset with fastparquet by assuming metadata from first file"""
    base, fns = analyse_paths(paths)
    parsed_paths = [join_path(p) for p in paths]
    scheme = get_file_scheme(fns)
    pf = ParquetFile(paths[0], open_with=fs.open)
    pf.file_scheme = scheme
    pf.cats = _paths_to_cats(fns, scheme)
    (
        meta,
        _,
        index_name,
        all_columns,
        index_names,
        storage_name_mapping,
    ) = _pf_validation(pf, columns, index, categories, [])
    name = "read-parquet-" + tokenize(fs_token, paths, all_columns, categories)
    dsk = {
        (name, i): (
            _read_pf_simple,
            fs,
            path,
            base,
            index_names,
            all_columns,
            categories,
            pf.cats,
            pf.file_scheme,
            storage_name_mapping,
        )
        for i, path in enumerate(parsed_paths)
    }
    divisions = (None,) * (len(paths) + 1)
    return new_dd_object(dsk, name, meta, divisions)


def _read_pf_simple(
    fs,
    path,
    base,
    index_names,
    all_columns,
    categories,
    cats,
    scheme,
    storage_name_mapping,
):
    """Read dataset with fastparquet using ParquetFile machinery"""
    pf = ParquetFile(path, open_with=fs.open)
    relpath = path.replace(base, "").lstrip("/")
    for rg in pf.row_groups:
        for ch in rg.columns:
            ch.file_path = relpath
    pf.file_scheme = scheme
    pf.cats = cats
    pf.fn = base
    df = pf.to_pandas(all_columns, categories, index=index_names)
    if df.index.nlevels == 1:
        if index_names:
            df.index.name = storage_name_mapping.get(index_names[0], index_names[0])
    else:
        if index_names:
            df.index.names = [
                storage_name_mapping.get(name, name) for name in index_names
            ]
    df.columns = [
        storage_name_mapping.get(col, col)
        for col in all_columns
        if col not in (index_names or [])
    ]

    return df


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


def _read_parquet_file(
    fs,
    base,
    fn,
    index,
    columns,
    categories,
    cs,
    dt,
    scheme,
    storage_name_mapping,
    *args
):
    """Read a single file with fastparquet, to be used in a task"""

    name_storage_mapping = {v: k for k, v in storage_name_mapping.items()}
    assert isinstance(columns, (tuple, list))
    if index:
        index, = index
        if index not in columns:
            columns = columns + [index]
    columns = [name_storage_mapping.get(col, col) for col in columns]
    index = name_storage_mapping.get(index, index)
    cs = OrderedDict([(k, v) for k, v in cs.items() if k in columns])
    pf = ParquetFile(fn, open_with=fs.open)
    pf.file_scheme = scheme
    for rg in pf.row_groups:
        for ch in rg.columns:
            ch.file_path = fn.replace(base, "").lstrip("/")
    pf.fn = base
    df = pf.to_pandas(columns=columns, index=index, categories=categories)

    if df.index.nlevels == 1:
        if index:
            df.index.name = storage_name_mapping.get(index, index)
    else:
        if index:
            df.index.names = [storage_name_mapping.get(name, name) for name in index]
    df.columns = [storage_name_mapping.get(col, col) for col in columns if col != index]

    return df


def _read_parquet_row_group(
    fs,
    fn,
    index,
    columns,
    rg,
    categories,
    schema,
    cs,
    dt,
    scheme,
    storage_name_mapping,
    *args
):

    name_storage_mapping = {v: k for k, v in storage_name_mapping.items()}
    assert isinstance(columns, (tuple, list))
    if index:
        index, = index
        if index not in columns:
            columns = columns + [index]

    columns = [name_storage_mapping.get(col, col) for col in columns]
    index = name_storage_mapping.get(index, index)
    cs = OrderedDict([(k, v) for k, v in cs.items() if k in columns])

    df, views = _pre_allocate(rg.num_rows, columns, categories, index, cs, dt)
    read_row_group_file(
        fn,
        rg,
        columns,
        categories,
        schema,
        cs,
        open=fs.open,
        assign=views,
        scheme=scheme,
    )

    if df.index.nlevels == 1:
        if index:
            df.index.name = storage_name_mapping.get(index, index)
    else:
        if index:
            df.index.names = [storage_name_mapping.get(name, name) for name in index]
    df.columns = [storage_name_mapping.get(col, col) for col in columns if col != index]

    return df


def _write_partition_fastparquet(
    df, fs, path, filename, fmd, compression, partition_on
):
    # Fastparquet mutates this in a non-threadsafe manner. For now we just copy
    # it before forwarding to fastparquet.
    fmd = copy.copy(fmd)
    if not len(df):
        # Write nothing for empty partitions
        rgs = None
    elif partition_on:
        if LooseVersion(fastparquet.__version__) >= "0.1.4":
            rgs = partition_on_columns(
                df, partition_on, path, filename, fmd, compression, fs.open, fs.mkdirs
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
                fs.mkdirs,
            )
    else:
        # Fastparquet current doesn't properly set `num_rows` in the output
        # metadata. Set it here to fix that.
        fmd.num_rows = len(df)
        with fs.open(fs.sep.join([path, filename]), "wb") as fil:
            rgs = make_part_file(fil, df, fmd.schema, compression=compression, fmd=fmd)
    return rgs


def _write_fastparquet(
    df,
    fs,
    fs_token,
    path,
    write_index=None,
    append=False,
    ignore_divisions=False,
    partition_on=None,
    compression=None,
    **kwargs
):
    fs.mkdirs(path)
    sep = fs.sep

    object_encoding = kwargs.pop("object_encoding", "utf8")
    if object_encoding == "infer" or (
        isinstance(object_encoding, dict) and "infer" in object_encoding.values()
    ):
        raise ValueError(
            '"infer" not allowed as object encoding, '
            "because this required data in memory."
        )

    divisions = df.divisions
    if write_index is True or write_index is None and df.known_divisions:
        df = df.reset_index()
        index_cols = [df.columns[0]]
    else:
        ignore_divisions = True
        index_cols = []

    if append:
        try:
            pf = fastparquet.api.ParquetFile(path, open_with=fs.open, sep=sep)
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
            minmax = fastparquet.api.sorted_partitioned_columns(pf)
            old_end = minmax[index_cols[0]]["max"][-1]
            if divisions[0] < old_end:
                raise ValueError(
                    "Appended divisions overlapping with the previous ones.\n"
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

    filenames = ["part.%i.parquet" % (i + i_offset) for i in range(df.npartitions)]

    write = delayed(_write_partition_fastparquet, pure=False)
    writes = [
        write(part, fs, path, filename, fmd, compression, partition_on)
        for filename, part in zip(filenames, df.to_delayed())
    ]

    return delayed(_write_metadata)(writes, filenames, fmd, path, fs, sep)


def _write_metadata(writes, filenames, fmd, path, fs, sep):
    """ Write Parquet metadata after writing all row groups

    See Also
    --------
    to_parquet
    """
    fmd = copy.copy(fmd)
    for fn, rg in zip(filenames, writes):
        if rg is not None:
            if isinstance(rg, list):
                for r in rg:
                    fmd.row_groups.append(r)
            else:
                for chunk in rg.columns:
                    chunk.file_path = fn
                fmd.row_groups.append(rg)

    fn = sep.join([path, "_metadata"])
    fastparquet.writer.write_common_metadata(
        fn, fmd, open_with=fs.open, no_row_groups=False
    )

    fn = sep.join([path, "_common_metadata"])
    fastparquet.writer.write_common_metadata(fn, fmd, open_with=fs.open)
