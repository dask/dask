import json

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from ....delayed import delayed
from ....bytes.core import get_pyarrow_filesystem
from ....utils import natural_sort_key, getargspec
from ..utils import _get_pyarrow_dtypes, _meta_from_dtypes
from ...utils import clear_known_categories

from .utils import _parse_pandas_metadata, _normalize_index_columns, Engine


class ArrowEngine(Engine):
    @staticmethod
    def read_partition(fs, piece, columns, partitions, categories):

        with fs.open(piece.path, mode="rb") as f:
            table = piece.read(
                columns=columns, partitions=partitions,
                use_pandas_metadata=True,
                file=f,
                use_threads=False
            )

        df = table.to_pandas(categories=categories, use_threads=False)

        return df[list(columns)]

    @staticmethod
    def read_metadata(
        fs, paths, categories=None, index=None, gather_statistics=None
    ):

        # In pyarrow, the physical storage field names may differ from
        # the actual dataframe names. This is true for Index names when
        # PyArrow >= 0.8.
        # We would like to resolve these to the correct dataframe names
        # as soon as possible.

        dataset = pq.ParquetDataset(paths, filesystem=get_pyarrow_filesystem(fs))

        if dataset.partitions is not None:
            partitions = [
                n for n in dataset.partitions.partition_names if n is not None
            ]
        else:
            partitions = []

        schema = dataset.schema.to_arrow_schema()
        columns = None

        has_pandas_metadata = (
            schema.metadata is not None and b"pandas" in schema.metadata
        )

        if has_pandas_metadata:
            pandas_metadata = json.loads(schema.metadata[b"pandas"].decode("utf8"))
            index_names, column_names, storage_name_mapping, _ = _parse_pandas_metadata(
                pandas_metadata
            )
        else:
            index_names = []
            column_names = schema.names
            storage_name_mapping = {k: k for k in column_names}

        column_names += [p for p in partitions if p not in column_names]
        column_names, index_names = _normalize_index_columns(
            columns, column_names, index, index_names
        )

        all_columns = index_names + column_names

        pieces = sorted(dataset.pieces, key=lambda piece: natural_sort_key(piece.path))

        dtypes = _get_pyarrow_dtypes(schema, categories)
        dtypes = {storage_name_mapping.get(k, k): v for k, v in dtypes.items()}

        meta = _meta_from_dtypes(all_columns, dtypes)
        meta = clear_known_categories(meta, cols=categories)

        if gather_statistics is None and dataset.metadata:
            gather_statistics = True

        if gather_statistics and pieces:
            # Read from _metadata file
            if dataset.metadata and dataset.metadata.num_row_groups == len(
                pieces
            ):  # one rg per piece
                row_groups = [
                    dataset.metadata.row_group(i)
                    for i in range(dataset.metadata.num_row_groups)
                ]
                names = dataset.metadata.schema.names
            else:
                # Read from each individual piece (quite possibly slow)
                row_groups = [
                    piece.get_metadata().row_group(0)
                    for piece in pieces
                ]
                if dataset.metadata:
                    names = dataset.metadata.schema.names
                else:
                    piece = pieces[0]
                    md = piece.get_metadata()
                    names = md.schema.names

            stats = []
            for row_group in row_groups:
                s = {"num-rows": row_group.num_rows, "columns": []}
                for i, name in enumerate(names):
                    column = row_group.column(i)
                    d = {"name": name}
                    if column.statistics:
                        d.update(
                            {
                                "min": column.statistics.min,
                                "max": column.statistics.max,
                                "null_count": column.statistics.null_count,
                            }
                        )
                    s["columns"].append(d)
                stats.append(s)

        else:
            stats = None
        # TODO: these are both sets, maybe not safe to zip over them
        if dataset.partitions:
            for name, partition_set in zip(dataset.partitions.partition_names, dataset.partitions.levels):
                if name in meta.columns:
                    meta[name] = pd.Categorical(categories=partition_set.keys, values=[])

        return (
            meta,
            stats,
            [
                {
                    "piece": piece,
                    "kwargs": {
                        "partitions": dataset.partitions,
                        "categories": categories,
                    },
                }
                for piece in pieces
            ],
        )

    @staticmethod
    def write(df, fs, path, append=False, partition_on=None, **kwargs):
        if append:
            try:
                dataset = pq.ParquetDataset(path, filesystem=get_pyarrow_filesystem(fs))
            except Exception:
                start = 0
            else:
                start = len(dataset.pieces)
            # TODO: check for hive directory structure
        else:
            start = 0

        # We can check only write_table kwargs, as it is a superset of kwargs for write functions
        if not set(kwargs).issubset(getargspec(pq.write_table).args):
            msg = "Unexpected keyword arguments: " + "%r" % (
                set(kwargs) - set(getargspec(pq.write_table).args)
            )
            raise TypeError(msg)

        fs.mkdirs(path)

        template = fs.sep.join([path, "part.%i.parquet"])

        write = delayed(ArrowEngine.write_partition, pure=False)

        first_kwargs = kwargs.copy()
        first_kwargs["metadata_path"] = fs.sep.join([path, "_common_metadata"])

        writes = [
            write(
                part,
                path,
                fs,
                template % (i + start),
                partition_on,
                **(kwargs if i else first_kwargs)
            )
            for i, part in enumerate(df.to_delayed())
        ]
        return delayed(writes)

    @staticmethod
    def initialize_write(df, fs, path, append=False, partition_on=None,
                        ignore_divisions=False, **kwargs):
        meta = None
        i_offset = 0
        if ignore_divisions:
            raise NotImplementedError("`ignore_divisions` not implemented"
                                      " for `engine='pyarrow'`")
        if append:
            raise NotImplementedError("`append` not implemented for "
                                      "`engine='pyarrow'`")
        return meta, i_offset

    @staticmethod
    def write_metadata(parts, fmd, fs, path, append=False, **kwargs):
        if parts:
            metadata_path = fs.sep.join([path, '_metadata'])
            common_metadata_path = fs.sep.join([path, '_common_metadata'])
            # Get only arguments specified in the function
            keywords = getargspec(pq.write_metadata).args
            kwargs_meta = {k: v for k, v in kwargs.items() if k in keywords}
            with fs.open(common_metadata_path, "wb") as fil:
                pq.write_metadata(parts[0][0]['schema'], fil, **kwargs_meta)
            # Aggregate metadata and write to _metadata file
            _meta = parts[0][0]['meta']
            for i in range(1, len(parts[0])):
                _meta.append_row_groups(parts[i][0]['meta'])
            with fs.open(metadata_path, "wb") as fil:
                _meta.write_metadata_file(fil)

    @staticmethod
    def write_partition(
        df, path, fs, filename, partition_on, fmd=None, compression=None,
        return_metadata=True, with_metadata=True, **kwargs
    ):
        md_list = []
        t = pa.Table.from_pandas(df, preserve_index=False)
        if partition_on:
            pq.write_to_dataset(
                t,
                path,
                partition_cols=partition_on,
                preserve_index=False,
                filesystem=fs,
                metadata_collector=md_list,
                **kwargs
            )
        else:
            with fs.open(fs.sep.join([path, filename]), "wb") as fil:
                pq.write_table(t, fil, compression=compression,
                               metadata_collector=md_list, **kwargs)
        # Return the schema needed to write the metadata
        if return_metadata:
            return [{'schema': t.schema, 'meta': md_list[0]}]
        else:
            return []
