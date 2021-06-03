from distutils.version import LooseVersion

import pyarrow as pa
import pyarrow.orc as orc

from ..utils import _get_pyarrow_dtypes, _meta_from_dtypes


class ArrowORCEngine:
    @classmethod
    def read_metadata(
        cls,
        fs,
        paths,
        columns,
        index,
        partition_stripe_count,
        **kwargs,
    ):

        # Convert root directory to file list.
        # TODO: Handle hive-partitioned data
        if len(paths) == 1 and fs.isdir(paths[0]):
            paths = fs.find(paths[0])

        schema = None
        parts = []
        for path in paths:
            _stripes = []
            with fs.open(path, "rb") as f:
                o = orc.ORCFile(f)
                if schema is None:
                    schema = o.schema
                elif schema != o.schema:
                    raise ValueError("Incompatible schemas while parsing ORC files")
                for stripe in range(o.nstripes):
                    # TODO: Can filter out stripes here
                    _stripes.append(stripe)
                    if (
                        partition_stripe_count
                        and len(_stripes) >= partition_stripe_count
                    ):
                        parts.append([(path, _stripes)])
                        _stripes = []
            if _stripes:
                # TODO: Enable multi-file parts
                parts.append([(path, _stripes)])
        schema = _get_pyarrow_dtypes(schema, categories=None)
        if columns is not None:
            ex = set(columns) - set(schema)
            if ex:
                raise ValueError(
                    "Requested columns (%s) not in schema (%s)" % (ex, set(schema))
                )

        columns = list(schema) if columns is None else columns
        index = [index] if isinstance(index, str) else index
        meta = _meta_from_dtypes(columns, schema, index, [])
        return parts, schema, meta

    @classmethod
    def read_partition(cls, fs, parts, schema, columns, **kwargs):
        batches = []
        for path, stripes in parts:
            batches += _read_orc_stripes(fs, path, stripes, schema, columns)
        if pa.__version__ < LooseVersion("0.11.0"):
            return pa.Table.from_batches(batches).to_pandas()
        else:
            return pa.Table.from_batches(batches).to_pandas(date_as_object=False)

    @classmethod
    def write_partition(cls, df, path, fs, filename, **kwargs):
        table = pa.Table.from_pandas(df)
        with fs.open(fs.sep.join([path, filename]), "wb") as f:
            orc.write_table(table, f)


def _read_orc_stripes(fs, path, stripes, schema, columns):
    # Construct a list of RecordBatch objects.
    # Each ORC stripe will corresonpond to a single RecordBatch.
    if columns is None:
        columns = list(schema)

    batches = []
    with fs.open(path, "rb") as f:
        o = orc.ORCFile(f)
        for stripe in stripes:
            batches.append(o.read_stripe(stripe, columns))
    return batches
