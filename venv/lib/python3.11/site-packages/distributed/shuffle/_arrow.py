from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING

from packaging.version import parse

from dask.utils import parse_bytes

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa


_INPUT_PARTITION_ID_COLUMN = "__input_partition_id__"


def check_dtype_support(meta_input: pd.DataFrame) -> None:
    import pandas as pd

    for name in meta_input:
        column = meta_input[name]
        # FIXME: PyArrow does not support complex numbers: https://issues.apache.org/jira/browse/ARROW-638
        if pd.api.types.is_complex_dtype(column):
            raise TypeError(
                f"p2p does not support data of type '{column.dtype}' found in column '{name}'."
            )
        # FIXME: PyArrow does not support sparse data: https://issues.apache.org/jira/browse/ARROW-8679
        if isinstance(column.dtype, pd.SparseDtype):
            raise TypeError("p2p does not support sparse data found in column '{name}'")


def check_minimal_arrow_version() -> None:
    """Verify that the the correct version of pyarrow is installed to support
    the P2P extension.

    Raises a ModuleNotFoundError if pyarrow is not installed or an
    ImportError if the installed version is not recent enough.
    """
    minversion = "14.0.1"
    try:
        import pyarrow as pa
    except ModuleNotFoundError:
        raise ModuleNotFoundError(f"P2P shuffling requires pyarrow>={minversion}")
    if parse(pa.__version__) < parse(minversion):
        raise ImportError(
            f"P2P shuffling requires pyarrow>={minversion} but only found {pa.__version__}"
        )


def concat_tables(tables: Iterable[pa.Table]) -> pa.Table:
    import pyarrow as pa

    return pa.concat_tables(tables, promote_options="permissive")


def convert_shards(
    shards: list[pa.Table], meta: pd.DataFrame, partition_column: str, drop_column: bool
) -> pd.DataFrame:
    import pandas as pd
    from pandas.core.dtypes.cast import find_common_type  # type: ignore[attr-defined]

    from dask.dataframe.dispatch import from_pyarrow_table_dispatch

    table = concat_tables(shards)
    table = table.sort_by(_INPUT_PARTITION_ID_COLUMN)
    table = table.drop([_INPUT_PARTITION_ID_COLUMN])

    if drop_column:
        meta = meta.drop(columns=partition_column)
    df = from_pyarrow_table_dispatch(meta, table, self_destruct=True)
    reconciled_dtypes = {}
    for column, dtype in meta.dtypes.items():
        actual = df[column].dtype
        if actual == dtype:
            continue
        # Use the specific string dtype from meta (e.g., string[pyarrow])
        if isinstance(actual, pd.StringDtype) and isinstance(dtype, pd.StringDtype):
            reconciled_dtypes[column] = dtype
            continue
        # meta might not be aware of the actual categories so the two dtype objects are not equal
        # Also, the categories_dtype does not properly roundtrip through Arrow
        if isinstance(actual, pd.CategoricalDtype) and isinstance(
            dtype, pd.CategoricalDtype
        ):
            continue
        reconciled_dtypes[column] = find_common_type([actual, dtype])

    from dask.dataframe._compat import PANDAS_GE_300

    kwargs = {} if PANDAS_GE_300 else {"copy": False}
    return df.astype(reconciled_dtypes, **kwargs)


def buffers_to_table(data: list[tuple[int, bytes]]) -> pa.Table:
    import numpy as np
    import pyarrow as pa

    """Convert a list of arrow buffers and a schema to an Arrow Table"""

    def _create_input_partition_id_array(
        table: pa.Table, input_partition_id: int
    ) -> pa.ChunkedArray:
        arrays = (
            np.full(
                (batch.num_rows,),
                input_partition_id,
                dtype=np.uint32(),
            )
            for batch in table.to_batches()
        )
        return pa.chunked_array(arrays)

    tables = (
        (input_partition_id, deserialize_table(buffer))
        for input_partition_id, buffer in data
    )
    tables = (
        table.append_column(
            _INPUT_PARTITION_ID_COLUMN,
            _create_input_partition_id_array(table, input_partition_id),
        )
        for input_partition_id, table in tables
    )

    return concat_tables(tables)


def serialize_table(table: pa.Table) -> bytes:
    import pyarrow as pa

    stream = pa.BufferOutputStream()
    with pa.ipc.new_stream(stream, table.schema) as writer:
        writer.write_table(table)
    return stream.getvalue().to_pybytes()


def deserialize_table(buffer: bytes) -> pa.Table:
    import pyarrow as pa

    with pa.ipc.open_stream(pa.py_buffer(buffer)) as reader:
        return reader.read_all()


def read_from_disk(path: Path) -> tuple[list[pa.Table], int]:
    import pyarrow as pa

    batch_size = parse_bytes("1 MiB")
    batch = []
    shards = []

    with pa.OSFile(str(path), mode="rb") as f:
        size = f.seek(0, whence=2)
        f.seek(0)
        prev = 0
        offset = f.tell()
        while offset < size:
            sr = pa.RecordBatchStreamReader(f)
            shard = sr.read_all()
            offset = f.tell()
            batch.append(shard)

            if offset - prev >= batch_size:
                table = concat_tables(batch)
                shards.append(_copy_table(table))
                batch = []
                prev = offset
    if batch:
        table = concat_tables(batch)
        shards.append(_copy_table(table))
    return shards, size


def _copy_table(table: pa.Table) -> pa.Table:
    import pyarrow as pa

    arrs = [pa.concat_arrays(column.chunks) for column in table.columns]
    return pa.table(data=arrs, schema=table.schema)
