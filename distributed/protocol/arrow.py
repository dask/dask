from __future__ import print_function, division, absolute_import

from .serialize import dask_serialize, dask_deserialize

import pyarrow

if pyarrow.__version__ < "0.10":
    raise ImportError(
        "Need pyarrow >= 0.10 . "
        "See https://arrow.apache.org/docs/python/install.html"
    )


@dask_serialize.register(pyarrow.RecordBatch)
def serialize_batch(batch):
    sink = pyarrow.BufferOutputStream()
    writer = pyarrow.RecordBatchStreamWriter(sink, batch.schema)
    writer.write_batch(batch)
    writer.close()
    buf = sink.getvalue()
    header = {}
    frames = [buf]
    return header, frames


@dask_deserialize.register(pyarrow.RecordBatch)
def deserialize_batch(header, frames):
    blob = frames[0]
    reader = pyarrow.RecordBatchStreamReader(pyarrow.BufferReader(blob))
    return reader.read_next_batch()


@dask_serialize.register(pyarrow.Table)
def serialize_table(tbl):
    sink = pyarrow.BufferOutputStream()
    writer = pyarrow.RecordBatchStreamWriter(sink, tbl.schema)
    writer.write_table(tbl)
    writer.close()
    buf = sink.getvalue()
    header = {}
    frames = [buf]
    return header, frames


@dask_deserialize.register(pyarrow.Table)
def deserialize_table(header, frames):
    blob = frames[0]
    reader = pyarrow.RecordBatchStreamReader(pyarrow.BufferReader(blob))
    return reader.read_all()
