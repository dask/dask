from __future__ import print_function, division, absolute_import

from .serialize import register_serialization


def serialize_batch(batch):
    import pyarrow as pa
    sink = pa.BufferOutputStream()
    writer = pa.RecordBatchStreamWriter(sink, batch.schema)
    writer.write_batch(batch)
    writer.close()
    buf = sink.get_result()
    header = {}
    frames = [buf]
    return header, frames


def deserialize_batch(header, frames):
    import pyarrow as pa
    blob = frames[0]
    reader = pa.RecordBatchStreamReader(pa.BufferReader(blob))
    return reader.read_next_batch()


def serialize_table(tbl):
    import pyarrow as pa
    sink = pa.BufferOutputStream()
    writer = pa.RecordBatchStreamWriter(sink, tbl.schema)
    writer.write_table(tbl)
    writer.close()
    buf = sink.get_result()
    header = {}
    frames = [buf]
    return header, frames


def deserialize_table(header, frames):
    import pyarrow as pa
    blob = frames[0]
    reader = pa.RecordBatchStreamReader(pa.BufferReader(blob))
    return reader.read_all()


register_serialization(
    'pyarrow.lib.RecordBatch',
    serialize_batch,
    deserialize_batch
)
register_serialization(
    'pyarrow.lib.Table',
    serialize_table,
    deserialize_table
)
