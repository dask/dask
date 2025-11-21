from __future__ import annotations

import pyarrow.fs

from distributed.protocol.serialize import dask_deserialize, dask_serialize

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


@dask_serialize.register(pyarrow.fs.FileInfo)
def serialize_fileinfo(fileinfo):
    return {}, [(fileinfo.path, fileinfo.size, fileinfo.mtime_ns)]


@dask_deserialize.register(pyarrow.fs.FileInfo)
def serialize_filesystem(header, frames):
    path, size, mtime_ns = frames[0]
    return pyarrow.fs.FileInfo(path=path, size=size, mtime_ns=mtime_ns)
