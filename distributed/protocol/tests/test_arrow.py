import pandas as pd
import pytest

pa = pytest.importorskip('pyarrow')

from distributed.utils_test import gen_cluster
from distributed.protocol import deserialize, serialize
from distributed.protocol.serialize import class_serializers, typename


df = pd.DataFrame({'A': list('abc'), 'B': [1,2,3]})
tbl = pa.Table.from_pandas(df, preserve_index=False)
batch = pa.RecordBatch.from_pandas(df, preserve_index=False)


@pytest.mark.parametrize('obj', [batch, tbl], ids=["RecordBatch", "Table"])
def test_roundtrip(obj):
    # Test that the serialize/deserialize functions actually
    # work independent of distributed
    header, frames = serialize(obj)
    new_obj = deserialize(header, frames)
    assert obj.equals(new_obj)


@pytest.mark.parametrize('obj', [batch, tbl], ids=["RecordBatch", "Table"])
def test_typename(obj):
    # The typename used to register the custom serialization is hardcoded
    # ensure that the typename hasn't changed
    assert typename(type(obj)) in class_serializers


def echo(arg):
    return arg


@pytest.mark.parametrize('obj', [batch, tbl], ids=["RecordBatch", "Table"])
def test_scatter(obj):
    @gen_cluster(client=True)
    def run_test(client, scheduler, worker1, worker2):
        obj_fut = yield client.scatter(obj)
        fut = client.submit(echo, obj_fut)
        result = yield fut
        assert obj.equals(result)
    run_test()
