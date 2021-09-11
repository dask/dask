import json
from typing import Union

import pyarrow as pa

PYARROW_CHECKPOINT_SCHEMA = [
    "txn",
    "add",
    "remove",
    "metaData",
    "protocol",
    "commitInfo",
]
FASTPARQUET_CHECKPOINT_SCHEMA = [
    "txn.appId",
    "txn.version",
    "txn.lastUpdated",
    "add.path",
    "add.partitionValues",
    "add.size",
    "add.modificationTime",
    "add.dataChange",
    "add.stats",
    "add.tags",
    "remove.path",
    "remove.deletionTimestamp",
    "remove.dataChange",
    "metaData.id",
    "metaData.name",
    "metaData.description",
    "metaData.format.provider",
    "metaData.format.options",
    "metaData.schemaString",
    "metaData.partitionColumns",
    "metaData.configuration",
    "metaData.createdTime",
    "protocol.minReaderVersion",
    "protocol.minWriterVersion",
    "commitInfo.version",
    "commitInfo.timestamp",
    "commitInfo.userId",
    "commitInfo.userName",
    "commitInfo.operation",
    "commitInfo.operationParameters",
    "commitInfo.job.jobId",
    "commitInfo.job.jobName",
    "commitInfo.job.runId",
    "commitInfo.job.jobOwnerId",
    "commitInfo.job.triggerType",
    "commitInfo.notebook.notebookId",
    "commitInfo.clusterId",
    "commitInfo.readVersion",
    "commitInfo.isolationLevel",
    "commitInfo.isBlindAppend",
    "commitInfo.operationMetrics",
    "commitInfo.userMetadata",
]


def schema_from_string(schema_string: str):

    fields = []
    schema = json.loads(schema_string)
    for field in schema["fields"]:
        name = field["name"]
        type = field["type"]
        nullable = field["nullable"]
        metadata = field["metadata"]
        pa_type = map_type(type)

        fields.append(pa.field(name, pa_type, nullable=nullable, metadata=metadata))
    return pa.schema(fields)


def map_type(input_type: Union[dict, str]):

    simple_type_mapping = {
        "byte": pa.int8(),
        "short": pa.int16(),
        "integer": pa.int32(),
        "long": pa.int64(),
        "float": pa.float32(),
        "double": pa.float64(),
        "string": pa.string(),
        "boolean": pa.bool_(),
        "binary": pa.binary(),
        "date": pa.date32(),
        "timestamp": pa.timestamp("ns"),
    }

    # If type is string, it should be a "simple" datatype
    if isinstance(input_type, str):

        # map simple data types, that can be directly converted
        if input_type in simple_type_mapping:
            pa_type = simple_type_mapping[input_type]
        else:
            raise TypeError(
                f"Got type unsupported {input_type} when trying to parse schema"
            )

    # nested field needs special handling
    else:
        if input_type["type"] == "array":
            # map list type to pyarrow types
            element_type = map_type(input_type["elementType"])
            # pass a field as the type to the list with a name of "element".
            # This is just to comply with the way pyarrow creates lists when infering schemas
            pa_field = pa.field("element", element_type)
            pa_type = pa.list_(pa_field)

        elif input_type["type"] == "map":
            key_type = map_type(input_type["keyType"])
            item_type = map_type(input_type["valueType"])
            pa_type = pa.map_(key_type, item_type)

        elif input_type["type"] == "struct":
            fields = []
            for field in input_type["fields"]:
                name = field["name"]
                input_type = field["type"]
                nullable = field["nullable"]
                metadata = field["metadata"]
                field_type = map_type(input_type)

                fields.append(
                    pa.field(name, field_type, nullable=nullable, metadata=metadata)
                )
            pa_type = pa.struct(fields)

        else:
            raise TypeError(
                f"Got type unsupported {input_type} when trying to parse schema"
            )
    return pa_type
