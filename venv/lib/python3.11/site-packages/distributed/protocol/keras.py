from __future__ import annotations

import keras

from distributed.protocol.serialize import (
    dask_deserialize,
    dask_serialize,
    deserialize,
    serialize,
)


@dask_serialize.register(keras.Model)
def serialize_keras_model(model):
    import keras

    if keras.__version__ < "1.2.0":
        raise ImportError(
            "Need Keras >= 1.2.0. Try python -m pip install keras --upgrade --no-deps"
        )

    header = model._updated_config()
    weights = model.get_weights()
    headers, frames = list(zip(*map(serialize, weights)))
    header["headers"] = headers
    header["nframes"] = [len(L) for L in frames]
    frames = [frame for L in frames for frame in L]
    return header, frames


@dask_deserialize.register(keras.Model)
def deserialize_keras_model(header, frames):
    from keras.models import model_from_config

    n = 0
    weights = []
    for head, length in zip(header["headers"], header["nframes"]):
        x = deserialize(head, frames[n : n + length])
        weights.append(x)
        n += length
    model = model_from_config(header)
    model.set_weights(weights)
    return model
