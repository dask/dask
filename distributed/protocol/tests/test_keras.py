import functools
import sys
import traceback

import numpy as np
from numpy.testing import assert_allclose
import pytest

keras = pytest.importorskip('keras')

from distributed.protocol import serialize, deserialize, dumps, loads, to_serialize


def test_serialize_deserialize_model():
    model = keras.models.Sequential()
    model.add(keras.layers.Dense(5, input_dim=3))
    model.add(keras.layers.Dense(2))
    model.compile(optimizer='sgd', loss='mse')
    x = np.random.random((1, 3))
    y = np.random.random((1, 2))
    model.train_on_batch(x, y)

    loaded = deserialize(*serialize(model))
    assert_allclose(loaded.predict(x), model.predict(x))

    data = {'model': to_serialize(model)}
    frames = dumps(data)
    result = loads(frames)
    assert_allclose(result['model'].predict(x), model.predict(x))
