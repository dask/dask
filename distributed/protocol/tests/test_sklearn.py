import pytest

pytest.importorskip("sklearn")

import sklearn.linear_model

from distributed.protocol import serialize, deserialize


@pytest.mark.xfail(
    reason="We no longer special-case the BaseEstimator "
    "super class. It's hard to guarantee support for all subclasseses"
)
def test_basic():
    est = sklearn.linear_model.LinearRegression()
    est.fit([[0, 0], [1, 1], [2, 2]], [0, 1, 2])

    header, frames = serialize(est)
    assert header["serializer"] == "dask"

    est2 = deserialize(header, frames)

    inp = [[2, 3], [-1, 3]]
    assert (est.predict(inp) == est2.predict(inp)).all()
