from sklearn.linear_model import SGDClassifier
import dask.array as da
import numpy as np
import dask


x = np.array([[1, 0],
              [2, 0],
              [3, 0],
              [4, 0],
              [0, 1],
              [0, 2],
              [3, 3],
              [4, 4]])

y = np.array([1, 1, 1, 1, -1, -1, 0, 0])

z = np.array([[1, -1],
              [-1, 1],
              [10, -10],
              [-10, 10]])

X = da.from_array(x, blockshape=(3, 2))
Y = da.from_array(y, blockshape=(3,))
Z = da.from_array(z, blockshape=(2, 2))

def test_fit():
    sgd = SGDClassifier()

    sgd = da.learn.fit(sgd, X, Y, get=dask.get, classes=[-1, 1])

    result = sgd.predict(z)
    assert result.tolist() == [1, -1, 1, -1]

    result = da.learn.predict(sgd, Z)
    assert result.blockdims == ((2, 2),)
    assert result.compute(get=dask.get).tolist() == [1, -1, 1, -1]

