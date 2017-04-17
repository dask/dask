from sklearn.svm import LinearSVC
from sklearn.datasets import load_iris
from sklearn.feature_selection import SelectKBest
from sklearn.decomposition import PCA
from sklearn.cross_validation import train_test_split
import numpy as np

import dask.learn as dl
import dask.imperative as di

iris = load_iris()

X_train, X_test, y_train, y_test = train_test_split(iris.data, iris.target)


def test_grid_search():
    pipeline = dl.Pipeline([("pca", PCA()),
                            ("select_k", SelectKBest()),
                            ("svm", LinearSVC())])
    param_grid = {'select_k__k': [1, 2, 3, 4],
                  'svm__C': np.logspace(-3, 2, 3)}
    grid = dl.GridSearchCV(pipeline, param_grid)

    grid.fit(X_train, y_train)
    result = grid.score(X_test, y_test)
    assert isinstance(result, float)
