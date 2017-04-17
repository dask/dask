GridSearch, Cross Validation, Pipeline
======================================


Motivation
----------

Scikit learn already parallelizes through Joblib.
What are their current pain points?

1.  Grid searches of pipelines don't reuse shared results.
    For example, consider the following search:

    pipe = Pipeline(A->B->C)
    search = ParameterSearch(pipe, [(a1, b1, c1), (a1, b1, c2), ..., (ai, bj, ck)])

    Currently scikit-learn evaluates A with parameters a1 many times without
    reuse.  By better tracking intermediate results we can avoid many repeated
    computations
2.  Nesting of parallelism.  When two systems try to parallelize at once they
    interfere with each other.  (does this happen?)
3.  Out-of-core cross validation with partial fit
4.  Proper distributed computing


Objective
---------

We'd like a tiny proof of concept library that includes `Pipeline` and
`ParameterSearch`.  These don't need to support the full API but do need to be
useful on a common case problem.

Our model problem is to create a Pipeline of scikit learn estimators:

    pipe = Pipeline(A, B, C)

And then search over a parameter space within this pipeline:

    grid = GridSearchCV(pipe, {parameter space})


Plan
----

We create the following class hierarchy

    Base - DaskEstimator - ParameterSearch - GridSearch
                         \
                          - Pipeline

DaskEstimator supports the following subset of the scikit-learn API

    clone
    fit
    predict
    score
    set_params


### Compatibility with scikit-learn

These operations act as scikit-learn users expect.  They trigger computation

### Laziness with dask

There will also be a lazy version of this API:

    _dask_fit
    _dask_predict
    _dask_score
    _dask_set_params

These operations inject tasks into a `.dask` graph on the `DaskEstimator`
object.  Note that, unlike other dask collecitons, `DaskEstimator` is mutable.
We are entirely willing to mutate the dask dictionary on this object.  We
should still ensure that tasks are defined in a consistent/deterministic way,
e.g. using `dask.base.tokenize`.

And so we can define fit as follows

```python
def fit(self, *args, **kwargs):
    return self._dask_fit(*args, **kwargs).compute()
```


Compatibility
-------------

Consider how the `ParameterSearch._dask_fit` function would interact with a
`sklearn.Estimator` and with a `dask.learn.Estimator`.

*   If the estimator is a concrete `sklearn.Estimator` then we can make a fairly
    straightforward, embarassingly parallel graph of the estimator under
    different parameters with no other inputs.

        dsk = {(name, i): (clone_parameterize_fit_score, est, params, *data)
                for i, params in enumerate(self.params)}

*   If the estimator is a lazy `dask.learn.Estimator` then then we need to
    merge in the graph of the input under the parameters

        dsk = {}
        for i, params in enumerate(self.params):
            est = clone_and_set_params(est, params)
            dsk.update(est.dask)
            dsk[(name, i)] = (fit_and_score, est._name)


