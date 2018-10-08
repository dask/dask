msg = """ It is no longer necessary to `import dask_ml.joblib` or
`import distributed.joblib`.

This functionality has moved into the core Joblib codebase.

To use Joblib's Dask backend with Scikit-Learn >= 0.20.0

    from dask.distributed import Client
    client = Client()

    from sklearn.externals import joblib

    with joblib.parallel_backend('dask'):
        # your scikit-learn code

See http://ml.dask.org/joblib.html for more information."""


raise ImportError(msg)
