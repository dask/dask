Machine Learning
================

Machine learning is a broad field involving many different workflows.  This
page lists a few of the more common ways in which Dask can help you with ML
workloads.

- :ref:`hpo`
- :ref:`boosted-trees`
- :ref:`batch-prediction`

.. _hpo:

Hyperparameter Optimization
---------------------------

Optuna
~~~~~~

For state of the art hyperparameter optimization (HPO) we recommend the
`Optuna <https://optuna.org/>`_ library,
with the associated
`Dask integration <https://optuna-integration.readthedocs.io/en/latest/reference/generated/optuna_integration.DaskStorage.html>`_.


In Optuna you construct an objective function that takes a trial object, which
generates parameters from distributions that you define in code.  Your
objective function eventually produces a score.  Optuna is smart about what
values from the distribution it suggests based on the scores it has received.

.. code-block:: python

   def objective(trial):
       params = {
           "max_depth": trial.suggest_int("max_depth", 2, 10, step=1),
           "learning_rate": trial.suggest_float("learning_rate", 1e-8, 1.0, log=True),
           ...
       }
       model = train_model(train_data, **params)
       result = score(model, test_data)
       return result

Dask and Optuna are often used together by running many objective functions in
parallel and synchronizing the scores and parameter selection on the Dask
scheduler.  To do this, we use the ``DaskStore`` object found in Optuna.

.. code-block:: python

   import optuna

   storage = optuna.integration.DaskStorage()

   study = optuna.create_study(
       direction="maximize",
       storage=storage,  # This makes the study Dask-enabled
   )

Then we run many optimize methods in parallel.

.. code-block:: python

   from dask.distributed import LocalCluster, wait

   cluster = LocalCluster(processes=False)  # replace this with some scalable cluster
   client = cluster.get_client()

   futures = [
       client.submit(study.optimize, objective, n_trials=1, pure=False) for _ in range(500)
   ]
   wait(futures)

   print(study.best_params)

For a more fully worked example see this :bdg-link-primary:`Optuna + XGBoost example <https://docs.coiled.io/user_guide/hpo.html?utm_source=dask-docs&utm_medium=ml>`.


Dask Futures
~~~~~~~~~~~~

Additionally, for simpler situations people often use :doc:`Dask Futures <futures>` to
train the same model on lots of parameters.  Dask Futures are a general purpose
API that is used to run normal Python functions on various inputs.  An example
might look like the following:

.. code-block:: python

   from dask.distributed import LocalCluster

   cluster = LocalCluster(processes=False)  # replace this with some scalable cluster
   client = cluster.get_client()

   def train_and_score(params: dict) -> float:
       data = load_data()
       model = make_model(**params)
       train(model)
       score = evaluate(model)
       return score

   params_list = [...]
   futures = [
       client.submit(train_and_score, params) for params in params_list
   ]
   scores = client.gather(futures)
   best = max(scores)

   best_params = params_list[scores.index(best)]

For a more fully worked example see :bdg-link-primary:`Futures Documentation <futures.html>`.

.. _boosted-trees:

Gradient Boosted Trees
----------------------

Popular GBT libraries, like XGBoost and LightGBM, have native Dask support which allows you to train models
on very large datasets in parallel.

-  `XGBoost <https://xgboost.readthedocs.io/en/stable/tutorials/dask.html>`_
-  `LightGBM <https://lightgbm.readthedocs.io/en/latest/Parallel-Learning-Guide.html#dask>`_

For example, using Dask DataFrame, XGBoost, and a local Dask cluster looks like the following:

.. code-block:: python

   import dask.dataframe as dd
   import xgboost as xgb
   from dask.distributed import LocalCluster

   df = dask.datasets.timeseries()  # Randomly generated data
   # df = dd.read_parquet(...)      # In practice, you would probably read data though

   train, test = df.random_split([0.80, 0.20])
   X_train, y_train, X_test, y_test = ...

   with LocalCluster() as cluster:
       with cluster.get_client() as client:
           d_train = xgb.dask.DaskDMatrix(client, X_train, y_train, enable_categorical=True)
           model = xgb.dask.train(
               ...
               d_train,
           )
           predictions = xgb.dask.predict(client, model, X_test)

For a more fully worked example see this :bdg-link-primary:`XGBoost example <https://docs.coiled.io/user_guide/xgboost.html?utm_source=dask-docs&utm_medium=ml>`.

.. _batch-prediction:

Batch Prediction
----------------

Once a model is trained, it's common to want to apply the model across
lots of data.  We see this done most often in two ways:

1.  Using Dask Futures
2.  Using :py:meth:`DataFrame.map_partitions <dask.dataframe.DataFrame.map_partitions>`
    or :py:meth:`Array.map_blocks <dask.array.Array.map_blocks>`

We'll show examples of each approach below.

Dask Futures
~~~~~~~~~~~~

Dask Futures are a general purpose API that lets you run arbitrary Python
functions on Python data in parallel. It's easy to apply this tool to solve the problem of
batch prediction.

For example, we often see this when people want to apply a model across many
data files.

.. code-block:: python

   from dask.distributed import LocalCluster

   cluster = LocalCluster(processes=False)  # replace this with some scalable cluster
   client = cluster.get_client()

   filenames = [...]

   def predict(filename, model):
       data = load(filename)
       result = model.predict(data)
       return result

   model = client.submit(load_model, path_to_model)
   predictions = client.map(predict, filenames, model=model)
   results = client.gather(predictions)

For a more fully worked example see :bdg-link-primary:`Batch Scoring for Computer Vision Workloads (video) <https://developer.download.nvidia.com/video/gputechconf/gtc/2019/video/S9198/s9198-dask-and-v100s-for-fast-distributed-batch-scoring-of-computer-vision-workloads.mp4>`.

Dask DataFrame
~~~~~~~~~~~~~~

Sometimes we want to process our model with a higher
level Dask API, like Dask DataFrame or Dask Array. This is more common with
record data, for example if we had a set of patient records and wanted to
see which patients were likely to become ill.

.. code-block:: python

   import dask.dataframe as dd

   df = dd.read_parquet("/path/to/my/data.parquet")

   model = load_model("/path/to/my/model")

   # pandas code
   # predictions = model.predict(df)
   # predictions.to_parquet("/path/to/results.parquet")

   # Dask code
   predictions = df.map_partitions(model.predict)
   predictions.to_parquet("/path/to/results.parquet")

For more information see :bdg-link-primary:`Dask DataFrame documentation <dataframe.html>`.
