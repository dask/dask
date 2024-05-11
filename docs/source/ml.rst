Machine Learning
================

Machine learning is a broad field involving many different workflows.  This
page lists a few of the more common ways in which Dask can help you with ML
workloads.


Hyper-Parameter Optimization
----------------------------

Optuna
~~~~~~

For state of the art Hyper Parameter Optimization (HPO) we recommend the
`Optuna <https://optuna.org/>`_ library,
with the associated
`Dask-Optuna integration <https://optuna-integration.readthedocs.io/en/latest/reference/generated/optuna_integration.DaskStorage.html>`_

Consider also this video:

.. raw:: html

   <iframe width="560"
           height="315"
           src="https://www.youtube.com/embed/euT6_h7iIBA"
           frameborder="0"
           allow="autoplay; encrypted-media"
           style="margin: 0 auto 20px auto; display: block;"
           allowfullscreen>
   </iframe>

TODO: what's the best optuna example here?

Dask Futures
~~~~~~~~~~~~

Additionally, for simpler situations people often use simple :doc:`Dask futures <futures>` to
train the same model on lots of parameters.  Dask futures are a general purpose
API that is used to run normal Python functions on various inputs.  An example
might look like the following:

.. code-block:: python

   from dask.distributed import LocalCluster

   cluster = LocalCluster(processes=False)  # replace this with some scalable cluster
   client = cluster.get_client()

   def train_and_score(params: dict) -> float:
       # TODO: your code here
       data = load_data()
       model = make_model(**params)
       train(model)
       score = evaluate(model)
       return score

   params_list = [...]
   futures = [client.submit(train_and_score, params) for params in params_list]
   scores = client.gather(futures)
   best = max(scores)

   best_params = params_list[scores.index(best)]


Gradient Boosted Trees
----------------------

Popular GBT libraries have native Dask support which allows you to train models
on very large datasets in parallel.  Both XGBoost and LightGBM have pretty good
documentation and examples:

-  `XGBoost <https://xgboost.readthedocs.io/en/stable/tutorials/dask.html>`_
-  `LightGBM <https://lightgbm.readthedocs.io/en/latest/Parallel-Learning-Guide.html#dask>`_

For convenience, here is a copy-pastable example using Dask Dataframe, XGBoost,
and the Dask LocalCluster to train on randomly generated data

.. code-block:: python

   import dask.dataframe as dd
   import xgboost as xgb
   from dask.distributed import LocalCluster

   df = dask.datasets.timeseries()  # randomly generated data
   # df = dd.read_parquet(...)  # probably you would read data though in practice

   train, test = df.random_split(...)  # TODO

   with LocalCluster() as cluster:
       with cluster.get_client() as client:
           # TODO


Batch Inference
---------------

Often you already have a machine learning model and just want to apply it to
lots of data.  We see this done most often in two ways:

1.  Using Dask Futures
2.  Using ``map_partitions`` or ``map_blocks`` calls of Dask Dataframe or Dask
    Array

We'll show two examples below:

Dask Futures for Batch Inference
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Dask futures are a general purpose API that lets us run arbitrary Python
functions on Python data.  It's easy to apply this tool to solve the problem of
batch inference.

For example, we often see this when people want to apply a model to many
different files.

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

Batch Prediction with Dask Dataframe
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Sometimes we access our that we want to process with our model with a higher
level Dask API, like Dask Dataframe or Dask Array.  This is more common with
record data, for example if we had a set of patient records and we wanted to
see which were likely to become ill

.. code-block:: python

   import dask.dataframe as dd

   df = dd.read_parquet("/path/to/my/data.parquet")

   model = load_model("/path/to/my/model")

   # pandas code
   # predictions = model.predict(df)
   predictions.to_parquet("/path/to/results.parquet")

   # Dask code
   predictions = df.map_partitions(model.predict)
   predictions.to_parquet("/path/to/results.parquet")
