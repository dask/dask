Machine Learning
================

Dask facilitates machine learning, statistics, and optimization workloads in a
variety of ways.  Generally Dask tries to support other high-quality solutions
within the PyData ecosystem rather than reinvent new systems.  Dask makes it
easier to scale single-machine libraries like Scikit-Learn where possible and
makes using distributed libraries like XGBoost or Tensorflow more comfortable
for average users.

Dask can be helpful in the following ways:

SKLearn: Model selection
------------------------

Dask can help to accelerate model selection and hyper parameter searches
(GridSearchCV, RandomSearchCV) with dask-searchcv_.

.. code-block:: python

   from sklearn.feature_extraction.text import CountVectorizer
   from sklearn.feature_extraction.text import TfidfTransformer
   from sklearn.linear_model import SGDClassifier
   from sklearn.pipeline import Pipeline

   pipeline = Pipeline([('vect', CountVectorizer()),
                        ('tfidf', TfidfTransformer()),
                        ('clf', SGDClassifier())])

   # from sklearn.model_selection import GridSearchCV  # use SKLearn
   from dask_searchcv import GridSearchCV              # or use Dask

   grid_search = GridSearchCV(pipeline, parameters)
   grid_search.fit(data.data, data.target)


SKLearn: Accelerated Joblib
---------------------------

Scikit-Learn parallelizes its algorithms internally with Joblib_ a simple
parallel computing API used throughout sklearn's codebase.  While Joblib
normally runs from a local thread or processing pool Dask can also step in and
provide distributed computing support.  See
http://distributed.readthedocs.io/en/latest/joblib.html for more
information.

.. code-block:: python

   import distributed.joblib
   import joblib

   with joblib.parallel_backend('dask.distributed', scheduler_host='localhost:8786'):
       # normal sklearn code

Convex Optimization Dask.Array
------------------------------

Just as Scikit-Learn implements machine learning algorithms like gradient
descent with the NumPy API we can implement many of those same algorithms with
the Dask.array API (which is a drop-in replacement).  Many algorithms for
convex optimization are implemented in dask-glm_

.. code-block:: python

    from dask_glm.algorithms import newton, bfgs, proximal_gradient, admm
    from dask_glm.families import Logistic, Normal

    import dask.array as da
    X = da....
    y = da....

    beta = admm(X, y, family=Logistic)


Other Distributed Systems
---------------------------------

Other high-quality parallel and distributed systems for machine learning
already exist.  For example XGBoost supports gradient boosted trees and
Tensorflow supports deep learning.  Both of these libraries already have robust
support for distributed computing.  In these cases Dask does not compete with
these libraries, but instead facilitates setting them up on a pre-existing Dask
cluster and then handles the smooth transfer of data from Dask workers to
XGBoost or Tensorflow workers running in the same Python processes.

For more information, see the following subprojects:

-  dask-xgboost_
-  dask-tensorflow_


.. _dask-searchcv: https://github.com/dask/dask-searchcv
.. _dask-glm: https://github.com/dask/dask-glm
.. _dask-xgboost: https://github.com/dask/dask-xgboost
.. _dask-tensorflow: https://github.com/dask/dask-tensorflow
.. _Joblib: https://pythonhosted.org/joblib/
