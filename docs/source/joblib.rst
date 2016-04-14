Joblib Frontend
===============

Joblib_ is a library for simple parallel programming primarily developed and
used by the Scikit Learn community.  As of version 0.10 it contains a plugin
mechanism to allow Joblib code to use other parallel frameworks to execute
computations.  The ``distributed`` scheduler implements such a plugin in the
``distributed.joblib`` module and registers it appropriately with Joblib.  As a
result, any joblib code (including many scikit-learn algorithms) will run on
the distributed scheduler if you enclose it in a context manager as follows:

.. code-block:: python

   import distributed.joblib
   from joblib import Parallel, parallel_backend

   with parallel_backend('distributed', scheduler_host='HOST:PORT'):
       # normal Joblib code

For example you might distributed a randomized cross validated parameter search
as follows:

.. code-block:: python

   import distributed.joblib
   from joblib import Parallel, parallel_backend
   from sklearn.datasets import load_digits
   from sklearn.grid_search import RandomizedSearchCV
   from sklearn.svm import SVC
   import numpy as np

   digits = load_digits()

   param_space = {
       'C': np.logspace(-6, 6, 13),
       'gamma': np.logspace(-8, 8, 17),
       'tol': np.logspace(-4, -1, 4),
       'class_weight': [None, 'balanced'],
   }

   model = SVC(kernel='rbf')
   search = RandomizedSearchCV(model, param_space, cv=3, n_iter=50, verbose=10)

   with parallel_backend('distributed', scheduler_host='localhost:8786'):
       search.fit(digits.data, digits.target)

.. _`Joblib`: https://pythonhosted.org/joblib/
