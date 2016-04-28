Read JSON records from disk
===========================

We commonly use ``dask.bag`` to process unstructured or semi-structured data:

.. code-block:: python

   >>> import dask.bag as db
   >>> import json
   >>> js = db.read_text('logs/2015-*.json.gz').map(json.loads)
   >>> js.take(2)
   ({'name': 'Alice', 'location': {'city': 'LA', 'state': 'CA'}},
    {'name': 'Bob', 'location': {'city': 'NYC', 'state': 'NY'})

   >>> result = js.pluck('name').frequencies()  # just another Bag
   >>> dict(result)                             # Evaluate Result
   {'Alice': 10000, 'Bob': 5555, 'Charlie': ...}
