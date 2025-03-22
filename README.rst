Dask
====

|Build Status| |Coverage| |Doc Status| |Discourse| |Version Status| |NumFOCUS|

Dask is a flexible parallel computing library for analytics. Visit our website_ or see 
documentation_ for more information.

Dask enables parallel and distributed computing in Python, allowing you to scale the Python tools you already know and love.

Installation
-----------

Dask can be installed with conda or pip:

.. code-block:: bash

   # Install with conda
   conda install dask

   # Install with pip (full installation)
   pip install "dask[complete]"

For more information and options, see the `installation guide`_.

Quick Start
-----------

.. code-block:: python

   # Create a Dask DataFrame
   import dask.dataframe as dd
   df = dd.read_csv('my-data-*.csv')
   
   # Use familiar pandas-like operations
   result = df.groupby('column').mean().compute()
   
   # Create a local cluster
   from dask.distributed import LocalCluster
   cluster = LocalCluster()
   client = cluster.get_client()

Supported Python Versions
-------------------------

Dask supports Python 3.9 to 3.12.

LICENSE
-------

New BSD. See `License File <https://github.com/dask/dask/blob/main/LICENSE.txt>`__.

.. _documentation: https://docs.dask.org/en/stable/
.. _website: https://dask.org/
.. _`installation guide`: https://docs.dask.org/en/stable/install.html
.. |Build Status| image:: https://github.com/dask/dask/actions/workflows/tests.yml/badge.svg
   :target: https://github.com/dask/dask/actions/workflows/tests.yml
.. |Coverage| image:: https://codecov.io/gh/dask/dask/branch/main/graph/badge.svg
   :target: https://codecov.io/gh/dask/dask/branch/main
   :alt: Coverage status
.. |Doc Status| image:: https://readthedocs.org/projects/dask/badge/?version=latest
   :target: https://docs.dask.org/en/stable/
   :alt: Documentation Status
.. |Discourse| image:: https://img.shields.io/discourse/users?logo=discourse&server=https%3A%2F%2Fdask.discourse.group
   :alt: Discuss Dask-related things and ask for help
   :target: https://dask.discourse.group
.. |Version Status| image:: https://img.shields.io/pypi/v/dask.svg
   :target: https://pypi.python.org/pypi/dask/
.. |NumFOCUS| image:: https://img.shields.io/badge/powered%20by-NumFOCUS-orange.svg?style=flat&colorA=E1523D&colorB=007D8A
   :target: https://www.numfocus.org/
