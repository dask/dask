Dataframes from CSV files
=========================

Suppose we have a collection of CSV files with data:

**data1.csv:**

.. code-block:: none

    time,temperature,humidity
    0,22,58
    1,21,57
    2,25,57
    3,26,55
    4,22,53
    5,23,59

**data2.csv:**

.. code-block:: none

    time,temperature,humidity
    0,24,85
    1,26,83
    2,27,85
    3,25,92
    4,25,83
    5,23,81

**data3.csv:**

.. code-block:: none

    time,temperature,humidity
    0,18,51
    1,15,57
    2,18,55
    3,19,51
    4,19,52
    5,19,57

and so on.

We can create Dask dataframes from CSV files using ``dd.read_csv``.

.. code-block:: python

    >>> import dask.dataframe as dd
    >>> df = dd.read_csv('data*.csv')

We can work with the Dask dataframe as usual, which is composed of Pandas
dataframes. We can list the first few rows.

.. code-block:: python

    >>> df.head()
    time  temperature  humidity
    0     0           22        58
    1     1           21        57
    2     2           25        57
    3     3           26        55
    4     4           22        53

Or we can compute values over the entire dataframe.

.. code-block:: python

    >>> df.temperature.mean().compute()
    22.055555555555557

    >>> df.humidity.std().compute()
    14.710829233324224
