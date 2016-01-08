Glossary
========

Context manager: 
  A Python context manager uses the "with" statement to acquire a resource such 
  as a file, network socket, database, or other mutual exclusion lock before 
  performing an operation that requires it, and ensures that the resource is 
  released cleanly at the end of the operation, even if the operation ends 
  abnormally in some way, such as being interrupted by an exception. Context 
  managers are described in `PEP 343 <https://www.python.org/dev/peps/pep-0343/>`_ 
  and the `Python documentation <https://docs.python.org/3/library/contextlib.html>`_.

Dask array:
  :doc:`Dask arrays <array>` are a drop-in replacement for a commonly used subset 
  of NumPy algorithms. They implement a subset of the NumPy ndarray interface to 
  provide blocked algorithms that divide each large array into small arrays. This 
  enables computation on arrays larger than memory and enables the use of multiple 
  cores.

Dask bag:
  A set is an unordered collection of elements, each of which may be present only 
  once in the set. A multiset or "bag" is an unordered collection of elements, each 
  of which may be present multiple times in the bag. :doc:`Dask bags <bag>` 
  parallelize computations across large bags of generic Python objects. Dask bags 
  are suitable for processing unstructured or semi-structured data such as large 
  JSON blobs or log files.

Dask dataframe:
  The pandas dataframe is a two dimensional labeled data structure with columns 
  which may have different types, similar to a spreadsheet or SQL table, or a dict 
  of pandas series objects. :doc:`Dask dataframes <dataframe>` look and feel like 
  pandas dataframes but operate on datasets larger than memory using multiple 
  threads. Dask.dataframe does not implement the complete pandas interface.

Opportunistic caching: 
  Dask's :doc:`caching` monitors tasks to measure their past computation cost, 
  storage cost, and frequency of use, and to predict their future frequency of 
  use, and uses this information to choose automatically which tasks to cache.
