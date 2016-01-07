Glossary
========

Context manager: 
  A Python context manager uses the "with" statement to acquire a resource such as a file, network socket, database, or other mutual exclusion lock before performing an operation that requires it, and ensures that the resource is released cleanly at the end of the operation, even if the operation ends abnormally in some way, such as being interrupted by an exception. Context managers are described in `PEP 343 <https://www.python.org/dev/peps/pep-0343/>`_ and the `Python documentation <https://docs.python.org/3/library/contextlib.html>`_.

Dask array:
  Dask arrays are a drop-in replacement for a commonly used subset of NumPy algorithms that provide blocked algorithms on top of NumPy to handle larger-than-memory arrays and to leverage multiple cores. They implement a subset of the NumPy ndarray interface using blocked algorithms, cutting up the large array into many small arrays. This lets us compute on arrays larger than memory using all of our cores. 

Dask bag:
  Parallelizes computations across a large collection of generic Python objects. A bag is like a list but doesn’t guarantee an ordering among elements. There can be repeated elements, but you can’t ask for a particular element. Dask bag can process unstructured or semi-structured data.

Opportunistic caching: 
  A caching mechanism that automatically picks out and stores useful tasks.

Tuple: 
  A sequence of immutable Python objects. Tuples and lists are both sequences. The difference between tuples and lists is that tuples use parenthesis and are immutable (cannot be changed), whereas lists use square brackets and are mutable (can be changed).
