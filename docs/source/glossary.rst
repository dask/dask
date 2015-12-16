Glossary
========

Context manager: 
  A way of allocating and releasing a resource in Python exactly where you need it. 

Dask array:
  Dask arrays are a drop-in replacement for a commonly used subset of NumPy algorithms that provide blocked algorithms on top of NumPy to handle larger-than-memory arrays and to leverage multiple cores. They implement a subset of the NumPy ndarray interface using blocked algorithms, cutting up the large array into many small arrays. This lets us compute on arrays larger than memory using all of our cores. 

Dask bag:
  Parallelizes computations across a large collection of generic Python objects. A bag is like a list but doesn’t guarantee an ordering among elements. There can be repeated elements, but you can’t ask for a particular element. Dask bag can process unstructured or semi-structured data.

Opportunistic caching: 
  A caching mechanism that automatically picks out and stores useful tasks.

Tuple: 
  A sequence of immutable Python objects. Tuples are sequences, like lists. The difference between tuples and lists is that the tuples cannot be changed, and tuples use parentheses, whereas lists use square brackets.


