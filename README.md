Dask High Level Expressions Proof of Concept
============================================

This proof of concept is a partial rewrite of Dask Dataframe to provide high
level expressions.  These capture original user intent, allowing better
understanding and optimization.

Install with ...

```
pip install -e .
```

You should then be able to run tests

```
py.test dask_match
```

There is then a small demonstration notebook

```
jupyter lab demo.ipynb
```
