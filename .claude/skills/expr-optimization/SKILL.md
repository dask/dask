---
name: expr-optimization
description: Add or improve expression optimizations. Use when adding simplify_down rules, pushing operations through each other, or testing optimization correctness. (project)
---

# Query Optimization

Dask Dataframe and Dask Array (and hopefully others in the future) use the Expr
class to represent computations.  These expressions hold the intend of the user
and can be used to generate task graphs.

Before we generate task graphs though we can optimize the expression, producing
a better version that produces the same result with less work.  Typically we
write down optimizations with the following methods:

```
def _simplify_down(self):
    """ Return a better version of this expression and subtree """
    ... inspect subtree ...
    ... maybe make a better expression ...
    return new_expr

def _lower(self):
    """ Return a more concrete and less abstract version of this expression """
```

We don't always have to return something new.  In fact the common situation is
just to remain the same.

## Testing

When we test we often write the optimized version of the expression, and test
expression equality against that.

For example

```python
def test_transposing_twice_is_identity(self):
    x = da.ones((5, 5))
    y = x.T.T
    result = y.expr.simplify()
    expected = x.expr

    assert result._name == expected._name
```

Here testing against _name suffices because `_name` is deterministic based on
the structure of the graph (it's pretty impossible for different expression
trees to produce the same name).

**Subtlety**: When the expected expression also contains optimizable patterns
(e.g., slices into `from_array`), you may need `.simplify()` on both sides:

```python
# When expected also has slices that push into IO
assert result.expr.simplify()._name == expected.expr.simplify()._name
```

### Task Count Verification

For optimizations that reduce work, verify task count decreases:

```python
def test_optimization_reduces_tasks():
    arr = np.ones((100, 100))
    x = da.from_array(arr, chunks=(10, 10))

    full_tasks = len(x.sum(axis=0).optimize().__dask_graph__())
    sliced_tasks = len(x.sum(axis=0)[:5].optimize().__dask_graph__())

    assert sliced_tasks < full_tasks  # Optimization reduces work
```

We might also check expression structure using tools like `._depth`, or
verifying that the name of some expression doesn't occur in `expr.pprint()`

If we need to we can also test value equality

```python
assert_eq(result, expected)
```

But this is a bit weaker because it doesn't check expression structure, which
is what we really care about here for performance.

## TDD

As always, we prefer to make tests before we start development, and use those
tests as we develop to constrain and guide our work.
