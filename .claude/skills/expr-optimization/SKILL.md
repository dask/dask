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

**Prefer structural equality over value equality.** When testing optimizations,
we construct both a naive expression and the expected optimized form, then
compare their `._name` attributes after simplification:

```python
def test_slice_through_reduction():
    """x.sum(axis=0)[:5] should optimize to x[:, :5].sum(axis=0)"""
    x = da.ones((100, 100), chunks=(10, 10))

    # Naive: slice after reduction
    result = x.sum(axis=0)[:5]

    # Expected: slice pushed through reduction
    expected = x[:, :5].sum(axis=0)

    # Structural equality - this is the key assertion
    assert result.expr.simplify()._name == expected.expr.simplify()._name
```

Why `._name`? Names are deterministic based on expression structure - it's
essentially impossible for different expression trees to produce the same name.

### When to use `.simplify()` on both sides

**Always call `.simplify()` on both sides** unless you're testing that the
naive expression simplifies to an already-simple base expression:

```python
# When expected also has optimizable patterns (common case)
assert result.expr.simplify()._name == expected.expr.simplify()._name

# Only skip simplify on expected when it's a base expression like FromArray
x = da.from_array(arr, chunks=(5, 5))
assert x.T.T.expr.simplify()._name == x.expr._name  # x.expr is already simple
```

### Complete test pattern

A thorough optimization test includes both structural and value verification:

```python
def test_slice_through_elemwise():
    """(x + y)[:5] should optimize to x[:5] + y[:5]"""
    x = da.ones((100, 100), chunks=(10, 10))
    y = da.ones((100, 100), chunks=(10, 10))

    result = (x + y)[:5]
    expected = x[:5] + y[:5]

    # 1. Structural equality - verifies the optimization happened
    assert result.expr.simplify()._name == expected.expr.simplify()._name

    # 2. Value correctness - ensures we didn't break anything
    assert_eq(result, expected)
```

### Task count verification

For optimizations that reduce work, verify task count decreases:

```python
def test_optimization_reduces_tasks():
    arr = np.ones((100, 100))
    x = da.from_array(arr, chunks=(10, 10))

    full_tasks = len(x.sum(axis=0).optimize().__dask_graph__())
    sliced_tasks = len(x.sum(axis=0)[:5].optimize().__dask_graph__())

    assert sliced_tasks < full_tasks  # Optimization reduces work
```

### Other structural checks

Beyond `._name`, you can also verify structure with:
- `isinstance(expr.simplify(), ExpectedType)` - check expression type
- `expr._depth` - verify expression tree depth
- Checking that an operation name doesn't appear in `expr.pprint()`

### When value-only tests are acceptable

Use `assert_eq` alone only for:
- Correctness tests where optimization is NOT the focus
- Edge cases where structural verification is complex
- Parametrized tests covering many inputs where one structural test exists

## Modifying Expressions with substitute_parameters

When pushing operations through expression subclasses, use `substitute_parameters`
to preserve the subclass type while changing specific operands:

```python
# Instead of constructing a new Blockwise manually:
# result = Blockwise(bw.func, bw.out_ind, ...)  # Fails for subclasses!

# Use substitute_parameters to preserve the subclass:
sliced_input = new_collection(bw.array)[input_slices]
result = bw.substitute_parameters({"array": sliced_input.expr})
```

This pattern works with any expression subclass and only changes the specified
parameters while keeping everything else (including the type) intact.

## TDD

As always, we prefer to make tests before we start development, and use those
tests as we develop to constrain and guide our work.
