---
name: array-expr
description: Develop Dask Array expressions including new operations, optimizations, and debugging. Use for work in dask/array/_array_expr/ like implementing operations, adding simplification rules, or investigating expression behavior. (project)
---

# Array Expression Development

**Design doc**: `designs/array-expr.md`

## Architecture

Array expressions separate **intent** from **execution**:

```python
# User code creates expression tree
result = (x + y).sum(axis=0)

# Expression tree: Sum(Elemwise(+, x.expr, y.expr), axis=0)
# Graph generated only at compute time
```

### Core Components

| Component | Location | Purpose |
|-----------|----------|---------|
| `ArrayExpr` | `_expr.py` | Base class for all expressions |
| `Array` | `_collection.py` | User-facing wrapper |
| `Blockwise` | `_blockwise.py` | Aligned block operations |
| `Elemwise` | `_blockwise.py` | Broadcasting element-wise ops |

### Expression Class Anatomy

```python
class MyOp(ArrayExpr):
    _parameters = ["array", "option"]     # Operand names
    _defaults = {"option": None}          # Default values

    @cached_property
    def chunks(self):
        return self.array.chunks          # Output chunking

    @cached_property
    def _meta(self):
        return self.array._meta           # dtype/type info

    def _layer(self):
        return {(self._name, i): ...}     # Task dict
```

## Key Patterns

### Blockwise Operations

Most array operations align blocks across inputs:

```python
from dask.array._array_expr._blockwise import Blockwise, Elemwise

# Element-wise with broadcasting
class Add(Elemwise):
    _parameters = ["x", "y"]
    func = staticmethod(np.add)

# Custom blockwise
class MatMul(Blockwise):
    _parameters = ["a", "b"]
    # _blockwise_arg defines input/output indices
```

### Reductions

Use tree reduction pattern:

```python
from dask.array._array_expr._reductions import PartialReduce

class Sum(PartialReduce):
    chunk = staticmethod(np.sum)
    combine = staticmethod(np.sum)
    aggregate = staticmethod(np.sum)
```

### Shape-Changing Operations

Implement custom `_layer()` for reshape, transpose, rechunk:

```python
def _layer(self):
    # Build task dict mapping output keys to input key references
    return {
        (self._name, i, j): (func, (input_name, ...))
        for i, j in product(...)
    }
```

## Optimization

Expressions optimize via three methods:

```python
def _simplify_down(self):
    """Rewrite self based on children. Return new expr or None."""
    # Example: push slices through operations
    if isinstance(self.child, Transpose):
        return self.child.child[transposed_slice]

def _simplify_up(self, parent, dependents):
    """Rewrite self based on parent context. Return new expr or None."""

def _lower(self):
    """Convert abstract to concrete form before graph generation."""
```

Run optimization: `arr.expr.simplify()` or `arr.optimize()`

### Fusion

Linear chains of blockwise operations fuse automatically:
```
a -> blockwise -> blockwise -> blockwise -> b
becomes:
a -> fused_blockwise -> b
```

Check fusion: `len(arr.optimize().__dask_graph__())`

## File Organization

Each operation in its own module:
- `_reshape.py` → `Reshape` class + `reshape()` function
- `_concatenate.py` → `Concatenate` class + `concatenate()` function

Subdirectories for groups:
- `manipulation/` - transpose, flip, roll
- `routines/` - diff, where, gradient
- `linalg/` - matrix operations
- `core/` - asarray, from_array

## Testing

For optimization tests, **prefer structural equality over value-only tests**.
See the `expr-optimization` skill for comprehensive testing guidance.

### Structural equality pattern

```python
def test_slice_pushes_through_elemwise():
    """(x + y)[:5] should optimize to x[:5] + y[:5]"""
    x = da.ones((100, 100), chunks=(10, 10))
    y = da.ones((100, 100), chunks=(10, 10))

    result = (x + y)[:5]
    expected = x[:5] + y[:5]

    # Key assertion: structural equality via ._name
    assert result.expr.simplify()._name == expected.expr.simplify()._name

    # Also verify value correctness
    assert_eq(result, expected)
```

**Why `.simplify()` on both sides?** The expected expression often contains
optimizable patterns too (e.g., slices into `from_array`).

### Value correctness tests

```python
def test_foo():
    x = np.random.random((10, 10))
    y = da.from_array(x, chunks=(5, 5))
    assert_eq(np.foo(x), da.foo(y))
```

`assert_eq` tests value equality plus structural checks on graphs.

## Common Tasks

### Add a new operation

1. Create expression class with `_parameters`, `chunks`, `_meta`, `_layer()`
2. Add function in same module
3. Export from `__init__.py`
4. Wire up in `_collection.py` if it's an Array method

### Debug expression issues

```python
arr.expr.pprint()              # View expression tree
arr.expr.simplify().pprint()   # View after optimization
arr.__dask_graph__()           # View generated tasks
```

### Add optimization

1. Identify pattern to optimize (e.g., `Slice(Transpose(...))`)
2. Add `_simplify_down()` method to parent expression
3. Test: verify `expr.simplify()._name` differs appropriately

## Anti-patterns

- **Don't** use `from_graph` except for interop with external graphs
- **Don't** create legacy Array objects and convert them
- **Don't** put function implementations in `_collection.py`
- **Don't** import expression modules at top-level (causes circular imports)

## Key Files

| File | Purpose |
|------|---------|
| `_expr.py` | Base `ArrayExpr` class |
| `_collection.py` | `Array` wrapper, method definitions |
| `_blockwise.py` | `Blockwise`, `Elemwise` base classes |
| `_reductions.py` | Reduction framework |
| `_slicing.py` | Getitem, setitem, slice expressions |
| `__init__.py` | Public API exports |
