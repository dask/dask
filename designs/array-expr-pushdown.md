# Array Expression Pushdown Optimization

## Motivation

Many array operations produce data that is immediately filtered or sliced. Pushing
these filters toward IO/creation expressions reduces work - we compute only what's
needed rather than computing everything then discarding.

```python
# Without pushdown: compute full sum, then slice
x.sum(axis=0)[:5]  # All 100 column chunks processed

# With pushdown: slice first, compute less
x[:, :5].sum(axis=0)  # Only 1 column chunk processed
```

## Core Principle: Push Toward Leaves

Operations like `Slice` and `Rechunk` should travel down the expression tree toward
IO/creation nodes. Each expression type defines how to let these operations pass
through (or blocks them when semantically unsafe).

```
User writes:           After pushdown:
Slice                  Elemwise
  |                      / \
Elemwise    -->     Slice  Slice
  / \                 |      |
 A   B               A      B
```

## Pushdown Candidates

**Slice** - Most valuable, reduces data volume
**Rechunk** - Aligns chunk boundaries for efficient access

## Expression Categories for Pushdown

1. **Transparent**: Operation passes through unchanged (Elemwise, Transpose)
2. **Transforming**: Operation reorders/remaps indices (Transpose remaps slice axes)
3. **Blocking**: Cannot push through (operations with `new_axes`, `adjust_chunks`)
4. **Terminal**: Absorbs the operation (IO, BroadcastTrick)

## Slice Type Hierarchy

Different indexing patterns have different pushdown semantics:

| Type | Example | Pushdown Complexity |
|------|---------|---------------------|
| Basic slices | `x[5:10, ::2]` | Simple - preserves alignment |
| Integer indices | `x[5, :]` | Simple - reduces dimensions |
| Boolean arrays | `x[mask]` | Complex - unknown output shape |
| Fancy indexing | `x[[1,3,5], :]` | Complex - reorders data |

Basic slices push most easily. Boolean/fancy indexing may need special handling
or may not push at all.

## Testing Strategy

1. **Expression structure tests**: Verify pushdown happens via `_name` comparison
2. **Task count tests**: Confirm fewer tasks after optimization
3. **Correctness tests**: Results match NumPy (always)

```python
def test_slice_pushes_through_elemwise():
    result = (x + y)[:5].expr.simplify()
    expected = (x[:5] + y[:5]).expr.simplify()
    assert result._name == expected._name  # Structure test

def test_slice_reduces_tasks():
    assert len(sliced.optimize().__dask_graph__()) < len(full.optimize().__dask_graph__())
```

## Constraints

- Pushdown must preserve correctness (results identical to no pushdown)
- Some operations legitimately block pushdown (new_axes, adjust_chunks on sliced dims)
- Unknown chunks (`nan`) may prevent some optimizations
