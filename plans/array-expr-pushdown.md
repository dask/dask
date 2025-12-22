# Array Expression Pushdown Optimization Plan

**Design doc**: `designs/array-expr-pushdown.md`
**Related skills**: `expr-optimization`, `array-expr`

## Current State

### Slice Types

| Expression | Example | Pushdown Status | Notes |
|------------|---------|-----------------|-------|
| SliceSlicesIntegers | `x[5:10, :, 3]` | **Active** | Basic slices + integers, most optimizations here |
| SlicesWrapNone | `x[None, :, None]` | Disabled | Adds dimensions via None, blocks pushdown |
| VIndexArray | `x.vindex[[1,3], [2,4]]` | None | Fancy point-wise indexing |
| BooleanIndexFlattened | `x[bool_mask]` | None | Unknown output shape (nan chunks) |
| TakeUnknownOneChunk | `x[idx]` (unknown) | None | Integer array with unknown values |

### SliceSlicesIntegers Pushdown (the main one)

| Target Expression | Status | Notes |
|-------------------|--------|-------|
| Slice | Done | Fuses nested slices |
| Elemwise | Done | Pushes to each input with broadcasting |
| Transpose | Done | Reorders slice indices |
| Blockwise | Done | With conditions (new_axes, adjust_chunks) |
| PartialReduce | Done | Maps output slice to input |
| IO (FromArray) | Done | Pushes into source array |
| BroadcastTrick | Done | Creates new with sliced shape |
| Concatenate | Done | Selects/trims relevant arrays |
| Stack | Done | Selects subset, pushes to inputs |
| BroadcastTo | Done | Pushes to input where possible |
| Reshape | **Missing** | Complex, low value |

### Rechunk Pushdown (Rechunk._simplify_down)

| Target Expression | Status | Notes |
|-------------------|--------|-------|
| Rechunk | Done | Fuses consecutive rechunks |
| Transpose | Done | Reorders chunk spec |
| Elemwise | Done | Pushes to each input |
| IO | Done | Modifies IO chunks directly |
| Concatenate | **Missing** | Medium value |

### Other Pushdowns

| Pattern | Status | Notes |
|---------|--------|-------|
| Transpose(Transpose) | Done | Composes axes |
| Transpose(Elemwise) | **Missing** | Low complexity, enables fusion |

---

## Phase 1: Complete Slice Pushdown for Common Operations ✓

**Status**: Complete

### 1.1 Slice through Concatenate ✓

Implemented in `slicing/_basic.py:_pushdown_through_concatenate()`

- Slice entirely within first input -> returns sliced first input
- Slice spans multiple inputs -> slices relevant inputs
- Slice on non-concat axis -> pushes to all inputs

### 1.2 Slice through Stack ✓

Implemented in `slicing/_basic.py:_pushdown_through_stack()`

- Slice on stacked axis -> selects subset of inputs
- Slice on other axes -> pushes to all inputs

### 1.3 Slice through BroadcastTo ✓

Implemented in `slicing/_basic.py:_pushdown_through_broadcast_to()`

- Dimensions added by broadcast -> affects output shape only
- Dimensions from input with size > 1 -> pushes slice to input
- Dimensions from input with size == 1 -> affects output shape only

---

## Phase 2: Rechunk Pushdown Extensions

### 2.1 Rechunk through Concatenate

**Goal**: `rechunk(concat([a, b]))` -> `concat([rechunk(a), rechunk(b)])`

May enable better chunk alignment before concatenation.

---

## Phase 3: Cross-Operation Optimizations

### 3.1 Transpose through Elemwise

**Goal**: `(x + y).T` -> `x.T + y.T`

Enables further fusion opportunities when transposed arrays are used together.

```python
def test_transpose_pushes_through_elemwise():
    x = da.ones((10, 5), chunks=5)
    y = da.ones((10, 5), chunks=5)
    result = (x + y).T.expr.simplify()
    expected = (x.T + y.T).expr.simplify()
    assert result._name == expected._name
```

---

## Phase 4: Advanced Slice Types

These are particularly important for xarray integration where fancy indexing is common.

### 4.1 SlicesWrapNone Pushdown

**Goal**: Enable pushdown for `x[None, :5, None]` -> `x[:5][None, :, None]`

**Current state**: `_simplify_down` explicitly disabled (returns None)

**Approach**: Separate the None-insertion from the slicing, push slicing through

### 4.2 VIndexArray Pushdown

**Goal**: `x.vindex[[1,3], [2,4]]` through elemwise operations

**Expression**: `VIndexArray` in `slicing/_vindex.py`

**Challenges**:
- Point-wise indexing (not rectangular selection)
- Output shape from index arrays, not input shape
- Need to apply same indices to all elemwise inputs

**Potential**: `(x + y).vindex[idx]` -> `x.vindex[idx] + y.vindex[idx]` when shapes align

### 4.3 BooleanIndexFlattened Pushdown

**Goal**: `x[mask]` through elemwise when mask applies element-wise

**Expression**: `BooleanIndexFlattened` in `slicing/_bool_index.py`

**Challenges**:
- Output has unknown size (nan chunks)
- Mask must apply to all inputs identically
- Shape changes unpredictably

**Limited potential**: Only safe when same mask applies to all inputs

### 4.4 Integer Array Indexing (take)

**Goal**: `x[[1,3,5]]` along one axis

**Challenges**:
- Reorders data (not just selection)
- May duplicate elements
- Different semantics than slicing

### 4.5 Abstract Projection Interface (Investigation)

Consider whether a unified interface would help:

```python
class Projection(ArrayExpr):
    """Base for operations that select/reorder elements."""

    def can_push_through(self, expr) -> bool:
        """Whether this projection can push through expr."""

    def push_through(self, expr) -> Expr:
        """Return equivalent expression with projection pushed down."""
```

Current slice types have different semantics:
- SliceSlicesIntegers: rectangular selection, preserves order
- VIndexArray: point selection, arbitrary order
- BooleanIndexFlattened: mask selection, preserves order, unknown size

A common interface could standardize pushdown logic while respecting these differences.

---

## Implementation Notes

### Skills Reference

- **`expr-optimization`**: Testing patterns, `_simplify_down` methodology, TDD approach
- **`array-expr`**: Expression class anatomy, Blockwise patterns, debugging (`pprint()`, `__dask_graph__()`)

### Testing Pattern (from expr-optimization skill)

```python
# Structure test - verify optimization applied
assert result.expr.simplify()._name == expected.expr.simplify()._name

# Task count test - verify work reduced
assert len(sliced.optimize().__dask_graph__()) < len(full.optimize().__dask_graph__())

# Correctness test - verify results match
assert_eq(result, numpy_equivalent)
```

### Using substitute_parameters

When modifying expressions, use `substitute_parameters` to preserve subclass types:

```python
# Instead of constructing manually
sliced_input = new_collection(concat.arrays[0])[slice_]
result = concat.substitute_parameters({"arrays": (sliced_input.expr,)})
```

---

## Priority Order

**Phase 1-3 (Basic slice pushdown)**:
1. **Slice(Concatenate)** - High value, common pattern
2. **Slice(Stack)** - Similar to concat, common in data loading
3. **Slice(BroadcastTo)** - Medium value
4. **Rechunk(Concatenate)** - Medium value
5. **Transpose(Elemwise)** - Low complexity, nice to have

**Phase 4 (Advanced slice types)** - Important for xarray:
6. **SlicesWrapNone pushdown** - Enable None-indexing optimization
7. **VIndexArray(Elemwise)** - Fancy indexing through elemwise
8. **Abstract Projection interface** - Investigation/design work

---

## Files to Modify

| Task | Files |
|------|-------|
| Slice(Concatenate) | `slicing/_basic.py`, `tests/test_slice_pushdown.py` |
| Slice(Stack) | `slicing/_basic.py`, `tests/test_slice_pushdown.py` |
| Slice(BroadcastTo) | `slicing/_basic.py`, `tests/test_slice_pushdown.py` |
| Transpose(Elemwise) | `manipulation/_transpose.py`, new test file |
| SlicesWrapNone pushdown | `slicing/_basic.py` |
| VIndexArray pushdown | `slicing/_vindex.py` |
| BooleanIndexFlattened pushdown | `slicing/_bool_index.py` |
