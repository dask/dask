# Array Expression Migration Plan

Design doc: `/designs/array-expr.md`
Skill: `.claude/skills/array-expr-migration/SKILL.md`

## Current Status

The array-expr system has foundational infrastructure in place:
- ArrayExpr base class with singleton pattern
- Array collection wrapper
- Blockwise/Elemwise operations
- Basic reductions (sum, mean, min, max, prod, any, all)
- Slicing and fancy indexing
- Field access for structured arrays
- Creation functions (arange, ones, zeros, etc.)
- Stack/concatenate
- Rechunking (tasks method)
- Map blocks/overlap
- Random number generation
- UFuncs
- Reshape, squeeze, transpose
- __array_function__ protocol
- Linear algebra: tensordot, matmul, dot, vdot
- Reductions: argmin, argmax, nanargmin, nanargmax, cumsum, cumprod, nancumsum, nancumprod
- Shape manipulation: ravel, flatten, expand_dims, atleast_*d, broadcast_to, roll
- Routines: diff, gradient, compress, searchsorted

## Testing Infrastructure

Test modules use whitelist approach (individual xfails rather than module-level skips). Each xfailed test represents work to be done. Decreasing xfails is progress.

Exception: `test_dispatch.py` has a module-level skip due to `register_chunk_type` not being implemented.

## Priority Tiers (Revised)

### Tier 1: Quick Wins - Stacking & Axis Manipulation
These are simple wrappers around existing infrastructure. High test-unlocking ratio.

| Operation | Impl Strategy | Tests Blocked | Status |
|-----------|--------------|---------------|--------|
| vstack | `atleast_2d` + `concatenate` | 3+ | Not started |
| hstack | `atleast_1d`/`2d` + `concatenate` | 3+ | Not started |
| dstack | `atleast_3d` + `concatenate` | 3+ | Not started |
| flip | slicing with `[::-1]` | 3+ | Not started |
| flipud | `flip(m, 0)` | 1 | Not started |
| fliplr | `flip(m, 1)` | 1 | Not started |
| rot90 | flip + transpose | 2+ | Not started |
| swapaxes | blockwise | 2+ | Not started |
| transpose (func) | blockwise | 2+ | Not started |
| moveaxis | transpose | 3+ | Not started |
| rollaxis | transpose | 2+ | Not started |

### Tier 2: Simple Routines
Straightforward implementations using existing blockwise/elemwise.

| Operation | Impl Strategy | Tests Blocked | Status |
|-----------|--------------|---------------|--------|
| round/around | elemwise | 2+ | Not started |
| isclose | elemwise | 2+ | Not started |
| allclose | reduction of isclose | 1 | Not started |
| isnull/notnull | elemwise | 3+ | Not started |
| append | concatenate wrapper | 2+ | Not started |
| count_nonzero | reduction | 5+ | Not started |
| shape (func) | property access | 1 | Not started |
| ndim (func) | property access | 1 | Not started |
| result_type | metadata only | 1 | Not started |
| broadcast_arrays | unify_chunks + broadcast_to | 3+ | Not started |
| unify_chunks | already exists internally | 1 | Not started |

### Tier 3: Block Assembly (~27 tests)
Critical for array construction patterns.

| Operation | Location | Notes | Status |
|-----------|----------|-------|--------|
| block | creation.py | Recursive nested lists -> concatenate | Not started |

### Tier 4: Store & IO (~12+ tests)
Essential for practical use - saving results.

| Operation | Location | Notes | Status |
|-----------|----------|-------|--------|
| store | _io.py | Write chunks to array-like targets | Not started |
| to_npy_stack | _io.py | Save to numpy files | Not started |
| from_npy_stack | _io.py | Load from numpy files | Not started |
| from_delayed | _io.py | Create from delayed objects | Not started |

### Tier 5: Advanced Indexing (~8 tests)
Complete the indexing story.

| Operation | Location | Notes | Status |
|-----------|----------|-------|--------|
| vindex | _slicing.py | Point-wise vectorized indexing | Not started |
| take | _slicing.py | Index along axis | Not started |
| nonzero | _routines.py | Returns indices of non-zero elements | Not started |
| argwhere | _routines.py | Returns indices where condition is true | Not started |
| flatnonzero | _routines.py | nonzero on flattened array | Not started |

### Tier 6: Creation Functions
Array construction.

| Operation | Impl Strategy | Status |
|-----------|--------------|--------|
| eye | diagonal ones | Not started |
| diag | extract/construct diagonal | Not started |
| diagonal | extract diagonal | Not started |
| tri | lower triangle mask | Not started |
| tril/triu | mask operations | Not started |
| fromfunction | map_blocks | Not started |
| indices | similar to meshgrid | Not started |
| meshgrid | stack + broadcast | Not started |
| pad | boundary handling | Not started |
| tile | repeat + reshape | Not started |

### Tier 7: Statistics & Histograms
Used in data analysis workflows.

| Operation | Notes | Status |
|-----------|-------|--------|
| histogram | binning | Not started |
| histogram2d | 2D binning | Not started |
| histogramdd | N-D binning | Not started |
| bincount | counting | Not started |
| unique | deduplication | Not started |
| cov | covariance | Not started |
| corrcoef | correlation | Not started |
| digitize | bin indices | Not started |

### Tier 8: Selection & Conditional
Conditional operations.

| Operation | Notes | Status |
|-----------|-------|--------|
| select | multi-condition select | Not started |
| piecewise | piecewise functions | Not started |
| choose | index-based selection | Not started |
| extract | condition-based extraction | Not started |
| isin | membership test | Not started |

### Tier 9: Linear Algebra Extensions
Beyond basic linalg.

| Operation | Notes | Status |
|-----------|-------|--------|
| einsum | Einstein summation | Not started |
| outer | outer product | Not started |
| trace | diagonal sum | Not started |
| tril_indices | triangle indices | Not started |
| triu_indices | triangle indices | Not started |

### Tier 10: Submodules (Large Scope)
Full submodule implementations.

| Submodule | Notes | Status |
|-----------|-------|--------|
| fft | FFT operations | Not started |
| linalg | Full linalg submodule | Not started |
| ma | Masked arrays | Not started |

### Tier 11: Advanced/Specialized
Lower priority operations.

| Operation | Notes | Status |
|-----------|-------|--------|
| coarsen | downsampling | Not started |
| argtopk/topk | partial sort | Not started |
| apply_along_axis | axis application | Not started |
| apply_over_axes | multiple axes | Not started |
| insert/delete | array modification | Not started |
| union1d | set operations | Not started |
| ediff1d | differences | Not started |
| ravel_multi_index | index conversion | Not started |
| unravel_index | index conversion | Not started |

### Zarr/TileDB IO (Separate Track)
External format support.

| Operation | Notes | Status |
|-----------|-------|--------|
| to_zarr | Zarr output | Not started |
| from_zarr | Zarr input | Not started |
| to_tiledb | TileDB output | Not started |
| from_tiledb | TileDB input | Not started |

## Implementation Notes

### Tier 1-2 Pattern (Simple Wrappers)
These operations typically just need to be added to `_collection.py` or `_routines.py`:
```python
def vstack(tup, allow_unknown_chunksizes=False):
    tup = tuple(atleast_2d(x) for x in tup)
    return concatenate(tup, axis=0, allow_unknown_chunksizes=allow_unknown_chunksizes)
```

### Tier 3-4 Pattern (New Expression Classes)
Create expression class with `_layer()` method in appropriate `_*.py` file.

### Testing Strategy

```bash
# Test array-expr specifically
DASK_ARRAY__QUERY_PLANNING=True pytest dask/array/tests/

# Quick check - stop on first failure
DASK_ARRAY__QUERY_PLANNING=True pytest -k {operation} -x

# Find xfail markers
grep -n "xfail.*_array_expr" dask/array/tests/*.py
```

### Current Test Failure Summary
- `test_array_core.py`: ~157 xfails (block: 27, store: 12, vindex: 7, other: 111)
- `test_routines.py`: ~115 xfails
- `test_array_function.py`: ~30 xfails
- `test_overlap.py`: ~5 xfails
- `test_gufunc.py`: ~1 xfail

## Migration Workflow

1. **Test Discovery**: Find tests for target operation
2. **Study Traditional Implementation**: Understand chunking, metadata logic
3. **Implement**: Add to array-expr (function or expression class)
4. **Wire API**: Export from `__init__.py`
5. **Remove xfails**: Clean up test markers
6. **Update this plan**: Mark status

## Quick Wins Checklist

Estimated effort for Tier 1-2 (should be quick):
- [ ] vstack, hstack, dstack (1 function each, ~10 lines total)
- [ ] flip family (flip, flipud, fliplr) (~20 lines)
- [ ] rot90 (~15 lines)
- [ ] axis manipulation (swapaxes, transpose, moveaxis, rollaxis) (~30 lines)
- [ ] round/around, isclose, allclose (~15 lines)
- [ ] isnull/notnull (~10 lines)
- [ ] broadcast_arrays, unify_chunks (~20 lines)
- [ ] shape, ndim, result_type (~10 lines)
- [ ] append (~5 lines)
- [ ] count_nonzero (~20 lines)

Total Tier 1-2: ~155 lines of mostly simple wrapper code, unlocking 30+ tests.

## References

- DataFrame expr: `dask/dataframe/dask_expr/`
- Base expr: `dask/_expr.py`
- Existing array-expr: `dask/array/_array_expr/`
- Traditional array: `dask/array/core.py`, `reductions.py`, `routines.py`, etc.
