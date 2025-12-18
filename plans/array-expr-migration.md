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
- Linear algebra: tensordot, matmul, dot, vdot, outer, trace, einsum
- Reductions: argmin, argmax, nanargmin, nanargmax, cumsum, cumprod, nancumsum, nancumprod
- Shape manipulation: ravel, flatten, expand_dims, atleast_*d, broadcast_to, roll
- Routines: diff, gradient, compress, searchsorted
- Stacking: vstack, hstack, dstack, block
- Axis manipulation: flip, flipud, fliplr, rot90, transpose, swapaxes, moveaxis, rollaxis
- Simple routines: round/around, isclose, allclose, isnull/notnull, append, count_nonzero
- Utilities: ndim, shape, result_type, broadcast_arrays, unify_chunks
- IO: store, to_npy_stack, from_npy_stack, from_delayed
- Advanced indexing: vindex, take, nonzero, argwhere, flatnonzero
- Creation: eye, diag, diagonal, tri, tril, triu, fromfunction, indices, meshgrid, pad, tile
- Statistics: histogram, histogram2d, histogramdd, digitize, bincount, cov, corrcoef, average
- Selection & conditional: select, piecewise, choose, extract, isin
- FFT: fft, ifft, fft2, ifft2, fftn, ifftn, rfft, irfft, rfft2, irfft2, rfftn, irfftn, hfft, ihfft, fftfreq, rfftfreq, fftshift, ifftshift, fft_wrap
- Linalg submodule: tsqr, qr, svd, svd_flip, norm

## Testing Infrastructure

Test modules use whitelist approach (individual xfails rather than module-level skips). Each xfailed test represents work to be done. Decreasing xfails is progress.

Exception: `test_dispatch.py` has a module-level skip due to `register_chunk_type` not being implemented.

## Priority Tiers (Revised)

### Tier 1: Quick Wins - Stacking & Axis Manipulation
These are simple wrappers around existing infrastructure. High test-unlocking ratio.

| Operation | Impl Strategy | Tests Blocked | Status |
|-----------|--------------|---------------|--------|
| vstack | `atleast_2d` + `concatenate` | 3+ | **Done** |
| hstack | `atleast_1d`/`2d` + `concatenate` | 3+ | **Done** |
| dstack | `atleast_3d` + `concatenate` | 3+ | **Done** |
| flip | slicing with `[::-1]` | 3+ | **Done** |
| flipud | `flip(m, 0)` | 1 | **Done** |
| fliplr | `flip(m, 1)` | 1 | **Done** |
| rot90 | flip + transpose | 2+ | **Done** |
| swapaxes | transpose | 2+ | **Done** (xfail: naming) |
| transpose (func) | method wrapper | 2+ | **Done** |
| moveaxis | transpose | 3+ | **Done** |
| rollaxis | transpose | 2+ | **Done** |

### Tier 2: Simple Routines
Straightforward implementations using existing blockwise/elemwise.

| Operation | Impl Strategy | Tests Blocked | Status |
|-----------|--------------|---------------|--------|
| round/around | elemwise | 2+ | **Done** |
| isclose | elemwise | 2+ | **Done** |
| allclose | reduction of isclose | 1 | **Done** |
| isnull/notnull | elemwise | 3+ | **Done** |
| append | concatenate wrapper | 2+ | **Done** |
| count_nonzero | reduction | 5+ | **Done** |
| shape (func) | property access | 1 | **Done** |
| ndim (func) | property access | 1 | **Done** |
| result_type | metadata only | 1 | **Done** |
| broadcast_arrays | unify_chunks + broadcast_to | 3+ | **Done** |
| unify_chunks | already exists internally | 1 | **Done** |

### Tier 3: Block Assembly (~27 tests)
Critical for array construction patterns.

| Operation | Location | Notes | Status |
|-----------|----------|-------|--------|
| block | _collection.py | Recursive nested lists -> concatenate | **Done** |

### Tier 4: Store & IO (~12+ tests)
Essential for practical use - saving results.

| Operation | Location | Notes | Status |
|-----------|----------|-------|--------|
| store | _io.py | Write chunks to array-like targets | **Done** (basic case; complex return_stored cases pending) |
| to_npy_stack | _io.py | Save to numpy files | **Done** |
| from_npy_stack | _io.py | Load from numpy files | **Done** |
| from_delayed | _io.py | Create from delayed objects | **Done** |

### Tier 5: Advanced Indexing (~8 tests)
Complete the indexing story.

| Operation | Location | Notes | Status |
|-----------|----------|-------|--------|
| vindex | _slicing.py | Point-wise vectorized indexing | **Done** |
| take | _routines.py | Index along axis | **Done** |
| nonzero | _routines.py | Returns indices of non-zero elements | **Done** |
| argwhere | _routines.py | Returns indices where condition is true | **Done** |
| flatnonzero | _routines.py | nonzero on flattened array | **Done** |

### Tier 6: Creation Functions
Array construction.

| Operation | Impl Strategy | Status |
|-----------|--------------|--------|
| eye | Eye expression class | **Done** |
| diag | Diag1D/Diag2DSimple expressions | **Done** |
| diagonal | Diagonal expression class | **Done** |
| tri | arange + reshape + greater_equal | **Done** |
| tril/triu | tri + where | **Done** |
| fromfunction | meshgrid + blockwise | **Done** |
| indices | arange + meshgrid + stack | **Done** |
| meshgrid | asarray + slicing + broadcast_arrays | **Done** |
| pad | concatenate + broadcast_to (constant/edge/linear_ramp/empty modes) | **Done** (stat modes pending) |
| tile | block | **Done** |

### Tier 7: Statistics & Histograms
Used in data analysis workflows.

| Operation | Notes | Status |
|-----------|-------|--------|
| histogram | binning | **Done** |
| histogram2d | 2D binning | **Done** |
| histogramdd | N-D binning | **Done** |
| bincount | counting | **Done** |
| unique | deduplication | **Done** |
| cov | covariance | **Done** |
| corrcoef | correlation | **Done** |
| digitize | bin indices | **Done** |

### Tier 8: Selection & Conditional
Conditional operations.

| Operation | Notes | Status |
|-----------|-------|--------|
| select | multi-condition select | **Done** |
| piecewise | piecewise functions | **Done** |
| choose | index-based selection | **Done** |
| extract | condition-based extraction | **Done** |
| isin | membership test | **Done** |

### Tier 9: Linear Algebra Extensions
Beyond basic linalg.

| Operation | Notes | Status |
|-----------|-------|--------|
| einsum | Einstein summation | **Done** |
| outer | outer product | **Done** |
| trace | diagonal sum | **Done** |
| tril_indices | triangle indices | **Done** |
| triu_indices | triangle indices | **Done** |

### Tier 10: Submodules (Large Scope)
Full submodule implementations.

| Submodule | Notes | Status |
|-----------|-------|--------|
| fft | FFT operations | **Done** |
| linalg | Full linalg submodule | **Done** (330/391 tests pass, 61 skipped) |
| ma | Masked arrays | Not started |

#### Linalg Status Detail
Native expression classes for TSQR algorithm (tall-skinny QR) and derived operations.

| Operation | Status |
|-----------|--------|
| tsqr | **Done** |
| qr | **Done** |
| svd | **Done** |
| svd_flip | **Done** |
| norm | **Done** |
| tensordot | **Done** (in _linalg.py) |
| dot | **Done** (in _linalg.py) |
| vdot | **Done** (in _linalg.py) |
| matmul | **Done** (in _linalg.py) |
| lu | **Done** |
| solve | **Done** |
| solve_triangular | **Done** |
| inv | **Done** |
| cholesky | **Done** |
| lstsq | **Done** |
| svd_compressed | **Done** |

### Tier 11: Advanced/Specialized
Lower priority operations.

| Operation | Notes | Status |
|-----------|-------|--------|
| coarsen | downsampling | **Done** |
| argtopk/topk | partial sort | Blocked (needs output_size in reduction()) |
| apply_along_axis | axis application | **Done** |
| apply_over_axes | multiple axes | **Done** |
| insert/delete | array modification | Not started |
| union1d | set operations | Blocked (needs unique) |
| ediff1d | differences | **Done** |
| ravel_multi_index | index conversion | **Done** |
| unravel_index | index conversion | **Done** |

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
- [x] vstack, hstack, dstack (1 function each, ~10 lines total)
- [x] flip family (flip, flipud, fliplr) (~20 lines)
- [x] rot90 (~15 lines)
- [x] axis manipulation (swapaxes, transpose, moveaxis, rollaxis) (~30 lines)
- [x] round/around, isclose, allclose (~15 lines)
- [x] isnull/notnull (~10 lines)
- [x] broadcast_arrays, unify_chunks (~20 lines)
- [x] shape, ndim, result_type (~10 lines)
- [x] append (~5 lines)
- [x] count_nonzero (~20 lines)

Tier 1-2 complete.

## References

- DataFrame expr: `dask/dataframe/dask_expr/`
- Base expr: `dask/_expr.py`
- Existing array-expr: `dask/array/_array_expr/`
- Traditional array: `dask/array/core.py`, `reductions.py`, `routines.py`, etc.
