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
| pad | concatenate + broadcast_to + block (all modes) | **Done** |
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
| argtopk/topk | partial sort | **Done** |
| apply_along_axis | axis application | **Done** |
| apply_over_axes | multiple axes | **Done** |
| insert/delete | array modification | **Done** |
| union1d | set operations | **Done** |
| ediff1d | differences | **Done** |
| ravel_multi_index | index conversion | **Done** |
| unravel_index | index conversion | **Done** |

### Tier 12: UFunc Advanced Features
Advanced ufunc parameters that require special handling.

| Feature | Issue | Tests | Status |
|---------|-------|-------|--------|
| `where` parameter | Array masks not applied correctly | 28 xfails | Not started |
| `out` parameter | Shape mismatch with broadcasting | 8 xfails | Not started |
| `frompyfunc` | Can't tokenize vectorized ufuncs | 1 xfail | Not started |

Notes:
- `where=True` (trivial case) works; actual array masks fail in compute path
- `out=` is an imperative concept that doesn't fit expression model well
  - Currently handled at collection level via `_handle_out` which replaces target's expr
  - Fails when combined with `where` because masked elements need original `out` values
- `frompyfunc` creates ufuncs that can't be deterministically tokenized
- These require deeper architectural changes and are low priority

### Tier 13: Reduction Features (Pending)
Additional reduction functionality needed for full compatibility.

| Feature | Tests Blocked | Notes | Status |
|---------|---------------|-------|--------|
| out= parameter | 4 | Output array pre-allocation for reductions/ufuncs | Not started |
| weights parameter | 1 | Weighted reductions in `da.reduction()` | Not started |
| compute_chunk_sizes() | 4 | Method to compute unknown chunk sizes after boolean indexing | Not started |
| cumulative axis=None | 16 | Cumulative reductions with axis=None (HLG dependency issues) | Not started |

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

### Current Test Status (December 2025)
- **3645 passed**, 211 xfailed, 24 xpassed, 612 skipped
- 3 flaky failures (intermittent, likely timing-related)

**XFails by file:**
- `test_array_core.py`: 76 xfails
- `test_ufunc.py`: 39 xfails
- `test_reductions.py`: 25 xfails
- `test_routines.py`: 21 xfails
- `test_array_function.py`: 15 xfails
- `test_slicing.py`: 14 xfails
- `test_creation.py`: 9 xfails
- Other files: ~12 xfails combined

## Remaining Work Streams (Parallelizable)

These work streams can be executed in parallel by agents. Each is independent.

**Priority Guide:**
- 🟢 Quick Win - Simple, low-risk
- 🟡 Medium - Moderate complexity
- 🔴 Complex - Architectural changes needed

### Stream A: Cleanup XPASSed Tests (24 tests) 🟢 **DONE**
Converted blanket xfail markers to targeted ones for passing variants.

| Test File | Tests | Status |
|-----------|-------|--------|
| test_creation.py | 3 | ✅ test_like_forgets_graph[array/asarray/asanyarray] now pass |
| test_slicing.py | 17 | ✅ test_index_with_int_dask_array (17 of 20 variants pass) |
| test_ufunc.py | 4 | ✅ test_ufunc_where with where=True and dtype=None pass |

### Stream B: compute_chunk_sizes() (9 tests) 🟡 ✅ DONE
Method to compute unknown chunk sizes after boolean indexing.

| Tests | Notes |
|-------|-------|
| test_compute_chunk_sizes | Basic implementation |
| test_compute_chunk_sizes_2d_array | 2D arrays |
| test_compute_chunk_sizes_3d_array | 3D arrays |
| test_compute_chunk_sizes_warning_fixes_* | 6 warning tests |

**Implementation:** Added `compute_chunk_sizes()` method to Array class in `_collection.py`. Uses `map_blocks` to get chunk shapes, computes them, then wraps the expression with `ChunksOverride` to set the new chunks.

### Stream C: Cumulative Reduction axis=None (16 tests) 🔴
cumsum/cumprod/nancumsum/nancumprod with axis=None.

| Functions | Issue |
|-----------|-------|
| cumsum, cumprod, nancumsum, nancumprod | axis=None requires flatten + cumulative, then reshape |

**Notes:** Currently fails due to HLG dependency issues when combining flatten with cumulative operations.

### Stream D: UFunc where Parameter 🟢 **DONE**
Array masks for ufunc `where=` parameter.

| Status | Tests |
|--------|-------|
| ✅ Fixed | 26 tests now pass (all `dtype=None` combinations) |
| ⏳ Remaining | 16 tests fail due to `dtype` parameter issue (separate from `where`) |

**Implementation:** Added `out` parameter to `Elemwise` class, updated `args` property to include both `where` and `out` when `where` is not True. Fixed meta computation to include `where` and `out` args. Updated `_pushdown_through_elemwise` to rechunk `where` and `out` arrays.

### Stream E: out= Parameter (4 tests) 🔴
Output array pre-allocation for reductions.

| Category | Tests | Notes |
|----------|-------|-------|
| Reduction out= | 4 | test_array_reduction_out, test_array_cumreduction_out |

**Notes:** Elemwise `out=` now works via `_handle_out` at collection level. Reduction `out=` still pending.

### Stream F: setitem (7 tests) 🔴
`__setitem__` implementation for array assignment.

| Tests | Notes |
|-------|-------|
| test_setitem_masked | Masked assignment |
| test_setitem_extended_API_2d_* | 2D setitem variants |
| test_setitem_errs | Error handling |
| test_setitem_bool_index_errs | Boolean index errors |

**Notes:** setitem is fundamentally imperative. Needs to create new expression with updated values.

### Stream G: Histogram Delayed Inputs (20 tests) 🟡
Histogram with delayed range and bins.

| Tests | Notes |
|-------|-------|
| test_histogram_delayed_range | 16 tests - delayed range parameter |
| test_histogram_delayed_bins | 4 tests - delayed bins parameter |

**Implementation:** Handle Delayed objects in histogram range/bins by computing them first.

### Stream H: register_chunk_type (4+ tests) 🔴
Custom chunk type registration for dispatching.

| Tests | Notes |
|-------|-------|
| test_dispatch.py (entire module) | Module-level skip |
| test_binary_function_type_precedence | 4 tests in test_array_function.py |

**Implementation:** Add `register_chunk_type()` function and type precedence logic.

### Stream I: Empty Chunk nanmin/nanmax (4 tests) 🟡
Handle empty chunks in nanmin/nanmax.

| Tests | Notes |
|-------|-------|
| test_empty_chunk_nanmin_nanmax | 2 tests |
| test_empty_chunk_nanmin_nanmax_raise | 2 tests |

**Implementation:** Propagate warnings and handle edge cases for empty chunks.

### Stream J: map_blocks Enhancements (5 tests) 🟡
Various map_blocks improvements.

| Tests | Notes |
|-------|-------|
| test_map_blocks_delayed | Delayed inputs |
| test_map_blocks_large_inputs_delayed | Large inputs as delayed |
| test_map_blocks_custom_name | Custom naming |
| test_map_blocks_unique_name_enforce_dim | Unique naming |
| test_map_blocks_dataframe | DataFrame output |

### Stream K: Single Chunk Compute Behavior (10 tests) 🟡
Single chunk arrays returning references vs copies.

| Tests | Notes |
|-------|-------|
| test_array_picklable | 2 tests |
| Various core tests | 8 tests |

**Notes:** When array has single chunk, compute may return reference to underlying data.

### Stream L: Miscellaneous (20+ tests) 🟢
Smaller independent fixes.

| Category | Tests | Notes |
|----------|-------|-------|
| Warning behavior | 5 | Warning messages differ |
| Graph structure | 5 | Graph serialization differs |
| Naming patterns | 3 | Name patterns differ |
| API differences | 4 | Error messages, etc. |
| Fusion | 3 | blockwise_fusion, block_id fusion |

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
