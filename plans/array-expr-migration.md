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
- Masked arrays (ma): masked_equal, masked_greater, masked_less, masked_where, filled, count, etc.

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
| ma | Masked arrays | **Done** (127/139 tests pass, 6 xfails for tensordot/average edge cases) |

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
- **3868 passed**, 81 xfailed, 1 failed, 611 skipped
- Significant progress from earlier (was 211 xfails)

**XFails by category (81 total):**
- ~~UFunc dtype parameter: 16 xfails~~ **DONE**
- Store advanced features: 6 xfails
- Unknown chunks handling: 6 xfails
- from_array features: 6 xfails
- Graph construction: 5 xfails
- Fusion (architectural): 4 xfails
- Boolean mask unknown shapes: 4 xfails
- Integer dask array indexing: 4 xfails
- Setitem edge cases: 3 xfails
- Rechunk auto: 2 xfails
- Random broadcasting: 2 xfails
- Array pickling: 2 xfails
- Misc single tests: ~37 xfails

## Remaining Work Streams (Parallelizable)

These work streams can be executed in parallel by agents. Each is independent.

**Priority Guide:**
- 🟢 Quick Win - Simple, low-risk
- 🟡 Medium - Moderate complexity
- 🔴 Complex - Architectural changes needed

### Priority Summary for New Streams

| Stream | Tests | Priority | Notes |
|--------|-------|----------|-------|
| ~~N: UFunc dtype~~ | ~~16~~ | ~~🟡 High~~ | **DONE** |
| M: Store advanced | 6 | 🟡 High | Practical importance |
| ~~P: Int dask indexing~~ | ~~4~~ | ~~🟢 Medium~~ | **DONE** (20/20 pass, 1 xfail for dict constructor) |
| Q: from_array features | 6 | 🟡 Medium | API completeness |
| O: Unknown chunks | 6 | 🟡 Medium | Edge case handling |
| S: Setitem edge | 3 | 🟡 Medium | Nearly complete (67/68 pass) |
| R: Boolean mask unknown | 4 | 🟡 Medium | Depends on unknown chunks |
| X: Rechunk auto | 2 | 🟢 Low | May be test adjustment |
| U: Array pickling | 2 | 🟢 Low | May be test pattern |
| Y: Random broadcast | 2 | 🟡 Low | Niche use case |
| W: Blockwise concat | 1 | 🟡 Low | Niche use case |
| T: Graph construction | 5 | 🔴 Low | Different paradigm |
| V: Fusion | 4 | 🔴 Deferred | Architectural change |
| Z: Misc | ~37 | 🟢 Varies | Mixed bag |

**Recommended next targets:** Stream M (store completeness), Stream Q (from_array features), Stream S (setitem edge cases)

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

### Stream C: Cumulative Reduction axis=None (16 tests) 🟢 **DONE**
cumsum/cumprod/nancumsum/nancumprod with axis=None.

| Functions | Status |
|-----------|--------|
| cumsum, cumprod, nancumsum, nancumprod | **Done** - Native expression classes in `_reductions.py` |

**Implementation:** Created `CumReduction` and `CumReductionBlelloch` expression classes that handle axis=None by flattening and rechunking first. Functions exported from `_array_expr/__init__.py`.

### Stream D: UFunc where Parameter 🟢 **DONE**
Array masks for ufunc `where=` parameter.

| Status | Tests |
|--------|-------|
| ✅ Fixed | 26 tests now pass (all `dtype=None` combinations) |
| ⏳ Remaining | 16 tests fail due to `dtype` parameter issue (separate from `where`) |

**Implementation:** Added `out` parameter to `Elemwise` class, updated `args` property to include both `where` and `out` when `where` is not True. Fixed meta computation to include `where` and `out` args. Updated `_pushdown_through_elemwise` to rechunk `where` and `out` arrays.

### Stream E: out= Parameter (4 tests) ✅
Output array pre-allocation for reductions.

| Category | Tests | Notes |
|----------|-------|-------|
| Reduction out= | 4 | test_array_reduction_out, test_array_cumreduction_out |

**Implementation:** Used `_handle_out` from `_blockwise_funcs.py` in `reduction()` and `_cumreduction_expr()` functions. This properly sets `out._expr = result._expr` for array-expr mode.

### Stream F: setitem (7 tests) 🟡 **IN PROGRESS**
`__setitem__` implementation for array assignment.

| Tests | Notes | Status |
|-------|-------|--------|
| test_setitem_masked | Masked assignment | ✅ Fixed - exported da.ma module |
| test_setitem_extended_API_2d_mask | 2D masked setitem | ⚠️ 1 variant fails - numpy RuntimeWarning |
| test_setitem_errs | Error handling | ✅ Fixed - eager validation |
| test_setitem_bool_index_errs | Boolean index errors | ✅ Fixed - eager validation |
| test_setitem_extended_API_2d_rhs_func_of_lhs | RHS function of LHS | ⏳ Needs investigation |

**Implementation:**
- Fixed `da.ma` module export in array-expr `__init__.py`
- Fixed `take()` function to handle dask array indices without warnings
- Added eager validation in `__setitem__` using `parse_assignment_indices` to validate value shape vs implied shape before creating SetItem expression
- 67/68 setitem tests now pass (excluding the numpy warning issue)

### Stream G: Histogram Delayed Inputs (20 tests) 🟢 ✅ DONE
Histogram with delayed range and bins.

| Tests | Notes | Status |
|-------|-------|--------|
| test_histogram_delayed_range | 16 tests - delayed range parameter | ✅ |
| test_histogram_delayed_bins | 4 tests - delayed bins parameter | ✅ |

**Implementation:** Added `LinspaceDelayed` and `HistogramBinnedDelayed` expression classes that properly handle delayed bins/range by creating task dependencies. For delayed range, creates linspace at compute time. For delayed bins array, rechunks to single chunk for histogram computation.

### Stream H: register_chunk_type (71+ tests) 🟢 **DONE**
Custom chunk type registration for dispatching.

| Tests | Status |
|-------|--------|
| test_dispatch.py (67 tests) | ✅ All pass |
| test_binary_function_type_precedence (4 tests) | ✅ All pass |

**Implementation:**
- Exported `register_chunk_type` from `chunk_types.py` in array-expr branch of `__init__.py`
- Added `@check_if_handled_given_other` decorator to arithmetic dunder methods (`__add__`, `__mul__`, etc.) in `_collection.py` for proper type precedence
- Removed module-level skip from `test_dispatch.py`
- Updated `conftest.py` to register `EncapsulateNDArray` for array-expr mode

### Stream I: Empty Chunk nanmin/nanmax (4 tests) 🟢 **DONE**
Handle empty chunks in nanmin/nanmax.

| Tests | Notes | Status |
|-------|-------|--------|
| test_empty_chunk_nanmin_nanmax | 2 tests | ✅ |
| test_empty_chunk_nanmin_nanmax_raise | 2 tests | ✅ |

**Implementation:** Already working due to `compute_chunk_sizes()` implementation in Stream B. Removed stale xfail markers.

### Stream J: map_blocks Enhancements (5 tests) 🟢 **DONE**
Various map_blocks improvements.

| Tests | Notes | Status |
|-------|-------|--------|
| test_map_blocks_delayed | Delayed inputs | ⏳ xfail - tests HLG.validate() |
| test_map_blocks_large_inputs_delayed | Large inputs as delayed | ✅ |
| test_map_blocks_custom_name | Custom naming | ✅ |
| test_map_blocks_unique_name_enforce_dim | Unique naming | ✅ |
| test_map_blocks_dataframe | DataFrame output | ⏳ xfail - unrelated (pyarrow) |
| test_blockwise_large_inputs_delayed | Large inputs in blockwise | ✅ (bonus fix) |

**Implementation:**
- Fixed delayed/large input handling by merging dependency graphs in `Blockwise._layer()`
- Fixed custom name by passing `name=` when user provides explicit name
- Fixed enforce_ndim uniqueness by passing `token=out.name` in second blockwise call

### Stream K: Single Chunk Compute Behavior (13 tests) 🟢 **DONE**
Single chunk arrays returning references vs copies.

| Tests | Status |
|-------|--------|
| test_numpy_asarray_copy_true | ✅ 6 tests pass |
| test_numpy_asarray_copy_false | ✅ 6 tests pass |
| test_numpy_asarray_copy_none | ✅ 6 tests pass |
| test_numpy_asarray_copy_default | ✅ 6 tests pass |
| test_compute_copy | ✅ 2 tests pass |

**Implementation:** Added `CopyArray` expression class in `_expr.py` that wraps single-chunk arrays and applies `.copy()` to prevent mutation of graph-stored data. Modified `FinalizeComputeArray._simplify_down()` to use `CopyArray` for single-chunk arrays instead of returning the raw expression.

### Stream L: Miscellaneous (20+ tests) 🟢 **DONE**
Smaller independent fixes.

| Category | Tests | Notes | Status |
|----------|-------|-------|--------|
| Warning behavior | 5 | Warning messages differ | ✅ Fixed - added `warnings` import to `_collection.py` |
| Graph structure | 1 | Graph serialization differs | ✅ Fixed - test checks correctness, skips internal checks in array-expr |
| Naming patterns | 2 | Name patterns differ | ✅ Fixed - test accepts both `full-` and `full_like-` prefixes |
| API differences | 1 | Error messages, etc. | ✅ Fixed - test excludes array-expr internal symbols |
| Stack sequence check | 3 | Single array passed to vstack/hstack/dstack | ✅ Fixed - added Array type check |
| asarray/asanyarray like kwarg | 4 | like kwarg with Array input | ✅ Fixed - use asarray_safe/asanyarray_safe |
| Fusion | 3 | blockwise_fusion, block_id fusion | ⏳ xfail - architectural (fusion not implemented) |

**Implementation:**
- Added `warnings` import to `_collection.py` for `__array_function__` FutureWarning
- Added Array type check to vstack/hstack/dstack in `stacking/_simple.py`
- Fixed asarray/asanyarray `like` kwarg by using `asarray_safe`/`asanyarray_safe` with `partial`
- Updated tests to be mode-agnostic where appropriate

### Stream M: Store Advanced Features (6 tests) 🟡
Store with complex options like delayed targets, regions, compute=False.

| Tests | Notes | Status |
|-------|-------|--------|
| test_store_delayed_target | Delayed objects as targets | ⏳ |
| test_store_regions | Region slicing for partial writes | ⏳ |
| test_store_compute_false | compute=False return delayed | ⏳ |
| test_store_locks | Lock handling during store | ⏳ |
| test_store_locks_failure_lock_released | Lock release on failure | ⏳ |
| test_store_method_return | return_stored=True | ⏳ |

**Notes:** Basic store works. These tests require handling delayed targets, region slicing, and return_stored cases.

### Stream N: UFunc dtype Parameter (16 tests) 🟢 **DONE**
The `dtype=` parameter in ufunc calls when combined with `where=`.

| Tests | Notes | Status |
|-------|-------|--------|
| test_ufunc_where[*-f8] | 16 variants with dtype='f8' | ✅ |

**Implementation:** Fixed `Elemwise._info` to normalize user-provided dtype using `np.dtype()`. The issue was that dtype strings like `'f8'` were not being normalized to `np.dtype('f8')` (which displays as `float64`), causing `assert_eq` to fail on dtype comparison.

### Stream O: Unknown Chunks Handling (6 tests) 🟡
Operations on arrays with NaN (unknown) chunk sizes.

| Tests | Notes | Status |
|-------|-------|--------|
| test_no_chunks | Direct graph construction with NaN chunks | ⏳ |
| test_no_chunks_yes_chunks | Mixed known/unknown chunks | ⏳ |
| test_no_chunks_slicing_2d | 2D slicing with unknown chunks | ⏳ |
| test_raise_informative_errors_no_chunks | Error messages for unknown chunks | ⏳ |
| test_slicing_and_unknown_chunks | Slicing with unknown chunks | ⏳ |
| test_unknown_chunks_length_one | flatnonzero with unknown chunks | ⏳ |

**Notes:** These tests construct arrays with `chunks=((np.nan, np.nan),)` directly. The array-expr Array constructor may need to accept this pattern.

### Stream P: Integer Dask Array Indexing (4 tests) 🟢 **DONE**
Indexing with dask arrays as indices.

| Tests | Notes | Status |
|-------|-------|--------|
| test_index_with_int_dask_array[x_chunks2-1] | Specific chunk configuration | ✅ |
| test_index_with_int_dask_array[x_chunks3-2] | Specific chunk configuration | ✅ |
| test_index_with_int_dask_array[x_chunks4-2] | Specific chunk configuration | ✅ |
| test_index_with_int_dask_array_nocompute | Indices should not be computed eagerly | ⏳ xfail - relies on dict Array constructor |

**Implementation:** Fixed `ArrayOffsetDep` to use 1D chunks `(x.chunks[axis],)` instead of full `x.chunks`, and pass it with `offset_axes = (axis,)` instead of `p_axes`. The bug was causing shape alignment errors in blockwise when the transpose dimension didn't match the original axis dimension.

### Stream Q: from_array Features (6 tests) 🟡
Various from_array parameters and edge cases.

| Tests | Notes | Status |
|-------|-------|--------|
| test_from_array_with_lock | Lock parameter (2 variants) | ⏳ |
| test_from_array_inline | inline_array parameter | ⏳ |
| test_from_array_name | Custom name parameter | ⏳ |
| test_from_array_raises_on_bad_chunks | Error on invalid chunks | ⏳ |
| test_creation_data_producers | data_producer argument | ⏳ |

**Notes:** Basic from_array works. These test specific parameters.

### Stream R: Boolean Mask with Unknown Shapes (4 tests) 🟡
Boolean masking when mask has unknown shape.

| Tests | Notes | Status |
|-------|-------|--------|
| test_boolean_mask_with_unknown_shape[shapes0] | from_delayed case | ⏳ |
| test_boolean_mask_with_unknown_shape[shapes1] | from_delayed case | ⏳ |
| test_boolean_mask_with_unknown_shape[shapes2] | Other case | ⏳ |
| test_boolean_mask_with_unknown_shape[shapes3] | Other case | ⏳ |

**Notes:** Boolean masking with known shapes works. These involve unknown shape arrays created via from_delayed or boolean indexing.

### Stream S: Setitem Edge Cases (3 tests) 🟡
Remaining setitem issues.

| Tests | Notes | Status |
|-------|-------|--------|
| test_setitem_extended_API_2d_rhs_func_of_lhs | RHS depends on LHS | ⏳ |
| test_setitem_with_different_chunks_preserves_shape | 2 variants | ⏳ |
| test_setitem_extended_API_2d_mask (FAILED) | RuntimeWarning in numpy | 🔴 |

**Notes:** 67/68 setitem tests pass. The FAILED test has a numpy RuntimeWarning about invalid cast.

### Stream T: Graph Construction (5 tests) 🔴
Direct graph/dict construction with Array class.

| Tests | Notes | Status |
|-------|-------|--------|
| test_dont_fuse_outputs | Direct graph construction | ⏳ |
| test_dont_dealias_outputs | Graph aliasing | ⏳ |
| test_dask_layers | __dask_layers__() method | ⏳ |
| test_chunks_error | from_array error checking | ⏳ |
| test_constructor_plugin | Constructor plugin | ⏳ |

**Notes:** These tests construct Array directly from dicts, which array-expr doesn't support in the same way. May need to create FromGraph expression or similar.

### Stream U: Array Pickling (2 tests) 🟢
Pickle serialization of arrays.

| Tests | Notes | Status |
|-------|-------|--------|
| test_array_picklable[array0] | Basic pickling | ⏳ |
| test_array_picklable[array1] | Basic pickling | ⏳ |

**Notes:** The test creates arrays via dict constructor. May pass if using standard creation functions.

### Stream V: Fusion (4 tests) 🔴
Blockwise fusion optimization.

| Tests | Notes | Status |
|-------|-------|--------|
| test_blockwise_fusion | Basic fusion | ⏳ (architectural) |
| test_map_blocks_block_id_fusion | block_id fusion | ⏳ (architectural) |
| test_trim_internal | Requires fusion | ⏳ (architectural) |
| test_map_blocks_optimize_blockwise | 2 variants | ⏳ (architectural) |

**Notes:** Fusion is not yet implemented in array-expr. This is a larger architectural change. Low priority for now.

### Stream W: Blockwise concatenate (1 test) 🟡
The `concatenate=True` parameter in blockwise.

| Tests | Notes | Status |
|-------|-------|--------|
| test_blockwise_concatenate | concatenate=True | ⏳ |

**Notes:** When `concatenate=True`, blockwise should concatenate chunks along specified axes before applying the function.

### Stream X: Rechunk Auto (2 tests) 🟢
Automatic chunk size calculation differences.

| Tests | Notes | Status |
|-------|-------|--------|
| test_rechunk_auto_image_stack[100] | Different chunk sizes | ⏳ |
| test_rechunk_auto_image_stack[1000] | Different chunk sizes | ⏳ |

**Notes:** Array-expr computes slightly different chunk sizes for `chunks="auto"`. May just need test adjustment if results are still valid.

### Stream Y: Random Broadcasting (2 tests) 🟡
Random arrays with broadcasted shapes.

| Tests | Notes | Status |
|-------|-------|--------|
| test_array_broadcasting[RandomState] | RandomState with broadcasting | ⏳ |
| test_array_broadcasting[default_rng] | default_rng with broadcasting | ⏳ |

**Notes:** Random number generation mostly works. These test broadcasting of shape parameters.

### Stream Z: Misc Single Tests (~37 tests) 🟢
Various individual tests.

| Test | Notes | Status |
|------|-------|--------|
| test_elemwise_on_scalars | 0-D dask scalars in elemwise | ⏳ |
| test_matmul | Specific matmul issue | ⏳ |
| test_astype_gh9316 | GitHub issue #9316 | ⏳ |
| test_slicing_with_non_ndarrays | Custom types in slicing | ⏳ |
| test_map_blocks3 | map_blocks edge case | ⏳ |
| test_raise_on_bad_kwargs | from_array kwargs checking | ⏳ |
| test_warn_bad_rechunking | Warning message | ⏳ |
| test_map_blocks_delayed | Returns dict not HLG | ⏳ (by design) |
| test_to_delayed_optimize_graph | Optimization | ⏳ |
| test_index_array_with_array_2d | 2D array indexing | ⏳ |
| test_index_array_with_array_3d_2d | Chunking alignment | ⏳ |
| test_normalize_chunks_object_dtype | 2 variants | ⏳ |
| test_pandas_from_dask_array | Pandas conversion | ⏳ |
| test_partitions_indexer | .partitions property | ⏳ |
| test_chunk_assignment_invalidates_cached_properties | Property caching | ⏳ |
| test_delayed_array_key_hygeine | Key hygiene | ⏳ |
| test_to_backend | Backend switching | ⏳ |
| test_linspace | 2 variants - dask scalar inputs | ⏳ |
| test_arange_cast_float_int_step | Float-to-int casting | ⏳ (by design) |
| test_nan_full_like | 2 variants - unsigned int edge case | ⏳ |
| test_like_forgets_graph | 2 remaining variants | ⏳ |
| test_gufunc_chunksizes_adjustment | Array.copy | ⏳ |
| test_cull | Graph culling | ⏳ |
| test_positional_indexer_newaxis | Newaxis in indexer | ⏳ |
| test_frompyfunc | Custom ufunc tokenization | ⏳ |
| test_weighted_reduction | Weighted reductions | ⏳ |
| test_select_broadcasting | Broadcasting in select() | ⏳ |
| test_meta_from_array_type_inputs | meta_from_array | ⏳ |
| test_assert_eq_scheduler | Test utility issue | ⏳ |
| test_map_blocks_dataframe | DataFrame output | ⏳ |

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
