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

## Priority Tiers

### Tier 1: Blocking Foundation
These operations block many others and should be done first.

| Operation | Location | Blocker For | Status |
|-----------|----------|-------------|--------|
| reshape | `_reshape.py` | boolean indexing, ravel, flatten | **Done** |
| __array_function__ | `_collection.py:336` | np.* function support | **Done** |

### Tier 2: Core Linear Algebra
Important for scientific computing users.

| Operation | Location | Notes | Status |
|-----------|----------|-------|--------|
| matmul | `_linalg.py` | __matmul__, np.matmul | **Done** |
| tensordot | `_linalg.py` | Used extensively | **Done** |
| dot | `_linalg.py` | Wraps tensordot, Array.dot() method | **Done** |
| vdot | `_linalg.py` | Vector dot product (needs ravel) | **Done** |
| einsum | `routines.py` | Einstein summation (needs asarray fix) | Not started |

### Tier 3: Indexing Completion
Complete the indexing story.

| Operation | Location | Notes | Status |
|-----------|----------|-------|--------|
| boolean indexing | `slicing.py` | Needs ravel (reshape) | Not started |
| field access | `_collection.py` | Structured arrays | **Done** |
| setitem | `_collection.py` | In-place assignment | **Done** |

### Tier 4: Reductions Completion
Fill out remaining reduction operations.

| Operation | Location | Notes | Status |
|-----------|----------|-------|--------|
| argmin/argmax | `reductions.py` | Need special handling | **Done** |
| cumsum/cumprod | `reductions.py` | Cumulative ops | **Done** (axis=None edge case needs work) |
| weighted mean/average | `routines.py` | weights parameter | **Done** (weights/returned need broadcast_to) |
| ptp | `routines.py` | Peak-to-peak | **Done** |
| median/percentile | `reductions.py`, `percentile.py` | Uses rechunk + map_blocks | **Done** |

### Tier 5: Shape Manipulation
Operations that change array structure.

| Operation | Location | Notes | Status |
|-----------|----------|-------|--------|
| ravel | `_collection.py` | Uses reshape | **Done** |
| flatten | `_collection.py` | Uses reshape | **Done** |
| squeeze | `_slicing.py` | Remove 1-d dims | **Done** |
| expand_dims | `_collection.py` | Add dims | **Done** |
| atleast_*d | `_collection.py` | Shape helpers | **Done** |
| broadcast_to | `_collection.py`, `_broadcast.py` | Broadcasting | **Done** |
| tile | `creation.py` | Repetition, needs block | Not started |
| roll | `_collection.py` | Circular shift | **Done** |

### Tier 6: Routines
General array routines.

| Operation | Location | Notes | Status |
|-----------|----------|-------|--------|
| where | `_collection.py` | Conditional select | **Done** |
| unique | `routines.py` | Deduplication | Not started |
| diff | `_collection.py` | Differences | **Done** |
| gradient | `_routines.py` | Numerical gradient (uses map_overlap) | **Done** |
| histogram | `routines.py` | Binning | Not started |
| searchsorted | `_routines.py` | Binary search | **Done** |
| insert/delete | `routines.py` | Array modification | Not started |
| compress | `_routines.py` | Boolean select (partial - dask array conditions need boolean indexing) | **Done** |

### Tier 7: Advanced
Lower priority advanced operations.

| Operation | Location | Notes | Status |
|-----------|----------|-------|--------|
| P2P rechunking | `_rechunk.py` | Distributed method | Not started |
| coarsen | `slicing.py` | Downsampling | Not started |
| block views | `core.py` | Block iteration | Not started |

## Testing Strategy

### Test Markers
- `@pytest.mark.array_expr`: Tests specific to array-expr
- `@pytest.mark.xfail(da._array_expr_enabled(), reason="...")`: Known failures

### Running Tests
```bash
# Test array-expr specifically
DASK_ARRAY__QUERY_PLANNING=True pytest dask/array/tests/

# Test traditional mode
DASK_ARRAY__QUERY_PLANNING=False pytest dask/array/tests/
```

### Test Patterns
1. Basic functionality with assert_eq
2. Various chunk patterns (single chunk, many chunks, uneven)
3. Edge cases (empty, scalar, nan chunks)
4. Integration with other operations

## Migration Workflow (TDD-First)

The existing test suite guides development. Tests define behavior we must match.

**Skill available**: `.claude/skills/array-expr-migration/SKILL.md`

### Phase 1: Test Discovery
```bash
# Find tests for target operation
grep -l "{operation}" dask/array/tests/*.py
grep -n "def test.*{operation}" dask/array/tests/test_*.py
```

### Phase 2: Study Traditional Implementation
```bash
# Find the current implementation
grep -n "def {operation}" dask/array/*.py
```

Understand: metadata computation, chunking logic, graph construction.

### Phase 3: Implement Expression Class
Create class in `dask/array/_array_expr/_*.py`.

**Run tests iteratively during development**:
```bash
DASK_ARRAY__QUERY_PLANNING=True pytest -k {operation} -x -v
```

Tests are your guide - run them frequently as you build.

### Phase 4: Wire API & Clean Up
- Add to `_collection.py` if needed
- Remove `NotImplementedError` placeholders
- Remove xfail markers from tests
- Update this plan with status

### Quick Reference
```bash
# Quick check - stop on first failure
DASK_ARRAY__QUERY_PLANNING=True pytest -k {operation} -x

# Verbose for debugging
DASK_ARRAY__QUERY_PLANNING=True pytest -k {operation} -v --tb=short

# Full test file
DASK_ARRAY__QUERY_PLANNING=True pytest dask/array/tests/test_routines.py -v

# Find xfail markers
grep -n "xfail.*_array_expr" dask/array/tests/*.py
```

## Open Questions

1. **__array_function__ approach**: Full protocol or shim?
2. **P2P rechunking**: When to default to p2p vs tasks?
3. **Simplification rules**: What algebraic simplifications are valuable for arrays?
4. **Fusion strategy**: How aggressively to fuse blockwise operations?

## References

- DataFrame expr: `dask/dataframe/dask_expr/`
- Base expr: `dask/_expr.py`
- Existing array-expr: `dask/array/_array_expr/`
- Traditional array: `dask/array/core.py`, `reductions.py`, `routines.py`, etc.
