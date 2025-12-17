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

## Testing Infrastructure

Test modules have been converted from blacklist (module-level skips) to whitelist (individual xfails):

| Module | Status |
|--------|--------|
| test_array_core.py | Converted: 221 pass, 240 xfail, 44 xpass |
| test_array_utils.py | Converted: 30 pass, 2 xfail |
| test_rechunk.py | Converted: 79 pass, 3 xfail |
| test_dispatch.py | Module skip (register_chunk_type fundamental difference) |
| test_array_function.py | Module skip (depends on test_dispatch) |
| test_routines.py | Converted: 25 pass, 700 xfail, 2 xpass |

Each xfailed test represents work to be done. Decreasing xfails is progress.

## Priority Tiers

### Tier 1: Blocking Foundation
These operations block many others and should be done first.

| Operation | Location | Blocker For | Status |
|-----------|----------|-------------|--------|
| reshape | `core.py:2100+` | boolean indexing, ravel, flatten | Not started |
| __array_function__ | `_collection.py:314` | np.* function support | Not started |

### Tier 2: Core Linear Algebra
Important for scientific computing users.

| Operation | Location | Notes | Status |
|-----------|----------|-------|--------|
| matmul | `routines.py` | __matmul__, np.matmul | Not started |
| tensordot | `routines.py` | Used extensively | Not started |
| dot | `routines.py` | Wraps tensordot | Not started |
| einsum | `routines.py` | Einstein summation | Not started |

### Tier 3: Indexing Completion
Complete the indexing story.

| Operation | Location | Notes | Status |
|-----------|----------|-------|--------|
| boolean indexing | `slicing.py` | Needs ravel (reshape) | Not started |
| field access | `_collection.py` | Structured arrays | **Done** |
| setitem | `slicing.py` | In-place assignment | Not started |

### Tier 4: Reductions Completion
Fill out remaining reduction operations.

| Operation | Location | Notes | Status |
|-----------|----------|-------|--------|
| argmin/argmax | `reductions.py` | Need special handling | Not started |
| cumsum/cumprod | `reductions.py` | Cumulative ops | Not started |
| weighted mean/average | `reductions.py` | weights parameter | Not started |
| ptp | `reductions.py` | Peak-to-peak | Not started |
| median/percentile | `reductions.py` | Complex shuffling | Not started |

### Tier 5: Shape Manipulation
Operations that change array structure.

| Operation | Location | Notes | Status |
|-----------|----------|-------|--------|
| ravel | `routines.py` | Needs reshape | Not started |
| flatten | `routines.py` | Needs reshape | Not started |
| squeeze | `_slicing.py` | Remove 1-d dims | **Done** |
| expand_dims | `routines.py` | Add dims | Not started |
| atleast_*d | `routines.py` | Shape helpers | Not started |
| broadcast_to | `routines.py` | Broadcasting | Not started |
| tile | `routines.py` | Repetition | Not started |
| roll | `routines.py` | Circular shift | Not started |

### Tier 6: Routines
General array routines.

| Operation | Location | Notes | Status |
|-----------|----------|-------|--------|
| where | `routines.py` | Conditional select | Not started |
| unique | `routines.py` | Deduplication | Not started |
| diff | `routines.py` | Differences | Not started |
| gradient | `routines.py` | Numerical gradient | Not started |
| histogram | `routines.py` | Binning | Not started |
| searchsorted | `routines.py` | Binary search | Not started |
| insert/delete | `routines.py` | Array modification | Not started |
| compress | `routines.py` | Boolean select | Not started |

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
