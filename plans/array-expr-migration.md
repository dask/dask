# Array Expression Migration Plan

Design doc: `/designs/array-expr.md`
Skill: `.claude/skills/array-expr-migration/SKILL.md`

## Current Status

**Test Results (December 2025):** 4509 passed, 32 xfailed, 2 failed, 610 skipped

The array-expr system is nearly complete. Most operations have been migrated. The remaining work falls into a few categories:
1. **Active Regressions** - 2 tests that were passing but now fail
2. **Fixable Issues** - Tests that could be fixed with targeted work
3. **Deferred** - Tests that require architectural changes or test internal details

## Active Regressions (Priority 1)

These tests were working and have regressed. Fix immediately.

| Test | Issue | Fix |
|------|-------|-----|
| `test_average_weights_with_masked_array` (2 tests) | `asanyarray()` in `_average()` uses legacy import | Use late import like `broadcast_to` fix |

**Root Cause:** Line 2514 in `routines.py` calls `asanyarray(a)` which imports from `dask.array.core` (legacy). When an array-expr Array is passed, it triggers the "already a Dask collection" warning. The `broadcast_to` import was fixed with a late import pattern but `asanyarray` still uses the module-level import.

## Remaining XFails by Category

### Category A: Legacy Graph API (7 tests) - Deferred

These tests construct Arrays directly from dicts/graphs. Array-expr fundamentally doesn't support this.

| Test | Location | Issue |
|------|----------|-------|
| `test_dont_fuse_outputs` | test_array_core.py:1958 | Direct graph construction |
| `test_dont_dealias_outputs` | test_array_core.py:1992 | Direct graph construction |
| `test_to_delayed_optimize_graph` | test_array_core.py:3225 | HLG layer inspection |
| `test_constructor_plugin` | test_array_core.py:4109 | array_plugins config |
| `test_dask_layers` | test_array_core.py:3787 | `__dask_layers__()` method |
| `test_meta_from_array_type_inputs` | test_array_utils.py:65 | Array(graph,...) constructor |
| `test_index_with_int_dask_array_nocompute` | test_slicing.py:712 | Legacy Array constructor |

**Decision:** These test internal implementation details rather than user-facing behavior. Low priority.

### Category B: HLG/Validate (2 tests) - Deferred

| Test | Location | Issue |
|------|----------|-------|
| `test_map_blocks_delayed` | test_array_core.py:4128 | Calls `.dask.validate()` (needs HLG) |
| `test_warn_bad_rechunking` | test_array_core.py:3795 | Graph structure inspection |

**Decision:** Array-expr returns dict graphs, not HLG. This is by design.

### Category C: Fusion/Optimization (1 test remaining) - ✅ Mostly Done

| Test | Location | Issue | Status |
|------|----------|-------|--------|
| `test_map_blocks_block_id_fusion` | test_map_blocks.py:9 | block_id in fused operations | ✅ Fixed |
| `test_trim_internal` | test_overlap.py:141 | Task count requires fusion | ✅ Fixed |
| `test_push` | test_overlap.py:758 | Bottleneck push implementation | ✅ Fixed |
| `test_cull` | test_slicing.py:1119 | Internal graph optimization | Deferred (tests internals) |

**Completed Work:**
- Made `map_blocks` fusable when `drop_axis` is not used (set `concatenate=False` when no indices are contracted)
- Added `push` function to array-expr using `cumreduction`
- Added `cumreduction` function to array-expr reductions module
- Updated tests to use `expr.optimize()` for fusion checks (array-expr optimizes at expression level)

### Category D: Specific Features (6 tests) - Mixed Priority

| Test | Location | Issue | Priority |
|------|----------|-------|----------|
| `test_store_regions` | test_array_core.py:2171 | Graph dependency in regions | Medium |
| `test_linspace[True/False]` | test_creation.py:132 | Dask scalar inputs | Low |
| `test_partitions_indexer` | test_array_core.py:5474 | `.partitions` property | Low |
| `test_index_array_with_array_3d_2d` | test_array_core.py:3828 | Chunk alignment | Low |
| `test_positional_indexer_newaxis` | test_slicing.py (not found in grep) | np.newaxis handling | Low |
| `test_map_blocks_dataframe` | test_array_core.py:4824 | DataFrame from map_blocks | Low (architectural) |

### Category E: Design Decisions (4 tests) - Won't Fix

| Test | Location | Issue |
|------|----------|-------|
| `test_creation_data_producers` | test_creation.py:568 | `data_producer` not in array-expr |
| `test_arange_cast_float_int_step` | test_creation.py | Float-to-int edge behavior |
| `test_gufunc_chunksizes_adjustment` | test_gufunc.py:705 | apply_gufunc rechunking |
| `test_xarray_reduction` | test_overlap.py:973 | XArray integration |

### Category F: Numpy Compatibility (2 tests) - Won't Fix

| Test | Issue |
|------|-------|
| `test_nan_full_like[u4-*--1]` | NumPy 2.1+ rejects -1 in unsigned int |

### Category G: Graph Serialization (2 tests) - Low Priority

| Test | Location | Issue |
|------|----------|-------|
| `test_like_forgets_graph[arange]` | test_creation.py:1120 | Graph pickling differs |
| `test_like_forgets_graph[tri]` | test_creation.py:1127 | Graph pickling differs |

### Category H: Non-Array-Expr Issues (2 tests)

These have xfails that aren't array-expr specific:

| Test | Issue |
|------|-------|
| `test_select_broadcasting` | General dask issue |
| `test_two[ttest_1samp-kwargs2]` | scipy 1.10+ compatibility |

## Recommended Work Order

### Immediate (Fix Regressions)
1. Fix `_average()` to use late imports for `asanyarray` - matches existing `broadcast_to` fix

### Short Term (High Value)
2. Store regions graph dependencies
3. ~~Block_id fusion support~~ ✅ Done

### Medium Term
4. Linspace with dask scalars
5. Partitions indexer
6. 3D/2D indexing chunk alignment

### Deferred
- Legacy graph API tests (by design)
- HLG-dependent tests (by design)
- XArray integration (external dependency)
- `test_cull` (tests internal optimization, not user behavior)

## Implementation Patterns

### Late Import Pattern (for avoiding legacy/array-expr mixing)
```python
# Instead of module-level import:
from dask.array.core import asanyarray  # BAD - legacy only

# Use late import:
import dask.array as _da
a = _da.asanyarray(a)  # GOOD - respects array-expr mode
```

### Testing
```bash
# Run all tests with array-expr
pytest --array-expr dask/array/tests/

# Quick check single operation
pytest --array-expr -k {operation} -x

# Find xfail markers
grep -rn "xfail.*array-expr" dask/array/tests/
```

## References

- Design doc: `designs/array-expr.md`
- Fusion design: `designs/array-expr-fusion.md`
- Skill: `.claude/skills/array-expr-migration/SKILL.md`
- Base expr: `dask/_expr.py`
- Array-expr implementation: `dask/array/_array_expr/`
