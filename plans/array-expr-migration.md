# Array Expression Migration Plan

Design doc: `/designs/array-expr.md`
Skill: `.claude/skills/array-expr-migration/SKILL.md`

## Current Status

**Test Results (December 2025):**
- Array tests: 4454 passed, 29 xfailed, 566 skipped (+ pre-existing rechunk issues)
- Dataframe tests: 3568 passed, 16 failed, 578 skipped
- Core/bag/diagnostics tests: 1187 passed, 41 failed, 76 skipped

The array-expr system is nearly complete for array-only operations. The remaining work falls into:
1. **Active Regressions** - Bugs in array-expr code
2. **Cross-Module Integration** - Dataframe↔array, delayed↔array interactions
3. **Deferred** - Tests that check internal implementation details (HLG, graph structure)

## Active Regressions (Priority 1)

These tests were working and have regressed. Fix immediately.

| Test | Issue | Fix |
|------|-------|-----|
| `test_average_weights_with_masked_array` (2 tests) | `asanyarray()` in `_average()` uses legacy import | Use late import like `broadcast_to` fix |
| `test_array_bag_delayed`, `test_finalize_name` | `FinalizeComputeArray.chunks` missing decorator | Add `@cached_property` to `_expr.py:373` |

**Root Cause 1:** Line 2514 in `routines.py` calls `asanyarray(a)` which imports from `dask.array.core` (legacy). When an array-expr Array is passed, it triggers the "already a Dask collection" warning. The `broadcast_to` import was fixed with a late import pattern but `asanyarray` still uses the module-level import.

**Root Cause 2:** `FinalizeComputeArray.chunks` at `dask/array/_array_expr/_expr.py:373` is defined as `def chunks(self):` but missing the `@cached_property` decorator. This causes `'method' object is not iterable` when delayed tries to unpack array collections.

## Cross-Module Integration (Priority 2)

These failures occur when array-expr arrays interact with other dask modules.

### Dataframe↔Array Bridge (16 tests) - Implement

When dataframe operations return arrays (e.g., `ddf.values`, `ddf.to_dask_array()`), the result is an array-expr `Array` collection wrapping a **dataframe-expr** `MapPartitions` expression. The dataframe expression lacks array properties (`chunks`, `dtype`, `shape`).

| Test | Issue |
|------|-------|
| `test_values`, `test_values_extension_dtypes` | `MapPartitions` has no `chunks` |
| `test_to_dask_array_unknown` (2 tests) | `MapPartitions` has no `chunks` |
| `test_to_dask_array[True-False-meta2]` | `_meta` has no setter |
| `test_map_partition_array` (2 tests) | Broadcasting/dtype issues |
| `test_mixed_dask_array_operations` | `MapPartitions` has no `dtype` |
| `test_mixed_dask_array_multi_dimensional` | Type dispatch for None |
| `test_scalar_with_array` | Deprecation warning |
| `test_array_to_df_conversion` | NoneType has no itemsize |
| `test_loc_with_array` (3 tests) | `MapPartitions` has no `dtype` |
| `test_to_numeric_on_dask_array` (2 tests) | `MapPartitions` has no `chunks` |

**Fix:** Create a bridge ArrayExpr that wraps dataframe expressions, providing array-like interface (`chunks`, `dtype`, `shape`, `_meta`) while delegating to the dataframe expression for graph generation. This is cleaner than having expr↔HLG interactions.

### from_graph rename parameter (5 tests) - Implement

| Test | Issue |
|------|-------|
| `test_blockwise_clone_with_literals` (5 params) | `from_graph()` got unexpected keyword argument 'rename' |

**Root Cause:** `clone()` in `graph_manipulation.py:407` calls rebuild function with `rename` kwarg, but array-expr's `from_graph` doesn't accept it.

**Fix:** Add `rename` parameter to `dask/array/_array_expr/core/_from_graph.py`.

### dask.optimize() (9 tests) - Implement

| Test | Issue |
|------|-------|
| `test_blockwise_array_creation` (9 params) | `NotImplementedError: Function optimize is not implemented for dask-expr` |

**Fix:** Implement `optimize` function for array-expr, likely as a no-op or calling `simplify()`.

### HLG API Tests (6 tests) - XFail

Array-expr returns plain dicts from `__dask_graph__()`, not `HighLevelGraph`. Tests expecting HLG methods fail by design.

| Test | Issue |
|------|-------|
| `test_keys_values_items_to_dict_methods` | `dict` has no `to_dict()` |
| `test_single_annotation` (2 params) | `dict` has no `layers` |
| `test_multiple_annotations` | `dict` has no `layers` |
| `test_blockwise_cull` (2 params) | `dict` has no `cull` |

**Fix:** Add `xfail(using_array_expr(), reason="array-expr returns dict graphs")` markers.

### Graph Order/Typing (5 tests) - XFail

| Test | Issue |
|------|-------|
| `test_reduce_with_many_common_dependents` (4 params) | Ordering heuristic assertions fail |
| `test_isinstance_core[HLGDaskCollection]` | Type check for HLG fails |

**Fix:** Add xfail markers - graph structure differs by design.

### Other Integration (2 tests) - Investigate

| Test | Issue |
|------|-------|
| `test_annotations_blockwise_unpack` | ZeroDivisionError |
| `test_combo_of_layer_types` | `MapPartitions` has no `chunks` |

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

| Test | Location | Issue | Status |
|------|----------|-------|--------|
| `test_store_regions` | test_array_core.py:2171 | Graph dependency in regions | ✅ Fixed (FromGraph key prefix matching) |
| `test_linspace[True/False]` | test_creation.py:132 | Dask scalar inputs | ⏭️ Deferred (requires architectural changes) |
| `test_partitions_indexer` | test_array_core.py:5474 | `.partitions` property | ✅ Fixed (added partitions alias) |
| `test_index_array_with_array_3d_2d` | test_array_core.py:3828 | Chunk alignment | ❌ Not array-expr specific |
| `test_positional_indexer_newaxis` | test_slicing.py:1119 | np.newaxis handling | ✅ Fixed (added ExpandDims expr) |
| `test_map_blocks_dataframe` | test_array_core.py:4824 | DataFrame from map_blocks | ❌ Not array-expr specific |

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
1. Fix `FinalizeComputeArray.chunks` - add `@cached_property` decorator at `_expr.py:373`
2. Fix `_average()` to use late imports for `asanyarray` - matches existing `broadcast_to` fix

### Short Term (Cross-Module Integration)
3. Add `rename` parameter to `from_graph()` - enables `clone()` support
4. Implement `dask.optimize()` for array-expr - likely calls `simplify()`
5. Add xfail markers to HLG API tests (6 tests) and graph order tests (5 tests)

### Medium Term (Dataframe↔Array Bridge)
6. Create bridge ArrayExpr for dataframe expressions - biggest architectural piece
   - Wrap dataframe `MapPartitions` with array-like interface
   - Provide `chunks`, `dtype`, `shape`, `_meta` properties
   - Delegate graph generation to underlying dataframe expression

### Existing Array Work (Category D - ✅ Done)
7. ~~Store regions graph dependencies~~ ✅ Done
8. ~~Block_id fusion support~~ ✅ Done
9. ~~Linspace with dask scalars~~ Deferred (requires 0-d array computation)
10. ~~Partitions indexer~~ ✅ Done

### Deferred
- Legacy graph API tests (by design)
- HLG-dependent tests (by design)
- XArray integration (external dependency)
- `test_cull` (tests internal optimization, not user behavior)

## Implementation Patterns

### Dataframe↔Array Bridge Pattern

When a dataframe operation produces an array (like `ddf.values`), we need to bridge between expression systems. Create an ArrayExpr that wraps dataframe expressions:

```python
class DataFrameToArray(ArrayExpr):
    """Bridge from dataframe-expr to array-expr."""
    _parameters = ["df_expr", "_meta", "_chunks"]

    @cached_property
    def chunks(self):
        return self._chunks

    @cached_property
    def dtype(self):
        return self._meta.dtype

    def _layer(self):
        # Delegate to the dataframe expression's graph
        return self.df_expr.__dask_graph__()
```

This keeps expr↔expr interactions clean without HLG in the middle.

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
