---
name: array-expr-migration
description: Migrate Dask array operations from traditional graph-building to the expression system. Use when implementing operations in dask/array/_array_expr/, converting operations like reshape, matmul, or other array functions to expression classes.
---

# Array Expression Migration Skill

Guides TDD-first migration of Dask array operations to the expression system.

**Design doc**: `designs/array-expr.md`
**Migration plan**: `plans/array-expr-migration.md`

## Phases

### Phase 1: Test Discovery
Find existing tests that exercise the target operation.

```bash
# Find test files mentioning the operation
grep -l "{operation}" dask/array/tests/*.py

# Find specific test functions
grep -n "def test.*{operation}" dask/array/tests/test_*.py

# Check for xfail markers
grep -n "xfail.*{operation}\|xfail.*_array_expr" dask/array/tests/*.py
```

**Output**: List of test files and functions to target.

### Phase 2: Traditional Implementation Study
Understand the existing implementation before converting.

```bash
# Find the implementation
grep -n "def {operation}" dask/array/*.py
```

Read the implementation and identify:
- How is dtype/meta computed?
- How are output chunks determined?
- How is the task graph built?
- What edge cases are handled?

**Output**: Notes on metadata logic, chunking logic, and graph construction.

### Phase 3: Expression Class Implementation
Create the expression class in `dask/array/_array_expr/`.

Required components:
```python
class {Operation}(ArrayExpr):
    _parameters = [...]  # Input parameters
    _defaults = {...}    # Default values

    @cached_property
    def _meta(self):
        # Return small array with correct dtype/type
        pass

    @cached_property
    def chunks(self):
        # Return tuple of tuples for output chunking
        pass

    def _layer(self):
        # Return dict of tasks
        pass
```

Run tests frequently during development:
```bash
DASK_ARRAY__QUERY_PLANNING=True .venv/bin/pytest dask/array/tests/test_*.py -k {operation} -x -v
```

Do not rely on manual testing with scripts.  We want to lean heavily on our tests.

Avoid from_graph.  We want to either use our API on existing operations, or
build new expressions.  We don't want to use from_graph; that limits future
potential for optimizations.

**Output**: Working expression class with basic tests passing.

### Phase 4: API Wiring
Connect the expression class to the user-facing API. This requires changes in 4 places:

1. **Expression module** (`_slicing.py`, `_reductions.py`, etc.) - the expression class and function
2. **`_collection.py`** - Add `Array.{operation}()` method and/or top-level function
3. **`_array_expr/__init__.py`** - Export the function
4. **`dask/array/__init__.py`** - Two changes needed:
   - Add to the imports from `_array_expr` (around line 750)
   - Remove from `raise_not_implemented_error` fallback list (around line 840)

**Output**: Operation accessible via normal dask.array API.

### Phase 5: Review our work for simplification and Cleanup

Let's review our work and see if there is anything we should simplify or clean
up

### Phase 6: Full Test Suite & Cleanup
Ensure all related tests pass and clean up.

```bash
# Run full test suite for the module
DASK_ARRAY__QUERY_PLANNING=True pytest dask/array/tests/test_routines.py -v

# Check for any remaining xfail markers to remove
grep -n "xfail.*{operation}" dask/array/tests/*.py
```

If there are new tests that are XPASS then clean up the xfail markers (assuming
that they're related to our changes).

**Output**: All tests passing, xfail markers removed.

### Phase 7: Update Plan
Update `plans/array-expr-migration.md`:
- Mark operation as **Done** in the appropriate tier table
- Add to "Current Status" list if it's a significant capability

**Output**: Plan reflects current state for future agents.

## Key Patterns

### Blockwise Operations
For element-wise or block-aligned operations, use `Blockwise` or `Elemwise`:
```python
from dask.array._array_expr._blockwise import Blockwise, Elemwise
```

### Reductions
For aggregations, use the reduction framework:
```python
from dask.array._array_expr._reductions import reduction, PartialReduce
```

### Shape Changes
For operations that restructure arrays, implement custom `_layer()` logic.

## Testing During Development

Run tests iteratively - they're your guide:
```bash
# Quick check - stop on first failure
DASK_ARRAY__QUERY_PLANNING=True pytest -k {operation} -x

# Verbose output for debugging
DASK_ARRAY__QUERY_PLANNING=True pytest -k {operation} -v --tb=short

# Run single specific test
DASK_ARRAY__QUERY_PLANNING=True pytest dask/array/tests/test_routines.py::test_{operation} -v
```

## Reference Locations

- **Examples to study**: Browse `dask/array/_array_expr/` for completed implementations. Read a few before starting.
- Base class: `dask/array/_array_expr/_expr.py` (ArrayExpr)
- Collection: `dask/array/_array_expr/_collection.py` (Array wrapper)
- Blockwise: `dask/array/_array_expr/_blockwise.py`
- Reductions: `dask/array/_array_expr/_reductions.py`
- Traditional impl: `dask/array/core.py`, `routines.py`, etc.
