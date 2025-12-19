# Array-Expr Blockwise Fusion

## Motivation

Blockwise fusion combines consecutive blockwise operations into single fused tasks, reducing scheduler overhead and intermediate materializations. For example, `(x + 1) * 2 + 3` becomes one task per block rather than three.

## How It Works

### Core Abstraction: `_task(key, block_id)`

Each fusable expression implements `_task(key, block_id)` which generates a `Task` object for a specific output block. This enables `FusedBlockwise` to collect tasks from multiple expressions and combine them via `Task.fuse()`.

```python
# Elemwise._task() handles broadcasting
task = expr._task(("add-abc", 0, 1), (0, 1))  # Task for block (0, 1)
```

### Block Coordinate Mapping: `_input_block_id(dep, block_id)`

Each expression also implements `_input_block_id(dep, block_id)` which maps an output block coordinate to the corresponding input block coordinate for a dependency. This handles:

- **Broadcasting**: Arrays with fewer dimensions or single-block dimensions
- **Transpose**: Axis permutations via inverse mapping
- **Identity**: Most operations pass through unchanged

### FusedBlockwise Class

`FusedBlockwise` wraps a tuple of expressions to fuse. At graph materialization:

1. `_compute_block_ids()` traces through expressions to compute each one's block coordinate
2. `_task()` calls each expression's `_task()` method and combines via `Task.fuse()`

### Fusion Algorithm

`optimize_blockwise_fusion_array()` traverses the expression tree:

1. Build dependency graph of fusable operations
2. Find "roots" (fusable ops with no fusable dependents)
3. For each root, greedily collect fusable ancestors where all dependents are in the group
4. Check for conflicting block patterns (`_remove_conflicting_exprs`)
5. Create `FusedBlockwise` and substitute into expression tree

## Key Decisions

**Follow the DataFrame pattern**: Arrays use the same `_task()` + `Fused` class approach as dataframes, extended for N-dimensional block indices.

**Specialized `_task()` implementations**: Rather than one generic implementation, each expression type (Elemwise, Transpose, BroadcastTrick) has its own `_task()` that understands its semantics. Base `Blockwise` provides a fallback using symbolic index mapping.

**Exclude IO operations**: `FromArray` and `FromDelayed` are not fusable - they represent data boundaries.

**Exclude `concatenate=True` blockwise**: Operations that concatenate blocks along a dimension (like some reductions) require special handling not yet implemented.

**Handle conflicting block patterns**: When the same expression is accessed via multiple paths with different block mappings (e.g., `a + a.T`), we detect the conflict and exclude the problematic expression from fusion rather than producing an invalid graph.

## Edge Cases

### Same Array, Different Patterns (`a + a.T`)

When computing `a + a.T`, the `add` operation accesses `a` directly (block `(i,j)`) and via transpose (block `(j,i)`). If we tried to fuse `a` into the group, we'd have a conflict - the same expression would need different block coordinates for the same output block.

Solution: `_remove_conflicting_exprs()` detects when an expression is reached via multiple paths with different block_ids. It removes the conflicting expression (and any expressions that become unreachable) from the fusion group.

### Broadcasting

Arrays with fewer dimensions or single-block dimensions need special handling. `_broadcast_block_id()` adjusts coordinates:
- Leading dimensions added by broadcasting: ignored (offset calculation)
- Single-block dimensions: always use block index 0

### Unreachable Expressions

After removing conflicting expressions, some expressions may become unreachable from the root. For example, in `mid + mid.T` where `mid = x + 1`, removing `mid` makes `x + 1` unreachable (it's only accessed through `mid`). These orphaned expressions are also removed.

## Files

- `dask/array/_array_expr/_blockwise.py` - Core implementation
  - `Blockwise._task()`, `_input_block_id()`, `_idx_to_block()` - base methods
  - `Elemwise._task()`, `_input_block_id()` - elemwise with broadcasting
  - `FusedBlockwise` - fused expression class
  - `optimize_blockwise_fusion_array()` - fusion algorithm
  - `_remove_conflicting_exprs()` - conflict detection
  - `is_fusable_blockwise()` - fusability check

- `dask/array/_array_expr/manipulation/_transpose.py`
  - `Transpose._task()`, `_input_block_id()` - axis permutation handling

- `dask/array/_array_expr/_creation.py`
  - `BroadcastTrick._task()` - creation operations (ones, zeros, etc.)

- `dask/array/_array_expr/_expr.py`
  - `ArrayExpr.optimize(fuse=True)` - integration point
  - `ArrayExpr.fuse()` - explicit fusion method

- `dask/array/tests/test_array_expr_fusion.py` - comprehensive tests

## What's Not Fused

- IO operations (`FromArray`, `FromDelayed`)
- Operations with `concatenate=True`
- Reductions (contracted dimensions - future work)
- Operations across rechunk boundaries

## Future Work

- Phase 3: Symbolic index substitution for contracted dimensions (reduction chains, matmul fusion)
