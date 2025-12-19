# Array-Expr Blockwise Fusion Design

## Summary

Add blockwise fusion to array-expr by:
1. Adding `_task(name, block_id)` method to `Blockwise`/`Elemwise` for per-block task generation
2. Creating `FusedBlockwise` class that collects fused expressions and uses `Task.fuse()`
3. Implementing fusion traversal algorithm (like dataframe's `optimize_blockwise_fusion`)
4. Override `ArrayExpr.fuse()` to run fusion at end of optimization pipeline

Start with Elemwise-only (Phase 1), extend to full Blockwise later.

## Motivation

Blockwise fusion combines multiple consecutive element-wise/blockwise operations into a single fused task, reducing scheduler overhead and intermediate materializations. For example, `(x + 1) * 2 + 3` should become a single task per block rather than three.

## Existing Approaches

### DataFrame Fusion
- Single dimension (partition index only)
- `Blockwise._task(name, index)` generates task for partition `i`
- `Fused` class collects expressions, calls `_task()` for each, then `Task.fuse()`
- `optimize_blockwise_fusion()` traverses expression tree to find fusable groups

### Traditional Array HLG Fusion
- Multi-dimensional block indices
- `Blockwise` layer stores: `output_indices`, `indices` (input name → index mapping), `task`
- `rewrite_blockwise()` performs **symbolic index substitution** to combine layers
- Handles contracted dimensions (e.g., `j` in matrix multiply `ij,jk→ik`)
- Uses `Task.fuse()` at the end

### Key Differences

| Aspect | DataFrame | Array HLG | Array-Expr (current) |
|--------|-----------|-----------|---------------------|
| Task generation | `_task(name, i)` | `_layer()` via `core_blockwise()` | `_layer()` via `core_blockwise()` |
| Index dimension | 1D | N-D | N-D |
| Fusion class | `Fused` | `Blockwise` (rewritten) | Not implemented |
| Index handling | Trivial | Symbolic substitution | N/A |

## Proposed Design

### Option A: Add `_task()` Method + Fused Class (Recommended)

Follow the dataframe pattern but extend for N-D indices.

**1. Add `_task()` method to `Blockwise`:**

```python
class Blockwise(ArrayExpr):
    def _task(self, name: Key, block_id: tuple[int, ...]) -> Task:
        """Generate task for a specific output block."""
        # Map output block_id to input block coordinates
        args = []
        for inp_array, inp_ind in self.arginds:
            if inp_ind is None:
                # Literal argument
                args.append(inp_array)
            else:
                # Array argument - compute its block_id
                inp_block_id = self._map_block_id(block_id, inp_ind)
                args.append(TaskRef((inp_array.name, *inp_block_id)))
        return Task(name, self.func, *args, **self.kwargs)

    def _map_block_id(self, out_block_id, inp_indices):
        """Map output block coordinates to input block coordinates."""
        # out_ind = (0, 1) for 2D output with indices reversed
        # inp_indices might be (1, 0) for transpose
        # out_block_id = (2, 3)
        # result = (3, 2) - swap to match input's index order
        out_to_dim = {idx: i for i, idx in enumerate(self.out_ind)}
        return tuple(
            out_block_id[out_to_dim[idx]] if idx in out_to_dim else 0
            for idx in inp_indices
        )
```

**2. Create `FusedBlockwise` class:**

```python
class FusedBlockwise(ArrayExpr):
    """Fused blockwise operations for arrays."""
    _parameters = ["exprs"]  # + external dependencies as operands

    @property
    def _meta(self):
        return self.exprs[0]._meta

    @property
    def chunks(self):
        return self.exprs[0].chunks

    def _task(self, name: Key, block_id: tuple[int, ...]) -> Task:
        internal_tasks = []
        for expr in self.exprs:
            # Handle broadcasting: single-block dims use 0
            expr_block_id = self._adjust_block_id(expr, block_id)
            subname = (expr._name, *expr_block_id)
            t = expr._task(subname, expr_block_id)
            internal_tasks.append(t)
        return Task.fuse(*internal_tasks, key=name)

    def _layer(self):
        result = {}
        for block_id in product(*[range(n) for n in self.numblocks]):
            key = (self._name, *block_id)
            result[key] = self._task(key, block_id)
        return result
```

**3. Implement fusion algorithm:**

```python
def optimize_blockwise_fusion_array(expr):
    """Find and fuse blockwise operations in array expression tree."""
    # Similar to dataframe but with chunk alignment checks

    def is_fusable(expr):
        return isinstance(expr, Blockwise) and not isinstance(expr, FromArray)

    def chunks_compatible(a, b):
        # Check if chunks align (same shape or broadcastable)
        return a.chunks == b.chunks or _is_broadcast_compatible(a, b)

    # ... traverse tree, find groups, create FusedBlockwise
```

**Advantages:**
- Follows established dataframe pattern
- Clean separation of concerns
- Per-block task generation is explicit

**Challenges:**
- Need to handle contracted dimensions (dummy indices)
- Broadcasting logic for single-block dimensions

### Option B: Symbolic Rewriting (Like HLG)

Port the `rewrite_blockwise` approach to expressions.

**1. Create lightweight `BlockwiseInfo` for fusion:**

```python
@dataclass
class BlockwiseInfo:
    output: str
    output_indices: tuple
    task: Task  # Template task with placeholders
    indices: list[tuple[str, tuple | None]]
    numblocks: dict
```

**2. Extract info from Blockwise expressions:**

```python
def blockwise_to_info(expr: Blockwise) -> BlockwiseInfo:
    # Convert expression to symbolic form for fusion
```

**3. Use existing `rewrite_blockwise`:**

```python
def fuse_blockwise_exprs(exprs: list[Blockwise]) -> Blockwise:
    infos = [blockwise_to_info(e) for e in exprs]
    fused_info = rewrite_blockwise(infos)
    return FusedBlockwise.from_info(fused_info, exprs)
```

**Advantages:**
- Reuses battle-tested HLG fusion logic
- Handles complex index substitution correctly

**Challenges:**
- HLG `rewrite_blockwise` operates on `Blockwise` layers, not expressions
- Need adapter layer between expression and HLG concepts
- More complex integration

## Recommendation: Option A with Careful Index Handling

Option A is cleaner and more consistent with the expression system. The key challenge is handling multi-dimensional indices correctly. Here's the detailed approach:

### Index Mapping Strategy

For `Blockwise` with:
- `out_ind = (0, 1)` (output dimensions)
- Input array `x` with `ind = (1, 0)` (transposed)

Given output `block_id = (i, j)`:
- Dimension 0 in output corresponds to index `0`
- Dimension 1 in output corresponds to index `1`
- Input `x` wants indices `(1, 0)` = (j, i)

```python
def _map_block_id(self, out_block_id, inp_indices):
    # Build mapping from symbolic index to output dimension
    idx_to_out_dim = {idx: dim for dim, idx in enumerate(self.out_ind)}

    result = []
    for idx in inp_indices:
        if idx in idx_to_out_dim:
            # This index appears in output
            result.append(out_block_id[idx_to_out_dim[idx]])
        elif idx in self.new_axes:
            # New axis - always block 0
            result.append(0)
        else:
            # Contracted dimension - need special handling
            # This shouldn't happen in simple fusion
            raise ValueError(f"Contracted index {idx} not supported in fusion")
    return tuple(result)
```

### Contracted Dimensions

Operations like `tensordot(x[ij], y[jk]) -> z[ik]` have contracted dimension `j`. These can't be fused in the simple model because each output block needs ALL blocks along the contracted dimension.

**Rule:** Only fuse operations where all input indices appear in the output indices (element-wise/non-reducing blockwise).

### Broadcasting

For broadcasting, single-block dimensions always use block index 0:

```python
def _adjust_block_id_for_broadcast(self, dep, block_id):
    if dep.numblocks == (1,) * dep.ndim:
        return (0,) * dep.ndim
    # Handle per-dimension broadcasting
    return tuple(
        0 if nb == 1 else block_id[i]
        for i, nb in enumerate(dep.numblocks)
    )
```

## Implementation Plan

### Phase 1: Add `_task()` infrastructure

Current `_layer()` delegates to `core_blockwise()` from HLG. Two options:

**Option A (Conservative):** Add `_task()` alongside existing `_layer()`. `FusedBlockwise` uses `_task()`, unfused operations continue using `core_blockwise()`. Lower risk, but two code paths.

**Option B (Clean):** Replace `_layer()` with `_task()`-based generation. Single code path, cleaner, but larger refactor.

Recommend starting with **Option A**, migrate to **Option B** once fusion is working.

Steps:
1. Add `_task(name, block_id)` method to `Elemwise` (simpler case - all indices same)
2. `_layer()` unchanged for now (still uses `core_blockwise()`)
3. `FusedBlockwise._layer()` iterates blocks and calls `_task()`:
   ```python
   def _layer(self):
       result = {}
       for block_id in product(*[range(n) for n in self.numblocks]):
           key = (self._name, *block_id)
           result[key] = self._task(key, block_id)
       return result
   ```
4. Later: refactor base `Blockwise._layer()` to also use `_task()`

### Phase 2: Implement `FusedBlockwise`
1. Create `FusedBlockwise` class
2. Handle metadata propagation
3. Implement `_task()` that collects and fuses

### Phase 3: Fusion algorithm
1. Port `optimize_blockwise_fusion` from dataframe
2. Adapt for chunk/shape compatibility checks
3. Handle broadcasting cases

### Phase 4: Testing
1. Port relevant tests from `test_atop.py`
2. Add array-specific fusion tests
3. Test edge cases (broadcasting, different chunk sizes)

## What NOT to Fuse

- Operations with contracted dimensions (reductions, tensordot)
- IO operations (FromArray, FromDelayed)
- Operations requiring different iteration patterns

## Test Cases

Adapted from `test_atop.py`:

```python
def test_simple_fusion():
    x = da.ones((10, 10), chunks=5)
    y = (x + 1) * 2
    # Should fuse to 4 tasks (one per block)

def test_diamond_fusion():
    x = da.ones((10,), chunks=5)
    y = x + 1
    z = x * 2
    w = y + z
    # All should fuse since they share same chunks

def test_transpose_fusion():
    x = da.ones((10, 20), chunks=(5, 10))
    y = x.T + 1
    # Transpose changes block mapping but should still fuse

def test_broadcast_fusion():
    x = da.ones((10, 10), chunks=5)
    y = da.ones((10,), chunks=5)
    z = x + y  # y broadcasts
    # Should fuse correctly with broadcast handling

def test_no_fusion_reduction():
    x = da.ones((10, 10), chunks=5)
    y = x.sum(axis=0)
    z = y + 1
    # sum has contracted dimension - shouldn't fuse through it
```

## Learnings from HLG Test Cases

The `test_atop.py` test cases reveal sophisticated index handling:

1. **Contracted dimensions** (Case 3): `sum(a[ij])→b[i]` then `inc(b[k])` fuses to input `a[kA]` where `A` is the renamed contracted dim
2. **Same array, different patterns** (Case 5): `transpose(a[ij])→b[ji]` then `add(a[ij], b[ij])` needs both `a[ij]` and `a[ji]`
3. **Index substitution** (Case 7): transpose `a[ji]→b[ij]` used as `b[ik]` in matmul requires substituting to `a[ki]`

## Phased Implementation

### Phase 1: Elemwise-only Fusion (MVP)
Fuse only `Elemwise` operations where all arrays share the same index pattern.

**Constraints:**
- All operations must be `Elemwise` (same indices for all dims)
- Chunks must align or broadcast (single-block dims)
- No contracted dimensions

**Covers:** `(x + 1) * 2 + 3`, element-wise chains

**Implementation detail - Elemwise `_task()` is simple:**

For Elemwise, all array inputs have indices `(n-1, n-2, ..., 1, 0)` (reversed range of ndim). Given output `block_id = (i, j, k)`, each input array uses the same block_id (adjusted for broadcasting).

```python
class Elemwise(Blockwise):
    def _task(self, name: Key, block_id: tuple[int, ...]) -> Task:
        args = []
        for arg in self.elemwise_args:
            if is_scalar_for_elemwise(arg):
                args.append(arg)
            else:
                # Adjust for broadcasting: use 0 for single-block dims
                arg_block_id = tuple(
                    0 if nb == 1 else block_id[i]
                    for i, nb in enumerate(arg.numblocks)
                )
                args.append(TaskRef((arg.name, *arg_block_id)))
        return Task(name, self.func, *args, **self.kwargs)
```

**Fusion algorithm (Phase 1):**

```python
def is_fusable_elemwise(expr):
    return isinstance(expr, Elemwise) and not isinstance(expr, (FromArray, ...))

def optimize_blockwise_fusion_array(expr):
    # Build dependency graph of Elemwise operations
    dependencies = {}  # name -> set of elemwise dependency names
    dependents = {}    # name -> set of elemwise dependent names
    expr_map = {}      # name -> expr

    # Traverse to collect Elemwise nodes
    for node in expr.walk():
        if is_fusable_elemwise(node):
            expr_map[node._name] = node
            dependencies[node._name] = {
                dep._name for dep in node.dependencies()
                if is_fusable_elemwise(dep)
            }

    # Reverse mapping
    dependents = reverse_dict(dependencies)

    # Find roots (no Elemwise dependents)
    roots = [name for name, deps in dependents.items() if not deps]

    # For each root, collect fusable group
    for root_name in roots:
        group = []
        stack = [expr_map[root_name]]
        while stack:
            node = stack.pop()
            if node._name in [g._name for g in group]:
                continue
            group.append(node)
            for dep_name in dependencies[node._name]:
                dep = expr_map[dep_name]
                # Check: all dependents of dep are in our group
                if dependents[dep_name] <= {g._name for g in group} | {node._name}:
                    stack.append(dep)

        if len(group) > 1:
            # Create FusedBlockwise and substitute
            external_deps = collect_external_deps(group)
            fused = FusedBlockwise(group, *external_deps)
            expr = expr.substitute(group[0], fused)

    return expr
```

### Phase 2: Simple Blockwise Fusion
Extend to `Blockwise` where output indices match input indices (permutations ok).

**Covers:** Transpose chains, axis swaps

### Phase 3: Full Index Substitution
Implement symbolic index renaming for contracted dimensions.

**Covers:** Reduction chains, matmul fusion

## Pipeline Integration

Fusion runs in `fuse()` method (like dataframe), called at end of `optimize_until()`:
```
simplify → tune → lower → simplify → fuse
```

Override in `ArrayExpr`:
```python
class ArrayExpr(SingletonExpr):
    def fuse(self):
        return optimize_blockwise_fusion_array(self)
```

## Annotations

Follow dataframe approach (skip for Phase 1, copy logic later if needed).

## Testing Strategy

### Unit Tests for `_task()` Method

Test that `_task()` generates correct tasks for individual blocks:

```python
def test_elemwise_task():
    x = da.ones((6, 8), chunks=(3, 4))
    y = x + 1
    expr = y.expr.lower_completely()

    # Block (0, 0) should reference x's block (0, 0)
    task = expr._task(("add", 0, 0), (0, 0))
    assert TaskRef(("ones", 0, 0)) in task.dependencies

    # Block (1, 1) should reference x's block (1, 1)
    task = expr._task(("add", 1, 1), (1, 1))
    assert TaskRef(("ones", 1, 1)) in task.dependencies

def test_elemwise_task_broadcast():
    x = da.ones((6, 8), chunks=(3, 4))  # 2x2 blocks
    y = da.ones((8,), chunks=(4,))       # 1x2 blocks (broadcasts)
    z = x + y
    expr = z.expr.lower_completely()

    # y broadcasts: block (0, 0) uses y's block (0,), block (1, 0) also uses y's (0,)
    task = expr._task(("add", 0, 0), (0, 0))
    # x uses (0, 0), y uses (0,)
    task = expr._task(("add", 1, 0), (1, 0))
    # x uses (1, 0), y still uses (0,) - first dim broadcasts
```

### Fusion Algorithm Tests

Adapted from `test_atop.py` patterns:

```python
def test_simple_chain():
    """Case 0/6 analog: linear chain fuses"""
    x = da.ones((10,), chunks=5)
    y = (x + 1) * 2
    fused = y.expr.optimize(fuse=True)

    # Should have 2 fused tasks (one per chunk)
    graph = dict(fused.__dask_graph__())
    assert len([k for k in graph if k[0].startswith("mul")]) == 2

def test_diamond():
    """Case 1/8/9 analog: diamond pattern fuses"""
    x = da.ones((10,), chunks=5)
    a = x + 1
    b = x * 2
    c = a + b
    fused = c.expr.optimize(fuse=True)

    # All should fuse: single set of output tasks
    graph = dict(fused.__dask_graph__())
    output_tasks = [k for k in graph if isinstance(k, tuple) and k[0].startswith("add")]
    assert len(output_tasks) == 2

def test_different_sources():
    """Case 4 analog: different sources with outer-product-like pattern"""
    x = da.ones((10,), chunks=5)
    y = da.zeros((10,), chunks=5)
    a = x + 1
    b = y * 2
    c = a + b
    fused = c.expr.optimize(fuse=True)
    assert_eq(c, np.ones(10) + 1 + np.zeros(10) * 2)

def test_broadcast_fusion():
    """Broadcast dimensions handled correctly"""
    x = da.ones((10, 10), chunks=5)
    y = da.ones((10,), chunks=5)  # broadcasts
    z = (x + y) * 2
    fused = z.expr.optimize(fuse=True)
    assert_eq(z, (np.ones((10, 10)) + np.ones(10)) * 2)

def test_no_fusion_through_reduction():
    """Reductions block fusion"""
    x = da.ones((10, 10), chunks=5)
    y = x.sum(axis=0)  # reduction
    z = y + 1
    # sum and +1 should NOT fuse (sum has contracted dimension)
    # But we should still compute correctly
    assert_eq(z, np.ones((10, 10)).sum(axis=0) + 1)

def test_fusion_preserves_correctness():
    """Fused and unfused compute same result"""
    x = da.random.random((100, 100), chunks=25)
    y = ((x + 1) * 2 - 3) / 4

    unfused = y.expr.optimize(fuse=False)
    fused = y.expr.optimize(fuse=True)

    assert_eq(unfused, fused)
```

### Integration with Existing Tests

The existing test in `test_array_core.py`:

```python
@pytest.mark.xfail(da._array_expr_enabled(), reason="blockwise fusion not implemented")
def test_blockwise_fusion():
    ...
```

Should pass after implementation. Also check `test_array_creation_blockwise_fusion` in `test_optimization.py`.

### Task Count Verification

Key metric: fused graph should have fewer tasks:

```python
def test_task_count_reduced():
    x = da.ones((10, 10), chunks=5)
    y = (x + 1) * 2 + 3  # 3 elemwise ops

    unfused_graph = y.expr.optimize(fuse=False).__dask_graph__()
    fused_graph = y.expr.optimize(fuse=True).__dask_graph__()

    # Unfused: 4 blocks × 4 ops (ones, +1, *2, +3) = 16 tasks
    # Fused: 4 blocks × 2 ops (ones, fused) = 8 tasks
    assert len(fused_graph) < len(unfused_graph)
```
