# Array Expression Pushdown Optimization

## Motivation

Array computations often produce data that is immediately filtered, sliced, or
reordered. Without optimization, we compute everything then discard what we
don't need:

```python
# User writes:
result = (x + y).sum(axis=0)[:5]

# Naive execution:
# 1. Compute x + y for ALL chunks
# 2. Sum ALL columns
# 3. Take first 5 elements, discard the rest
```

Pushdown optimization restructures the expression tree to compute only what's
needed:

```python
# After optimization:
# 1. Slice x[:, :5] and y[:, :5]
# 2. Compute (x + y) on sliced inputs
# 3. Sum the 5 columns

# Result: Process 5 column chunks instead of 100
```

This applies to multiple "projection" operations:
- **Slice**: `x[5:10, :]` - rectangular selection
- **Shuffle/Take**: `x[[3, 1, 4], :]` - reordering/selection by indices
- **Rechunk**: `x.rechunk(...)` - changing chunk boundaries
- **Transpose**: `x.T` - reordering dimensions

## Core Principle: Push Toward Leaves

These operations should travel down the expression tree toward IO/creation
nodes. Each expression type defines how to let operations pass through (or
blocks them when semantically unsafe).

```
User writes:           After pushdown:
Slice                  Elemwise
  |                      /    \
Elemwise    -->     Slice      Slice
  / \                 |          |
 A   B               A          B
```

The key insight: if an operation (like Elemwise) preserves the correspondence
between input and output positions, projections can push through it by applying
the same projection to each input.

## Expression Categories

Expressions fall into categories based on how projections interact with them:

### 1. Transparent Operations

The projection passes through unchanged to all inputs. The operation itself
doesn't care about position - it just combines corresponding elements.

**Examples**: Elemwise (add, multiply, sin, etc.)

```python
# Slice through add
(x + y)[:5] -> x[:5] + y[:5]

# Shuffle through multiply
(x * y)[[3,1,4], :] -> x[[3,1,4], :] * y[[3,1,4], :]
```

### 2. Transforming Operations

The projection pushes through but must be remapped. The operation reorders
dimensions or indices, so the projection must be adjusted accordingly.

**Examples**: Transpose, Concatenate, Stack

```python
# Slice through transpose - must reorder slice indices
x.T[:, 5:10]  ->  x[5:10, :].T
#   ^-- axis 1      ^-- becomes axis 0 after transpose reversal

# Shuffle through transpose - remap the shuffle axis
x.T[[1,3], :]  ->  x[:, [1,3]].T
#   ^-- axis 0       ^-- maps to axis 1 in input
```

### 3. Blocking Operations

The projection cannot pass through. The operation fundamentally changes the
relationship between positions in ways that would make pushdown incorrect.

**Blocking conditions**:
- `new_axes`: Operation creates new dimensions (can't slice what doesn't exist yet)
- `adjust_chunks`: Operation modifies chunk sizes (projection boundaries don't match)
- Shuffle on concatenation axis (indices span multiple arrays unpredictably)

```python
# Can't push slice through new_axes
x[:, None, :][0]  # The None creates axis - what would slice mean on input?

# Can't push shuffle through adjust_chunks
x.map_blocks(func, chunks=(1,))[[0, 2]]  # Output positions don't map to input
```

### 4. Terminal Operations

The projection is absorbed by the operation. Instead of pushing further, we
modify the operation's parameters to produce less data.

**Examples**: IO (FromArray), BroadcastTrick

```python
# Slice absorbed by IO
FromArray(source)[5:10]  ->  FromArray(source[5:10])

# Rechunk absorbed by IO
FromArray(source, chunks=old).rechunk(new)  ->  FromArray(source, chunks=new)
```

## Projection Types and Their Semantics

Different indexing operations have different characteristics that affect how
they push through expressions:

### SliceSlicesIntegers (Basic Slicing)

The workhorse - handles `x[5:10, :, 3]` with slices and integer indices.

**Characteristics**:
- Rectangular selection (independent per axis)
- Preserves relative order within selection
- Known output shape (computable from slice parameters)
- Integer indices reduce dimensions

**Pushdown**: Most complete implementation. Pushes through nearly everything.

### Shuffle (Take/Fancy Indexing)

Handles `x[[3, 1, 4], :]` - selection and reordering by integer arrays.

**Characteristics**:
- Can reorder elements (out-of-order indices like `[3, 1, 0]`)
- Selection on one axis at a time
- Known output shape (length of index array)
- Preserves other axes unchanged

**Pushdown**: Works through Elemwise, Transpose, Blockwise, Concatenate, Stack.
The same indexer applied to all inputs preserves the reordering relationship.

```python
# Out-of-order indices work correctly
(x + y)[[2, 0, 1], :]  ->  x[[2, 0, 1], :] + y[[2, 0, 1], :]
# Both inputs reordered identically, so addition still aligns
```

### VIndexArray (Point Indexing)

Handles `x.vindex[[1,3], [2,4]]` - vectorized point selection.

**Characteristics**:
- Selects specific (i, j) pairs, not rectangles
- Output shape from index arrays, not input shape
- More complex pushdown requirements

**Pushdown**: Limited. Requires same indices on all inputs.

### BooleanIndexFlattened (Mask Indexing)

Handles `x[bool_mask]` - selection by boolean array.

**Characteristics**:
- Unknown output size until mask is evaluated (nan chunks)
- Flattens result (loses original shape structure)
- Mask must align with array being indexed

**Pushdown**: Very limited due to unknown output shape.

## Axis Remapping Through Transformations

When pushing through operations that reorder dimensions, we must remap which
axis the projection applies to.

### Transpose Remapping

Transpose has an `axes` tuple where `axes[i]` tells us which input axis becomes
output axis `i`. To push a projection on output axis `a` through transpose:

```python
# For Shuffle through Transpose:
output_axis = self.axis  # The axis we're shuffling in the output
input_axis = axes[output_axis]  # Map to corresponding input axis

# Example: axes=(1, 0) means transpose
# Shuffle on output axis 0 -> shuffle on input axis axes[0] = 1
```

### Stack/Concatenate Remapping

Stack adds a new dimension. When pushing through, projections on axes after
the stack axis must be decremented:

```python
# Stack on axis 0 creates new dimension
# Projection on output axis 2 -> input axis 1 (one fewer dimension)
if shuffle_axis > stack_axis:
    input_axis = shuffle_axis - 1
else:
    input_axis = shuffle_axis
```

### Blockwise Index Remapping

Blockwise operations use symbolic indices (0, 1, 2, ...) to describe how
input dimensions map to output dimensions. The `out_ind` tuple maps output
positions to these symbols.

```python
# For Shuffle through Blockwise:
shuffle_ind = out_ind[axis]  # Get the symbolic index for shuffle axis

# For each input array:
for arr, ind in inputs:
    if shuffle_ind in ind:
        input_axis = ind.index(shuffle_ind)  # Find position of that symbol
        shuffle this input on input_axis
```

## Blocking Conditions in Detail

### new_axes

When a Blockwise operation creates new dimensions via `new_axes`, those
dimensions don't exist in the input. Projections on new axes cannot push
through.

```python
# new_axes={2: 5} means axis 2 is new with size 5
# Can't push: what would x[:, :, 3] mean when axis 2 doesn't exist in x?
if shuffle_ind in blockwise.new_axes:
    return None  # Block pushdown
```

### adjust_chunks

When a Blockwise operation modifies chunk sizes via `adjust_chunks`, the
correspondence between output and input chunk boundaries is broken.

```python
# adjust_chunks={0: lambda n: n*2} doubles chunk sizes on axis 0
# Output chunk 0 doesn't map cleanly to input chunk 0
if shuffle_ind in blockwise.adjust_chunks:
    return None  # Block pushdown
```

### Concatenation Axis

For Concatenate/Stack, pushing projections through the concatenation axis
would require splitting indices across multiple arrays - complex and
error-prone.

```python
# concat([a, b], axis=0) where a has 10 rows, b has 10 rows
# shuffle[[5, 15], :] would need: a[[5], :] and b[[5], :]
# This splitting is complex; we block it
if shuffle_axis == concat_axis:
    return None
```

## Rechunk Pushdown

Rechunk is special - it changes chunk boundaries without changing values.
Pushing rechunk toward leaves can:

1. **Fuse with IO**: `FromArray(x).rechunk(new)` -> `FromArray(x, chunks=new)`
2. **Fuse consecutive rechunks**: `x.rechunk(a).rechunk(b)` -> `x.rechunk(b)`
3. **Push through elemwise**: Rechunk all inputs identically

```python
# Rechunk through elemwise
(x + y).rechunk((10, 10))  ->  x.rechunk((10, 10)) + y.rechunk((10, 10))
```

Rechunk through transpose requires remapping the chunk specification:

```python
# axes=(1, 0) means transpose
# Output chunks (a, b) -> input chunks (b, a)
new_chunks = tuple(chunks[i] for i in axes)
```

## Testing Strategy

Three levels of testing ensure correctness:

### 1. Structure Tests

Verify the optimization actually happens by comparing expression names:

```python
def test_shuffle_pushes_through_elemwise():
    x = da.ones((10,), chunks=5)
    y = da.ones((10,), chunks=5)

    result = (x + y)[[0, 2, 4]]
    expected = x[[0, 2, 4]] + y[[0, 2, 4]]

    # Same structure after optimization
    assert result.expr.simplify()._name == expected.expr.simplify()._name
```

### 2. Task Count Tests

Verify optimization reduces work:

```python
def test_shuffle_reduces_tasks():
    x = da.ones((100,), chunks=10)

    full = (x * 2).sum()
    sliced = (x * 2)[:10].sum()

    # Sliced version should have fewer tasks
    assert len(sliced.optimize().__dask_graph__()) < len(full.optimize().__dask_graph__())
```

### 3. Correctness Tests

Always verify results match NumPy:

```python
def test_shuffle_correctness():
    x_np = np.random.random((10, 10))
    x_da = da.from_array(x_np, chunks=5)

    indices = [3, 1, 4]
    assert_eq(x_da[indices, :], x_np[indices, :])
```

## Implementation Pattern

Each pushdown is implemented as a `_simplify_down` method that:

1. Checks if the child expression is a pushdown target
2. Checks blocking conditions
3. Remaps axes/indices as needed
4. Constructs the pushed-down expression

```python
def _simplify_down(self):
    # Dispatch based on child type
    if isinstance(self.array, Elemwise):
        return self._pushdown_through_elemwise()
    if isinstance(self.array, Transpose):
        return self._pushdown_through_transpose()
    if isinstance(self.array, Blockwise):
        return self._pushdown_through_blockwise()
    # ... etc

def _pushdown_through_blockwise(self):
    blockwise = self.array
    shuffle_ind = blockwise.out_ind[self.axis]

    # Check blocking conditions
    if blockwise.new_axes and shuffle_ind in blockwise.new_axes:
        return None
    if blockwise.adjust_chunks and shuffle_ind in blockwise.adjust_chunks:
        return None

    # Remap and construct
    new_args = []
    for arr, ind in blockwise.args:
        if shuffle_ind in ind:
            input_axis = ind.index(shuffle_ind)
            new_args.append(Shuffle(arr, self.indexer, input_axis, ...))
        else:
            new_args.append(arr)

    return Blockwise(..., *new_args)
```

## Current Implementation Status

### Fully Implemented

| Projection | Target | Notes |
|------------|--------|-------|
| Slice | Elemwise, Transpose, Blockwise, Concatenate, Stack, BroadcastTo, IO, Reductions | Most complete |
| Shuffle | Elemwise, Transpose, Blockwise, Concatenate, Stack | Good coverage |
| Rechunk | Rechunk, Transpose, Elemwise, IO, Concatenate | Chunk boundary optimization |
| Transpose | Transpose, Elemwise | Dimension reordering |

### Not Yet Implemented

| Projection | Challenge |
|------------|-----------|
| SlicesWrapNone | Need to separate None-insertion from slicing |
| VIndexArray | Point selection semantics differ from rectangular |
| BooleanIndexFlattened | Unknown output shape (nan chunks) |

## Future Directions

### Unified Projection Interface

Currently each projection type implements its own pushdown logic. A unified
interface could reduce duplication:

```python
class Projection(ArrayExpr):
    """Base for operations that select/reorder elements."""

    def can_push_through(self, expr) -> bool:
        """Whether this projection can push through expr."""

    def remap_for_child(self, expr) -> 'Projection':
        """Return projection adjusted for child's coordinate system."""
```

### Automatic Blocking Detection

Rather than manually checking `new_axes` and `adjust_chunks`, we could
analyze the Blockwise specification to automatically determine which axes
are "stable" for pushdown.

### Cross-Projection Optimization

Combining multiple projections before pushing:

```python
# Currently two separate pushdowns
x.T[[1,3], :][:, 5:10]

# Could combine into single projection
x[5:10, [1,3]].T
```
