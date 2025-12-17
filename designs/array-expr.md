# Array Expression System Design

## Motivation

The array expression system reimplements Dask Array's computation model using
expression trees rather than direct graph construction. This enables:

1. **Query optimization**: Simplify expressions before graph generation
2. **Compact representation**: Expression trees are smaller than full graphs
3. **Multi-stage optimization**: Logical simplification, then physical lowering
4. **Consistency with DataFrame**: Same patterns as dask-expr for dataframes

## Core Principles

### Expression-Based Architecture

Operations create expression objects rather than task graphs. Graphs are
generated lazily when needed for execution.

```python
# Old pattern (core.py)
def some_operation(arr):
    graph = {...}  # Build graph immediately
    return Array(graph, ...)

# New pattern (array-expr)
def some_operation(arr):
    return new_collection(SomeExpr(arr.expr, params...))
```

### Separation of Concerns

Each expression class separates:
- **Metadata** (`_meta`, `dtype`): What type/shape the result will have
- **Chunks**: The partitioning structure
- **Graph generation** (`_layer`): How to actually compute it
- **Optimization** (`_simplify_down`, `_simplify_up`, `_lower`): Transformations

### Singleton Pattern

`SingletonExpr` ensures expressions with identical names are deduplicated.
This enables graph sharing and prevents duplicate computation.

## Key Components

### ArrayExpr (base class)
Location: `dask/array/_array_expr/_expr.py`

Required properties for subclasses:
- `chunks`: Tuple of tuples defining block sizes per dimension
- `_meta`: Small array representing dtype and array type
- `_name`: Unique identifier (usually `{prefix}-{token}`)
- `_layer()`: Returns dict of tasks

### Array (collection wrapper)
Location: `dask/array/_array_expr/_collection.py`

Wraps expressions for user-facing API. Delegates all properties to underlying
expression. Handles Dask protocol methods (`__dask_graph__`, etc.).

### Blockwise/Elemwise
Location: `dask/array/_array_expr/_blockwise.py`

Workhorse classes for operations that apply functions across aligned blocks.
`Elemwise` is a simplified interface for broadcasting element-wise operations.

### Reductions
Location: `dask/array/_array_expr/_reductions.py`

Uses tree reduction pattern: chunk -> combine -> aggregate
`PartialReduce` handles each level of the reduction tree.

## Optimization Flow

```
User Expression Tree
        |
        v
    simplify() -- Algebraic rewrites (push down projections, etc.)
        |
        v
    lower() -- Convert logical to physical operations
        |
        v
    simplify() -- Clean up physical plan
        |
        v
    fuse() -- Combine linear chains of blockwise operations
        |
        v
  __dask_graph__() -- Generate actual task graph
```

## Migration Pattern

When migrating an operation from traditional to expression-based:

1. **Identify the operation** in `dask/array/*.py`
2. **Extract metadata logic**: How are dtype/chunks/shape computed?
3. **Create expression class** in `dask/array/_array_expr/_*.py`:
   - Define `_parameters` and `_defaults`
   - Implement `chunks` property
   - Implement `_meta` property
   - Implement `_layer()` method
4. **Wire up the API** in `_collection.py` or appropriate module
5. **Add tests** to `_array_expr/tests/`
6. **Handle conditional import** if needed

## Constraints

- No custom `__init__` methods (use `_parameters`/`_defaults` pattern)
- Expressions should be stateless (use `@cached_property` for derived values)
- Graph generation deferred until `_layer()` called
- Tokenization must be deterministic for singleton deduplication

## Array-Specific Considerations

Unlike DataFrames, arrays have:
- **Multi-dimensional chunks**: Shape tracking is more complex
- **Broadcasting**: Chunks must be aligned across dimensions
- **Unknown chunks**: Some operations produce arrays with `nan` in chunks

These require careful handling in `unify_chunks_expr()` and operation-specific logic.
