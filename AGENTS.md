# AGENTS.md — Dask Project Guide for Coding Agents

## Project Overview

**Dask** is a flexible parallel computing library for dynamic task scheduling and analytics in Python. It provides high-level interfaces that mimic NumPy (arrays), Pandas (dataframes), and generic iterables (bags), with optional integration into the Distributed scheduler for cluster computing.

- **Language**: Python 3.10–3.14 (including free-threaded/3.14t)
- **Package name**: `dask`
- **License**: BSD 3-Clause
- **Repository**: https://github.com/dask/dask

---

## Architecture

### Core Concepts

1. **Task Graphs**: Dask represents computations as directed acyclic graphs (DAGs) of tasks. Each task is a callable (function + arguments), and edges represent data dependencies.

2. **High-Level Graph (HLG)**: A symbolic representation composed of *layers* (e.g., blockwise, data, callable) that can be optimized, fused, and inspected before materializing into a low-level task graph.

3. **Low-Level Graph (LLG)**: The materialized dict-of-tuples form passed to schedulers.

4. **Schedulers**:
   - `dask.threaded` — threaded scheduler (GIL-releasing for numpy/scipy)
   - `dask.multiprocessing` — process-based scheduler
   - `dask.local` — synchronous single-threaded (for testing)
   - `distributed` — remote cluster scheduler (separate repo)

5. **Expr System (dask-expr)**: A symbolic expression tree (`dask._expr.Expr`) that enables query planning and optimization. Enabled via `dataframe.query-planning` config.

### Key Modules

| Module | Purpose |
|---|---|
| `dask/base.py` | Core abstractions: `DaskMethodsMixin`, `compute()`, `persist()`, `optimize()`, `annotate()`, `visualize()` |
| `dask/core.py` | Low-level graph operations: `istask()`, `toposort()`, `get_dependencies()`, `flatten()` |
| `dask/highlevelgraph.py` | `HighLevelGraph`, `Layer` protocol, layer types (`MaterializedLayer`, `CallableLayer`) |
| `dask/blockwise.py` | `Blockwise` layer — the symbolic representation of element-wise operations (`atop`) |
| `dask/_expr.py` | Symbolic expression tree (`Expr`), `HLGExpr`, `LLGExpr`, `SingletonExpr` — the query-planning layer |
| `dask/_task_spec.py` | Modern task representation: `Task`, `DataNode`, `TaskRef`, `execute_graph()` |
| `dask/config.py` | Configuration system: YAML-based settings, `dask.config.get/set/set_options()` |
| `dask/optimization.py` | Graph optimization: `optimize()`, peephole optimizer, caching |
| `dask/delayed.py` | `delayed()` decorator for functional-style task creation |
| `dask/typing.py` | Type aliases: `Key`, `Graph`, `DaskCollection`, `SchedulerGetCallable` |
| `dask/utils.py` | Utility functions: `ensure_dict`, `funcname`, `key_split`, `normalize_url`, `apply_import` |
| `dask/rewrite.py` | Graph transformation utilities |
| `dask/order.py` | Task ordering heuristics |
| `dask/graph_manipulation.py` | `with_output_names`, `map_names`, `clone`, `with_replaced_keys` |
| `dask/dot.py` | Graphviz DOT visualization |

### Subpackages

| Subpackage | Purpose | Key Files |
|---|---|---|
| `dask/array/` | NumPy-compatible distributed arrays | `core.py` (`Array`), `slicing.py`, `reductions.py`, `blockwise.py`, `creation.py`, `linalg.py` |
| `dask/dataframe/` | Pandas-compatible dataframes | `core.py` (`DataFrame`, `Series`, `Index`), `groupby.py`, `shuffle.py`, `dispatch.py` |
| `dask/dataframe/dask_expr/` | dask-expr-based dataframe (query planning) | `_collection.py`, `_expr.py`, `_groupby.py`, `_shuffle.py`, etc. |
| `dask/bag/` | Distributed iterables (RDD-like) | `core.py` (`Bag`, `Item`) |
| `dask/array/_array_expr/` | dask-expr-based array (experimental) | `_collection.py`, `_blockwise.py`, `_reductions.py`, `_shuffle.py` |
| `dask/bytes/` | Byte-level I/O abstraction | — |
| `dask/diagnostics/` | Performance profiling | `profiler.py`, `watch.py` (requires bokeh) |
| `dask/widgets/` | Jupyter notebook widgets | `get_template.py` |

### Module Sizes (for navigation)

Largest files by line count:
- `dask/array/core.py` — 6,278 lines (Array class + methods)
- `dask/utils.py` — 2,361 lines (utilities)
- `dask/blockwise.py` — 1,569 lines (blockwise layer + atop)
- `dask/base.py` — 1,306 lines (compute/persist/annotate)
- `dask/_expr.py` — 1,420 lines (Expr tree)
- `dask/array/slicing.py` — 2,117 lines
- `dask/array/routines.py` — 2,703 lines (NumPy API compatibility)
- `dask/_task_spec.py` — 1,227 lines (modern task spec)

---

## Configuration

Configuration lives in `dask/dask.yaml` and `dask/dask-schema.yaml`. Key paths:

```yaml
dataframe:
  backend: "pandas"              # pandas, cudf, modin, etc.
  shuffle: {method: null}        # disk, tasks, p2p
  parquet: {metadata-task-size-local: 512}
  convert-string: null           # pyarrow string conversion
  query-planning: null           # enable dask-expr

array:
  backend: "numpy"               # numpy, cupy, etc.
  chunk-size: "128MiB"
  rechunk: {method: null}
  query-planning: null

optimization:
  annotations: {fuse: true}
  tune: {active: true}
  fuse: {active: null, ave-width: 1, max-width: null}

tokenize:
  ensure-deterministic: false

admin:
  async-client-fallback: null    # "sync" or "distributed"
```

Set config at runtime:
```python
import dask
with dask.config.set({"dataframe.shuffle.method": "p2p"}):
    ...
```

---

## Build & Test Setup

### Dev Environment (pixi)

The project uses **pixi** for dependency management. Install it from https://github.com/prefix-dev/pixi.

```bash
# Default environment (Python 3.14 + all optional deps)
pixi run test

# Minimum dependency tests
pixi run --environment mindeps-optional test

# Array-expr (experimental)
pixi run --environment py314 test-array-expr

# Run arbitrary Python commands
pixi run -- python -c 'print("Hello world!")'

# Linting
pixi run lint lint

# Doctests
pixi run doctest

# Specific module
pixi run test dask/tests/test_base.py
pixi run test dask/array/tests/test_array_core.py
pixi run test dask/dataframe/tests/test_dataframe.py

# Single test
pixi run test dask/tests/test_base.py::test_compute_depth

# Slow tests
pixi run test --runslow

# Full test suite
pixi run test-ci

# Test against all supported versions of NumPy, Pandas, and PyArrow
# This list may be fall out of sync; run `pixi ls -e <environment>` to double check
pixi run -e mindeps-optional ...  # NumPy 1.24, Pandas 2.0, PyArrow 16
pixi run -e py310 ...  # NumPy 1.26, Pandas 2.1, PyArrow 18
pixi run -e py311 ...  # NumPy 2.0, Pandas 2.2, PyArrow 20
pixi run -e py312 ...  # NumPy 2.2, Pandas 2.3, PyArrow 22
pixi run -e py313 ...  # NumPy 2.4, Pandas 3.0, PyArrow 24
pixi run -e nightly ...  # NumPy 2.6, Pandas 3.1, PyArrow 25
```

### pytest Configuration (from `pyproject.toml`)

- `timeout = 300` seconds per test
- `xfail_strict = true`
- Filter warnings as errors (with selective ignores)
- Markers: `network`, `slow`, `gpu`, `array_expr`, `normal_and_array_expr`

### Ruff (Linting)

- `line-length = 120`
- Extensions: `B`, `T10`, `ISC`, `TID`, `I`, `PLR`, `UP`, `FURB`
- Per-file ignores in `__init__.py` and `*_test.py`

---

## Testing Patterns

### Test File Organization

- Tests live alongside source: `dask/<module>/tests/test_<module>.py`
- Root tests: `dask/tests/test_*.py`
- Array tests: `dask/array/tests/test_*.py`
- Dataframe tests: `dask/dataframe/tests/test_*.py`
- `conftest.py` at root provides session-scoped fixtures

### Key Fixtures & Helpers

```python
# From dask/utils_test.py
assert_eq(a, b)      # Equality check with tolerance
assert_eq_types(a, b)
assert_dask_warning(func, cat, msg)

# From conftest.py
shuffle_method         # Parametrized fixture: ["disk", "tasks"]
allow_distributed_async_clients  # session-scoped config

# Skip markers
skip_with_pyarrow_strings   # Skip when pyarrow string conversion enabled
xfail_with_pyarrow_strings  # Xfail when pyarrow string conversion enabled
```

### Assertion Style

- Use `assert_eq()` for comparing dask collections vs pandas/numpy
- Prefer `pytest` markers over manual skip logic
- Tests use `pytest.warns()` for expected warnings
- Use `import_required()` for optional dependency tests

### Test Categories

1. **Unit tests** (`test_*.py`): Standard pytest files
2. **Doctests**: Run with `--doctest-modules`
3. **Slow tests**: Marked with `@pytest.mark.slow`, run with `--runslow`
4. **Array-expr tests**: Marked with `array_expr`, run with `--runarrayexpr`

---

## Code Style & Conventions

### Type Hints

- All new code should be fully typed
- `from __future__ import annotations` at the top of every file
- Use `TYPE_CHECKING` blocks for circular imports
- `typing.TypeAlias` for common aliases
- `typing_extensions` for features not in Python 3.10

### Docstrings

- Google-style docstrings with Parameters, Returns, See Also, Examples
- Examples must be runnable (they're doctests)
- `Examples` section is mandatory for public APIs

### Naming

- Private modules/functions prefixed with `_`
- Test files: `test_<module>.py`
- Internal helpers: `_helper`, `_impl`, `_dispatch`

### Import Organization

```python
from __future__ import annotations

import os                       # stdlib, alphabetically sorted
import numpy as np              # third-party
import dask                      # local
from dask.core import get       # local submodules
from dask.typing import Key     # local submodules
```

---

## Key Patterns for Contributors

### Key design patterns

- **IMPORTANT**: never call .compute() or .persist() in the middle of graph definition
  (e.g. in all methods of Array, Series, DataFrame, Bag, Delayed).
  The only place when the graph is materialized should be where the end user
  explicitly calls .compute() or .persist().
  When you are defining the graph, you must work with available metadata to infer
  the outputs.

### Adding a New Backend

1. Create entry point in `dask/<module>/backends.py` or `backends.py`
2. Inherit from `DaskBackendEntrypoint` in `dask/backends.py`
3. Register in `pyproject.toml` under `[project.entry-points."dask.<module>.backends"]`
4. Use `@register_inplace()` decorator for backend-dispatched functions

### Adding a New Layer Type

1. Subclass `Layer` from `dask/highlevelgraph.py`
2. Implement `__iter__()`, `__contains__()`, `keys()`, `values()`, `items()`
3. Optionally implement `__dask_graph__()` for materialization

### Adding a New Config Option

1. Add to `dask/dask.yaml` with a sensible default
2. Add to `dask/dask-schema.yaml` with type and description
3. Document in `dask/config.py` if needed
4. Wire up in relevant modules via `config.get()`

### Adding a New Expr (Query Planning)

1. Create an `Expr` subclass in `dask/dataframe/dask_expr/` or `dask/array/_array_expr/`
2. Define `_parameters`, `_defaults`, and computation methods
3. Register dispatch in the module's `__init__.py`
4. Add tests with `@pytest.mark.array_expr` or equivalent

---

## CI/CD

### GitHub Actions (`.github/workflows/`)

| Workflow | Purpose |
|---|---|
| `tests.yml` | Main test matrix (Python 3.10–3.14, multiple envs, scheduled reruns) |
| `conda.yml` | Conda packaging |
| `pre-commit.yml` | Linting on PRs |
| `upstream.yml` | Test against latest upstream packages |
| `release-*.yml` | Release automation |

### Matrix Coverage

- Python 3.10, 3.11, 3.12, 3.13, 3.14
- OS: Ubuntu (x86_64 & arm64), macOS, Windows
- Environments: default, mindeps-{array,dataframe,distributed,non-optional,optional}, nightly, spark, py314t (free-threaded)
- Scheduled: Twice daily for flaky test detection

### Codecov

- Config: `codecov.yml` — ignores `*/test_*.py` and `dask/_version.py`
- Coverage source: `dask/` package only

---

## Release Procedure

See `docs/release-procedure.md` for full details. Key steps:

1. **Changelog**: Run `git log` to generate entries, format with markdown
2. **Version bump**: Edit `pyproject.toml` — update `distributed` version pins
3. **Commit + Tag**: `git commit -am "Version YYYY.M.X"` → `git tag` → `git push --tags`
4. **PyPI**: `python -m build` → `twine upload dist/*`
5. **conda-forge**: Wait for bots or trigger manually
6. **Community issue**: Open tracking issue in `dask/community`

---

## Common Pitfalls & Gotchas

1. **Import cycles**: `dask.dataframe` imports from `dask.array._array_expr` for dispatch registration. Be mindful of import ordering.
2. **Query planning flag**: `array.query-planning` and `dataframe.query-planning` must be set before first import of `dask.array` / `dask.dataframe`. Cannot be changed mid-session.
3. **Free-threaded Python (3.14t)**: Several dependencies (matplotlib, pytables, zarr) lack free-threaded builds. Use `optional-gil` feature for these.
4. **Pyarrow string conversion**: `dataframe.convert-string` can cause test failures. Use `skip_with_pyarrow_strings` / `xfail_with_pyarrow_strings` markers.
5. **Distributed version pins**: The `pyproject.toml` pins a specific `distributed` version. Ensure this stays in sync with the `distributed` repo during releases.
6. **pixi vs pip**: The project is designed to be installable via both conda (pixi) and pip. Ensure `pyproject.toml` and `pixi.toml` have matching dependency versions.

---

## Key Files to Read First (by task)

| Task | Read Order |
|---|---|
| Understanding the scheduling model | `dask/base.py` → `dask/core.py` → `dask/_task_spec.py` |
| Understanding high-level graphs | `dask/highlevelgraph.py` → `dask/blockwise.py` → `dask/blockwise.py` |
| Adding array functionality | `dask/array/core.py` → `dask/array/slicing.py` → `dask/array/reductions.py` |
| Adding dataframe functionality | `dask/dataframe/core.py` → `dask/dataframe/groupby.py` → `dask/dataframe/shuffle.py` |
| Query planning (dask-expr) | `dask/_expr.py` → `dask/dataframe/dask_expr/_collection.py` → `dask/dataframe/dask_expr/_expr.py` |
| Configuration | `dask/config.py` → `dask/dask.yaml` → `dask/dask-schema.yaml` |
| Testing | `dask/utils_test.py` → `conftest.py` → `pyproject.toml` |
| CI/CD | `continuous_integration/` → `.github/workflows/tests.yml` |

---

## External Dependencies (Key)

| Dependency | Purpose | Minimum Version |
|---|---|---|
| `numpy` | Array backend, mathematical ops | 1.24+ |
| `pandas` | DataFrame operations | 2.0+ |
| `pyarrow` | PyArrow string conversion, parquet I/O | 16.0+ |
| `cloudpickle` | Serialization for distributed computing | 3.0+ |
| `dask.distributed` | Remote cluster scheduling | 2026.3.0 |
| `fsspec` | Filesystem abstraction for I/O | 2021.9+ |
| `toolz` | Functional programming utilities | 0.12+ |
| `pyyaml` | Configuration parsing | 5.4.1+ |
| `click` | CLI tools | 8.1+ |
| `partd` | Disk-based persistence | 1.4+ |

---

## Documentation

- **Source**: `docs/source/` (Sphinx, reStructuredText)
- **Build**: `cd docs && make html`
- **Online**: https://docs.dask.org
- **Requirements**: `docs/requirements-docs.txt`

---

## Quick Reference: Common Code Patterns

### Creating a Dask Collection

```python
import dask
import dask.array as da
import dask.dataframe as dd

# Array
x = da.ones(1000, chunks=(100,))
y = x + x.T

# Dataframe
df = dd.from_pandas(pd.DataFrame({"a": range(100)}), npartitions=4)
result = df.a.sum().compute()
```

### Scheduling

```python
# Default: use thread pool or distributed if available
result = dask.compute(x, y)

# Explicit scheduler
result = dask.threaded.get(graph, keys)
result = dask.multiprocessing.get(graph, keys)

# With annotations (for distributed)
with dask.annotate(retries=3):
    result = x.sum().compute()
```

### Configuration at Runtime

```python
import dask

# Set for current session
dask.config.set({"dataframe.shuffle.method": "p2p"})

# Context manager
with dask.config.set({"array.chunk-size": "256MiB"}):
    ...

# Check current value
size = dask.config.get("array.chunk-size")
```

### Testing Your Changes

```bash
# Run tests for the module you edited
pixi run test dask/array/tests/test_array_core.py::test_slicing

# Run all tests (full suite)
pixi run test-ci
```

## Contributing

You must never think or speak instead of the user in discussions, code reviews, or any
other interactions with other humans.

Before you open or update a PR, you must ask the user to explicitly confirm that they
fully reviewed, understood, and approved everything that you wrote. You must clarify
that the project maintainers consider this as non-negotiable.

## Releasing

A coding agent must NEVER create a new release.
