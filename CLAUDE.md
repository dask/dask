# Dask Development Guide

Dask is a parallel computing library for Python. It provides parallel implementations of NumPy arrays, pandas DataFrames, and more, using task graphs that can execute on a single machine or distributed cluster.

## Repository Structure

```
dask/
├── dask/                    # Main package
│   ├── array/               # Dask Array (parallel NumPy)
│   ├── dataframe/           # Dask DataFrame (parallel pandas)
│   ├── bag/                  # Dask Bag (parallel iterators)
│   ├── bytes/                # Byte handling utilities
│   ├── diagnostics/          # Profiling and visualization
│   ├── tests/                # Core tests
│   ├── base.py               # DaskMethodsMixin, compute, persist
│   ├── delayed.py            # Delayed interface
│   ├── _expr.py              # Expression base classes
│   └── ...
├── docs/source/              # RST documentation
├── continuous_integration/   # CI environment YAML files
├── designs/                  # Design documents (short, principles-focused)
├── plans/                    # Implementation plans (detailed, task-focused)
└── conftest.py               # Pytest configuration
```

Each collection subpackage (array, dataframe, bag) has its own `tests/` subdirectory.

## Development Setup

### Using uv (recommended)
```bash
uv venv
uv pip install -e ".[complete,test]"
```

### Using conda
```bash
conda env create -n dask-dev -f continuous_integration/environment-3.12.yaml
conda activate dask-dev
pip install --no-deps -e .
```

### Python Version Support
Dask supports Python 3.10 through 3.13.

## Testing

Dask uses pytest with a functional style (bare functions, not test classes).

### Running Tests
```bash
# Run all tests
pytest dask

# Run tests for a specific module
pytest dask/array/tests/test_slicing.py

# Run a specific test
pytest dask/array/tests/test_slicing.py::test_slice_1d

# Run in parallel
pytest dask -n auto

# Run slow tests (skipped by default)
pytest dask --runslow

# Run with array expression mode enabled
pytest dask/array --array-expr
```

### Testing Pattern: assert_eq

The primary testing pattern is comparing Dask results against NumPy/pandas:

```python
import numpy as np
import dask.array as da
from dask.array.utils import assert_eq

def test_sum():
    # Create NumPy array and equivalent Dask array
    x_np = np.array([1, 2, 3, 4, 5])
    x_da = da.from_array(x_np, chunks=2)

    # Test that Dask produces same result as NumPy
    assert_eq(x_da.sum(), x_np.sum())
    assert_eq(x_da + 1, x_np + 1)
```

Each collection has its own `assert_eq`:
- `dask.array.utils.assert_eq` - for arrays
- `dask.dataframe.utils.assert_eq` - for dataframes
- `dask.bag.utils.assert_eq` - for bags

Pass Dask collections directly to `assert_eq` rather than calling `.compute()` first - this enables additional validation checks.

### Test Markers

```python
@pytest.mark.slow          # Skipped unless --runslow passed
@pytest.mark.network       # Requires internet connection
@pytest.mark.gpu           # Requires GPU
```

### Doctest

Docstrings are tested automatically. Use `# doctest: +SKIP` for examples that shouldn't be tested:

```python
def my_function():
    """
    Examples
    --------
    >>> my_function()  # doctest: +SKIP
    """
```

Run doctests with:
```bash
pytest dask --doctest-modules
```

## Code Style

### Linting and Formatting

Dask uses pre-commit hooks with ruff, black, and mypy:

```bash
# Install hooks (runs automatically on commit)
pre-commit install

# Run manually on all files
pre-commit run --all-files
```

Configuration is in `pyproject.toml`:
- Line length: 120 characters
- Black for formatting
- Ruff for linting (replaces flake8, isort, pyupgrade)
- mypy for type checking

### Docstrings

Follow numpydoc style with Parameters, Returns, and Examples sections:

```python
def my_function(x, chunks):
    """Short one-line description.

    Longer description if needed, explaining behavior
    and any important details.

    Parameters
    ----------
    x : array-like
        Input array
    chunks : int or tuple
        Chunk sizes

    Returns
    -------
    Array
        Chunked dask array

    Examples
    --------
    >>> my_function([1, 2, 3], chunks=2)
    dask.array<...>
    """
```

## Continuous Integration

CI runs on GitHub Actions. Key workflows:
- `tests.yml` - Main test suite across Python versions and OS
- `upstream.yml` - Tests against development versions of NumPy, pandas, etc.

Trigger upstream tests by including `test-upstream` in a commit message.

## Documentation

Documentation uses Sphinx with RST files in `docs/source/`. Build locally:

```bash
cd docs
pip install -r requirements-docs.txt
make html
# Output in build/html/
```
