#!/usr/bin/env bash
set -o errexit


test_import () {
    echo "Create environment: python=$PYTHON $1"
    # Create an empty environment
    conda create -y -n test-imports -c conda-forge python=$PYTHON_VERSION pyyaml $1 > /dev/null 2>&1
    source activate test-imports > /dev/null 2>&1
    echo "python -c '$2'"
    python -c "$2"
    source deactivate > /dev/null 2>&1 || conda deactivate > /dev/null 2>&1
    conda env remove -n test-imports > /dev/null 2>&1
}

# Note: in setup.py, bag and delayed require cloudpickle, but it's omitted here as it is
# only a dependency for real-life usage and unit tests
test_import ""                                "import dask, dask.multiprocessing, dask.threaded, dask.optimization"
test_import "toolz"                           "import dask.delayed"
test_import "fsspec toolz partd"              "import dask.bag"
test_import "toolz numpy toolz"               "import dask.array"
test_import "fsspec numpy pandas toolz partd" "import dask.dataframe"
test_import "bokeh"                           "import dask.diagnostics"
test_import "distributed"                     "import dask.distributed"
