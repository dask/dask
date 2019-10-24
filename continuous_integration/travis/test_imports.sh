#!/usr/bin/env bash
set -o errexit


test_import () {
    # Create an empty environment
    conda create -y -n test-imports -c conda-forge python=$PYTHON $1
    source activate test-imports
    python -c "$2"
    conda deactivate
    conda env remove -n test-imports
}

# Note: in setup.py, bag and delayed require cloudpickle, but it's omitted here and in
# travis-reduced-deps.yaml as it is only a dependency for real-life usage
test_import ""                                "import dask, dask.multiprocessing, dask.threaded, dask.optimization"
test_import "toolz"                           "import dask.delayed"
test_import "fsspec toolz partd"              "import dask.bag"
test_import "toolz numpy toolz"               "import dask.array"
test_import "fsspec numpy pandas toolz partd" "import dask.dataframe"
test_import "bokeh"                           "import dask.diagnostics"
test_import "distributed"                     "import dask.distributed"
