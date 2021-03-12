#!/usr/bin/env bash
set -o errexit


test_import () {
    echo "Create environment: python=$PYTHON_VERSION $1"
    # Create an empty environment
    conda create -q -y -n test-imports -c conda-forge python=$PYTHON_VERSION pyyaml fsspec toolz partd cloudpickle $1
    conda activate test-imports
    pip install -e .
    echo "python -c '$2'"
    python -c "$2"
    conda deactivate
    conda env remove -n test-imports
}

test_import ""                                "import dask, dask.base, dask.multiprocessing, dask.threaded, dask.optimization, dask.bag, dask.delayed, dask.graph_manipulation"
test_import "numpy"                           "import dask.array"
test_import "pandas"                          "import dask.dataframe"
test_import "bokeh"                           "import dask.diagnostics"
test_import "distributed"                     "import dask.distributed"
