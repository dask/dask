#!/usr/bin/env bash
set -o errexit


test_import () {
    echo "Create environment: python=$PYTHON_VERSION $1"
    # Create an empty environment
    mamba create -q -y -n test-imports -c conda-forge python=$PYTHON_VERSION pyyaml fsspec toolz partd cloudpickle $1
    if [[ $1 =~ "distributed" ]]; then
        # dask[distributed] depends on the latest version of distributed
        python -m pip install git+https://github.com/dask/distributed
    fi
    conda activate test-imports
    python -m pip install -e .
    echo "python -c '$2'"
    python -c "$2"
    conda deactivate
    mamba env remove -n test-imports
}

test_import ""                                "import dask, dask.base, dask.multiprocessing, dask.threaded, dask.optimization, dask.bag, dask.delayed, dask.graph_manipulation, dask.layers"
test_import "numpy"                           "import dask.array"
test_import "pandas"                          "import dask.dataframe"
test_import "bokeh"                           "import dask.diagnostics"
test_import "distributed"                     "import dask.distributed"
