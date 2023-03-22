#!/usr/bin/env bash
set -o errexit


test_import () {
    echo "Create environment: python=$PYTHON_VERSION $1"
    # Create an empty environment
    mamba create -q -y -n test-imports -c conda-forge python=$PYTHON_VERSION packaging pyyaml fsspec toolz partd click cloudpickle importlib-metadata $1
    conda activate test-imports
    if [[ $1 =~ "distributed" ]]; then
        # dask[distributed] depends on the latest version of distributed
        python -m pip install git+https://github.com/dask/distributed
    fi
    python -m pip install -e .
    mamba list
    echo "python -c '$2'"
    python -c "$2"
    # Ensure that no non-deterministic objects are tokenized at init time,
    # which can prevent the library from being imported at all.
    echo "python -c '$2' (ensure deterministic)"
    DASK_TOKENIZE__ENSURE_DETERMINISTIC=True python -c "$2"
    conda deactivate
    mamba env remove -n test-imports
}

test_import ""                                "import dask, dask.base, dask.multiprocessing, dask.threaded, dask.optimization, dask.bag, dask.delayed, dask.graph_manipulation, dask.layers"
test_import "numpy"                           "import dask.array"
test_import "pandas"                          "import dask.dataframe"
test_import "bokeh"                           "import dask.diagnostics"
test_import "distributed"                     "import dask.distributed"
