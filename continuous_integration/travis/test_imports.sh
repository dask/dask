#!/usr/bin/env bash
test_import () {
    # Install dependencies
    if [[ -n "$2" ]]; then
        output=$(conda install -c conda-forge $2)
        if [[ $? -eq 1 ]]; then
            echo $output
            echo "$1 install failed" >&2
            exit 1
        fi
    fi
    # Check import
    python -c "$3"
    if [[ $? -eq 1 ]]; then
        echo "$1 import failed" >&2
        exit 1
    else
        echo "$1 import succeeded"
    fi
    # Uninstall dependencies
    if [[ -n "$2" ]]; then
        output=$(conda uninstall $2)
    fi
}

# Create an empty environment
conda create -n test-imports python=$PYTHON
source activate test-imports

(test_import "Core" "" "import dask, dask.threaded, dask.optimization") && \
(test_import "Delayed" "toolz" "import dask.delayed") && \
(test_import "Bag" "toolz partd cloudpickle" "import dask.bag") && \
(test_import "Array" "toolz numpy" "import dask.array") && \
(test_import "Dataframe" "numpy pandas toolz partd cloudpickle" "import dask.dataframe") && \
(test_import "Diagnostics" "bokeh" "import dask.diagnostics") && \
(test_import "Distributed" "distributed s3fs" "import dask.distributed")
