#!/bin/bash
set -xe

# Remove <2 version constraint once we've added pyarrow 2.0 compatibility
# xref https://github.com/dask/dask/issues/6754
docker exec hdfs conda install -y -q dask pyarrow">=0.14.0,<2" fsspec pyyaml -c conda-forge
docker exec hdfs python -m pip install -e .

set +xe
