#!/bin/bash
set -xe

docker exec hdfs conda install -y -q dask pyarrow">=0.14.0,!=2.0.0" fsspec pyyaml -c conda-forge
docker exec hdfs python -m pip install -e .

set +xe
