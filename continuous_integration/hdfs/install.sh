set -xe

docker exec -it $CONTAINER_ID conda install -y -q dask pyarrow">0.13.0" fsspec -c conda-forge
docker exec -it $CONTAINER_ID pip install -e .

set +xe
