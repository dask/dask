set -xe

docker exec -it $CONTAINER_ID conda install -y -q dask hdfs3 pyarrow -c twosigma -c conda-forge
docker exec -it $CONTAINER_ID pip install -e .

set +xe
