docker exec -it $CONTAINER_ID conda install -y -q dask hdfs3 pyarrow -c conda-forge
docker exec -it $CONTAINER_ID python setup.py install
