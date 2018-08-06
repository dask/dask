docker exec -it $CONTAINER_ID conda config --set auto_update_conda false
docker exec -it $CONTAINER_ID conda install -y -q dask hdfs3 pyarrow -c twosigma -c conda-forge
docker exec -it $CONTAINER_ID pip install -e .
