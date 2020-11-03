#!/bin/bash

export CONTAINER_ID=$(docker run -d -v $(pwd):/working daskdev/dask-hdfs-testing)
docker exec -it $CONTAINER_ID py.test dask/bytes/tests/test_hdfs.py -vv
