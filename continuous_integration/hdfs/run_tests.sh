#!/bin/bash

docker exec hdfs py.test dask/bytes/tests/test_hdfs.py -vv
