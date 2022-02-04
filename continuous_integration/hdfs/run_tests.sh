#!/bin/bash

docker exec hdfs py.test tests/bytes/test_hdfs.py -vv
