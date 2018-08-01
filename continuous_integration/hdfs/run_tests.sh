docker exec -it $CONTAINER_ID py.test dask/bytes/tests/test_hdfs.py -vv --junit-xml=testresults.xml
