# Setting up hdfs testing using docker

Assumes docker is already installed and the docker-daemon is running.

From the root directory in the repo:

- First get the docker container:

```bash
# Either pull it from docker hub
docker pull daskdev/dask-hdfs-testing

# Or build it locally
docker build -t daskdev/dask-hdfs-testing continuous_integration/hdfs/
```

- Start the container and wait for it to be ready:

```bash
source continuous_integration/hdfs/startup_hdfs.sh
```

- Start a bash session in the running container:

```bash
# CONTAINER_ID should be defined from above, but if it isn't you can get it from
export CONTAINER_ID=$(docker ps -l -q)

# Start the bash session
docker exec -it $CONTAINER_ID bash
```

- Install the library and run the tests

```bash
python setup.py install
# Test just the hdfs tests
py.test dask/bytes/tests/test_hdfs.py -s -vv
```
