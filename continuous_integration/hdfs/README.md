## HDFS testing on Travis CI

Dask & HDFS testing relies on a docker container. The tests are setup to run on
Travis CI, but only under the following conditions:

- Merges to master
- PRs where the commit message contains the string `"test-hdfs"`

If you make a PR changing HDFS functionality it'd be good to have the HDFS
tests run, please add `"test-hdfs"` to your commit message.

## Setting up HDFS testing locally using Docker

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

- Install dependencies and dask on the container

```bash
source continuous_integration/hdfs/install.sh
```

- Run the tests

```bash
source continuous_integration/hdfs/run_tests.sh
```

- Alternatively, you can start a terminal on the container and run the tests
  manually. This can be nicer for debugging:

```bash
# CONTAINER_ID should be defined from above, but if it isn't you can get it from
export CONTAINER_ID=$(docker ps -l -q)

# Start the bash session
docker exec -it $CONTAINER_ID bash

# Test just the hdfs tests
py.test dask/bytes/tests/test_hdfs.py -s -vv
```
