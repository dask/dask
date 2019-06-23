#!/usr/bin/env bash

set -e

# Need to make test order deterministic when parallelizing tests, hence PYTHONHASHSEED
# (see https://github.com/pytest-dev/pytest-xdist/issues/63)
if [[ $PARALLEL == 'true' ]]; then
    export XTRATESTARGS="-n3 $XTRATESTARGS"
    export PYTHONHASHSEED=42
fi

if [[ $COVERAGE == 'true' ]]; then
    echo "coverage run `which py.test` dask --runslow --doctest-modules $XTRATESTARGS"
    coverage run `which py.test` dask --runslow --doctest-modules $XTRATESTARGS
else
    echo "py.test dask --runslow $XTRATESTARGS"
    py.test dask --runslow $XTRATESTARGS
fi

set +e
