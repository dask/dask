#!/usr/bin/env bash

set -e

if [[ $PARALLEL == 'true' ]]; then
    export XTRATESTARGS="-n4 $XTRATESTARGS"
fi

if [[ $COVERAGE == 'true' ]]; then
    export XTRATESTARGS="--cov=dask --cov-report=xml --junit-xml pytest.xml $XTRATESTARGS"
fi

echo "py.test dask --runslow $XTRATESTARGS"
py.test dask --runslow $XTRATESTARGS

set +e
