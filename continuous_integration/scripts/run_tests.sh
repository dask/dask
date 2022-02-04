#!/usr/bin/env bash

set -e

if [[ $PARALLEL == 'true' ]]; then
    export XTRATESTARGS="-n4 $XTRATESTARGS"
fi

if [[ $COVERAGE == 'true' ]]; then
    export XTRATESTARGS="--cov=dask --cov-report=xml $XTRATESTARGS"
fi

echo "py.test tests --runslow $XTRATESTARGS"
py.test tests --runslow $XTRATESTARGS

set +e
