#!/usr/bin/env bash

set -e

if [[ $ARRAYEXPR == 'true' ]]; then
    export MARKERS="--runarrayexpr"
else
    export MARKERS=""
fi

if [[ $PARALLEL == 'true' ]]; then
    export XTRATESTARGS="-n4 $XTRATESTARGS"
fi

if [[ $COVERAGE == 'true' ]]; then
    export XTRATESTARGS="--cov=dask --cov-report=xml --junit-xml pytest.xml $XTRATESTARGS"
fi

echo "py.testdask --runslow $MARKERS $XTRATESTARGS"
py.test dask --runslow $MARKERS $XTRATESTARGS

set +e
