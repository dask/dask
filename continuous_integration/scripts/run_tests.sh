#!/usr/bin/env bash

set -e

CMD="python -m pytest dask --runslow"

if [[ $COVERAGE == 'true' ]]; then
    CMD="$CMD --cov --cov-report=xml --junit-xml=pytest.xml"
fi

if [[ $ARRAYEXPR == 'true' ]]; then
    CMD="$CMD --runarrayexpr"
fi

if [[ $PARALLEL == 'true' ]]; then
    CMD="$CMD --dist loadgroup -n auto"
fi

CMD="$CMD $@"

env | grep DASK || true
echo "$CMD"
$CMD
