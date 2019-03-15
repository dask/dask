#!/usr/bin/env bash
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

# This needs to be enabled to test __array_function__ protocol with
# NumPy v1.16.x, enabled by default starting in v1.17
export NUMPY_EXPERIMENTAL_ARRAY_FUNCTION=1
