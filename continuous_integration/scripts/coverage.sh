#!/usr/bin/env bash
if [[ $COVERAGE == 'true' ]]; then
    codecov
    coverage report --show-missing
    python -m pip install coveralls
    coveralls
fi
