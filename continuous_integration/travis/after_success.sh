#!/usr/bin/env bash
if [[ $COVERAGE == 'true' ]]; then
    coverage report --show-missing
    pip install coveralls
    coveralls
fi
