#!/usr/bin/env bash
if [[ $COVERAGE == 'true' ]]; then
    coverage report --show-missing
    python -m pip install coveralls
    coveralls
fi
