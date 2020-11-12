#!/usr/bin/env bash

codecov
coverage report --show-missing
python -m pip install coveralls
coveralls
