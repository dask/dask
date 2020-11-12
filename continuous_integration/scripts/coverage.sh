#!/usr/bin/env bash

coverage report --show-missing
python -m pip install coveralls
coveralls
