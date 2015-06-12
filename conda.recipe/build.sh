#!/bin/sh

pip install git+https://github.com/blosc/bcolz --upgrade
pip install partd
$PYTHON setup.py install
