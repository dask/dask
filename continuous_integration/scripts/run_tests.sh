#!/usr/bin/env bash

set -e

CMD="python -m pytest --count=1000 dask/dataframe/io/tests/test_parquet.py::test_filter_nonpartition_columns dask/dataframe/io/tests/test_parquet.py::test_roundtrip_partitioned_pyarrow_dataset dask/dataframe/io/tests/test_parquet.py::test_arrow_partitioning"

if [[ $COVERAGE == 'true' ]]; then
    CMD="$CMD --cov --cov-report=xml --junit-xml=pytest.xml"
fi

if [[ $ARRAYEXPR == 'true' ]]; then
    CMD="$CMD --runarrayexpr"
fi

if [[ $PARALLEL == 'true' ]]; then
    CMD="$CMD -n4"
fi

CMD="$CMD $@"

env | grep DASK || true
echo "$CMD"
$CMD
