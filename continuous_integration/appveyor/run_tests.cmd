call activate %CONDA_ENV%

@echo on

set PYTEST=pytest

@rem %PYTEST% -v --runslow dask\dataframe\tests\test_groupby.py
%PYTEST% dask
