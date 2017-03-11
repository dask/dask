call activate %CONDA_ENV%

@echo on

@rem Install Dask
%PIP_INSTALL% --no-deps -e .[complete]
