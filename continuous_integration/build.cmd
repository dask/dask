call activate %CONDA_ENV%

@echo on

@rem Install Distributed
%PIP_INSTALL% --no-deps -e .
