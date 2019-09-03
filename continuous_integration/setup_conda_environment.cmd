@rem The cmd /C hack circumvents a regression where conda installs a conda.bat
@rem script in non-root environments.
set CONDA=cmd /C conda
set CONDA_INSTALL=%CONDA% install -q -y
set PIP_INSTALL=pip install -q

@echo on

@rem Deactivate any environment
call deactivate
@rem Update conda
%CONDA% update -q -y conda
@rem Display root environment (for debugging)
%CONDA% list
@rem Clean up any left-over from a previous build
%CONDA% remove --all -q -y -n %CONDA_ENV%

@rem Create test environment
@rem (note: no cytoolz as it seems to prevent faulthandler tracebacks on crash)
%CONDA% create -n %CONDA_ENV% -q -y ^
    zstandard ^
    bokeh ^
    click ^
    cloudpickle ^
    dask ^
    dill ^
    futures ^
    lz4 ^
    ipykernel ^
    ipywidgets ^
    joblib ^
    jupyter_client ^
    mock ^
    msgpack-python ^
    prometheus_client ^
    psutil ^
    pytest ^
    python=%PYTHON% ^
    requests ^
    toolz ^
    tblib ^
    tornado=5 ^
    zict ^
    fsspec ^
    -c conda-forge

call activate %CONDA_ENV%

%CONDA% uninstall -q -y --force dask joblib zict
%PIP_INSTALL% pip --upgrade
%PIP_INSTALL% git+https://github.com/dask/dask --upgrade
%PIP_INSTALL% git+https://github.com/joblib/joblib.git --upgrade
%PIP_INSTALL% git+https://github.com/dask/zict --upgrade

%PIP_INSTALL% "pytest>=4" pytest-repeat pytest-timeout pytest-faulthandler sortedcollections pytest-asyncio

@rem Display final environment (for reproducing)
%CONDA% list
%CONDA% list --explicit
where python
where pip
pip list
python -m site
