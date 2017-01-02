@rem The cmd /C hack circumvents a regression where conda installs a conda.bat
@rem script in non-root environments.
set CONDA=cmd /C conda
set CONDA_INSTALL=%CONDA% install -q -y
set PIP_INSTALL=pip install -q

@echo on

@rem Deactivate any environment
call deactivate
@rem Display root environment (for debugging)
%CONDA% list
@rem Clean up any left-over from a previous build
%CONDA% remove --all -q -y -n %CONDA_ENV%

@rem Create test environment
@rem (note: no cytoolz as it seems to prevent faulthandler tracebacks on crash)
%CONDA% create -n %CONDA_ENV% -q -y python=%PYTHON% pytest toolz dill futures dask ipywidgets psutil bokeh requests joblib mock ipykernel jupyter_client tblib msgpack-python cloudpickle click zict lz4 -c conda-forge

call activate %CONDA_ENV%

%CONDA% uninstall -q -y --force dask joblib zict
%PIP_INSTALL% git+https://github.com/dask/dask --upgrade
%PIP_INSTALL% git+https://github.com/joblib/joblib.git --upgrade
%PIP_INSTALL% git+https://github.com/dask/zict --upgrade

%PIP_INSTALL% pytest-timeout pytest-faulthandler sortedcollections

@rem Display final environment (for reproducing)
%CONDA% list
%CONDA% list --explicit
pip list
python -m site
