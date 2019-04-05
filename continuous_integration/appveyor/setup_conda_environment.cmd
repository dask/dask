@rem The cmd /C hack circumvents a regression where conda installs a conda.bat
@rem script in non-root environments.
set CONDA=cmd /C conda
set CONDA_INSTALL=%CONDA% install -q -y
set PIP_INSTALL=pip install -q

@echo on

@rem Deactivate any environment
call deactivate
@rem Display root environment (for debugging)
conda list
@rem Clean up any left-over from a previous build
conda remove --all -q -y -n %CONDA_ENV%

@rem Create test environment
@rem (note: no cytoolz as it seems to prevent faulthandler tracebacks on crash)
conda create -n %CONDA_ENV% -q -y python=%PYTHON% pytest toolz

call activate %CONDA_ENV%

@rem Pin matrix items
@rem Please see PR ( https://github.com/dask/dask/pull/2185 ) for details.
copy NUL %CONDA_PREFIX%\conda-meta\pinned

@rem Install optional dependencies for tests
%CONDA_INSTALL% numpy pandas cloudpickle distributed
%CONDA_INSTALL% bcolz bokeh h5py ipython lz4 psutil pytables s3fs scipy
%CONDA_INSTALL% -c conda-forge fastparquet snappy zarr

%PIP_INSTALL% --no-deps --upgrade locket git+https://github.com/dask/partd
%PIP_INSTALL% --no-deps --upgrade heapdict git+https://github.com/dask/cachey
%PIP_INSTALL%           --upgrade git+https://github.com/dask/distributed
%PIP_INSTALL% --no-deps           git+https://github.com/pydata/sparse
%PIP_INSTALL% --no-deps --upgrade blosc --upgrade
%PIP_INSTALL% --no-deps moto Jinja2 boto boto3 botocore cryptography requests xmltodict six werkzeug PyYAML pytz python-dateutil python-jose mock docker jsondiff==1.1.2 aws-xray-sdk responses idna cfn-lint

if %PYTHON% LSS 3.0 (%PIP_INSTALL% backports.lzma mock)

@rem Display final environment (for reproducing)
%CONDA% list
%CONDA% list --explicit
python -m site
