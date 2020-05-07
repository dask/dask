#
# This file should be source'd, so as to update the caller's environment
# (such as the PATH variable)
#

# Note we disable progress bars to make Travis log loading much faster

# Set default variable values if unset
# (useful when this script is not invoked by Travis)
: ${PYTHON:=3.8}
: ${TORNADO:=6}
: ${PACKAGES:=python-snappy python-blosc}

# Install conda
case "$(uname -s)" in
    'Darwin')
        MINICONDA_FILENAME="Miniconda3-latest-MacOSX-x86_64.sh"
        ;;
    'Linux')
        MINICONDA_FILENAME="Miniconda3-latest-Linux-x86_64.sh"
        ;;
    *)  ;;
esac

if ! which conda; then
  wget https://repo.continuum.io/miniconda/$MINICONDA_FILENAME -O miniconda.sh
  bash miniconda.sh -b -p $HOME/miniconda
  export PATH="$HOME/miniconda/bin:$PATH"
fi

conda config --set always_yes yes --set quiet yes --set changeps1 no
conda update conda

# Create conda environment
conda create -n dask-distributed -c conda-forge -c defaults \
    asyncssh \
    bokeh \
    click \
    coverage \
    dask \
    flake8 \
    h5py \
    ipykernel \
    ipywidgets \
    joblib \
    jupyter_client \
    'msgpack-python>=0.6.0' \
    netcdf4 \
    paramiko \
    prometheus_client \
    psutil \
    'pytest>=4' \
    pytest-asyncio \
    pytest-faulthandler \
    pytest-repeat \
    pytest-timeout \
    python=$PYTHON \
    requests \
    scikit-learn \
    scipy \
    sortedcollections \
    'tblib>=1.5.0' \
    toolz \
    tornado=$TORNADO \
    zstandard \
    $PACKAGES

source activate dask-distributed

if [[ $PYTHON == 3.6 ]]; then
  conda install -c conda-forge -c defaults contextvars
fi

# stacktrace is not currently avaiable for Python 3.8.
# Remove the version check block below when it is avaiable.
if [[ $PYTHON != 3.8 ]]; then
    # For low-level profiler, install libunwind and stacktrace from conda-forge
    # For stacktrace we use --no-deps to avoid upgrade of python
    conda install -c conda-forge -c defaults libunwind
    conda install --no-deps -c conda-forge -c defaults -c numba stacktrace
fi

python -m pip install -q git+https://github.com/dask/dask.git --upgrade --no-deps
python -m pip install -q git+https://github.com/joblib/joblib.git --upgrade --no-deps
python -m pip install -q git+https://github.com/intake/filesystem_spec.git --upgrade --no-deps
python -m pip install -q git+https://github.com/dask/s3fs.git --upgrade --no-deps
python -m pip install -q git+https://github.com/dask/zict.git --upgrade --no-deps
python -m pip install -q keras --upgrade --no-deps

if [[ $CRICK == true ]]; then
    conda install -c conda-forge -c defaults cython
    python -m pip install -q git+https://github.com/jcrist/crick.git
fi

# Install distributed
python -m pip install --no-deps -e .

# For debugging
echo -e "--\n--Conda Environment\n--"
conda list

echo -e "--\n--Pip Environment\n--"
python -m pip list --format=columns
