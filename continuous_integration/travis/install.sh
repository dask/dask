#
# This file should be source'd, so as to update the caller's environment
# (such as the PATH variable)
#

# Note we disable progress bars to make Travis log loading much faster

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

wget https://repo.continuum.io/miniconda/$MINICONDA_FILENAME -O miniconda.sh
bash miniconda.sh -b -p $HOME/miniconda
export PATH="$HOME/miniconda/bin:$PATH"
conda config --set always_yes yes --set changeps1 no
conda update -q conda

# Create conda environment
conda create -q -n test-environment python=$PYTHON
source activate test-environment

# Install dependencies
conda install -q \
    bokeh \
    click \
    coverage \
    dask \
    dill \
    flake8 \
    h5py \
    ipykernel \
    ipywidgets \
    joblib \
    jupyter_client \
    netcdf4 \
    paramiko \
    prometheus_client \
    psutil \
    pytest>=4 \
    pytest-timeout \
    python=$PYTHON \
    requests \
    scipy \
    tblib \
    toolz \
    tornado=$TORNADO \
    $PACKAGES

# For low-level profiler, install libunwind and stacktrace from conda-forge
# For stacktrace we use --no-deps to avoid upgrade of python
conda install -c defaults -c conda-forge libunwind zstandard asyncssh
conda install --no-deps -c defaults -c numba -c conda-forge stacktrace

python -m pip install -q "pytest>=4" pytest-repeat pytest-faulthandler pytest-asyncio

python -m pip install -q git+https://github.com/dask/dask.git --upgrade --no-deps
python -m pip install -q git+https://github.com/joblib/joblib.git --upgrade --no-deps
python -m pip install -q git+https://github.com/intake/filesystem_spec.git --upgrade --no-deps
python -m pip install -q git+https://github.com/dask/s3fs.git --upgrade --no-deps
python -m pip install -q git+https://github.com/dask/zict.git --upgrade --no-deps
python -m pip install -q sortedcollections msgpack --no-deps
python -m pip install -q keras --upgrade --no-deps
python -m pip install -q asyncssh

if [[ $CRICK == true ]]; then
    conda install -q cython
    python -m pip install -q git+https://github.com/jcrist/crick.git
fi;

# Install distributed
python -m pip install --no-deps -e .

# For debugging
echo -e "--\n--Conda Environment\n--"
conda list

echo -e "--\n--Pip Environment\n--"
python -m pip list --format=columns
