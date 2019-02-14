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
    mock \
    netcdf4 \
    paramiko \
    prometheus_client \
    psutil \
    pytest \
    pytest-timeout \
    python=$PYTHON \
    requests \
    scipy \
    tblib \
    toolz \
    tornado \
    $PACKAGES

pip install -q pytest-repeat pytest-faulthandler

pip install -q git+https://github.com/dask/dask.git --upgrade --no-deps
pip install -q git+https://github.com/joblib/joblib.git --upgrade --no-deps
pip install -q git+https://github.com/dask/s3fs.git --upgrade --no-deps
pip install -q git+https://github.com/dask/zict.git --upgrade --no-deps
pip install -q sortedcollections msgpack --no-deps
pip install -q keras --upgrade --no-deps

if [[ $CRICK == true ]]; then
    conda install -q cython
    pip install -q git+https://github.com/jcrist/crick.git
fi;

# Install distributed
pip install --no-deps -e .

if [[ ! -z $TORNADO ]]; then
    pip install -U tornado==$TORNADO
fi

# For debugging
echo -e "--\n--Conda Environment\n--"
conda list

echo -e "--\n--Pip Environment\n--"
pip list --format=columns
