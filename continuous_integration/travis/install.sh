# Install conda
case "$(uname -s)" in
    'Darwin')
        MINICONDA_FILENAME="Miniconda-latest-MacOSX-x86_64.sh"
        ;;
    'Linux')
        MINICONDA_FILENAME="Miniconda-latest-Linux-x86_64.sh"
        ;;
    *)  ;;
esac


wget https://repo.continuum.io/miniconda/$MINICONDA_FILENAME -O miniconda.sh
bash miniconda.sh -b -p $HOME/miniconda
export PATH="$HOME/miniconda/bin:$PATH"
conda config --set always_yes yes --set changeps1 no

# Create conda environment
conda create -n test-environment python=$PYTHON
source activate test-environment

# Install dependencies.
# XXX: Due to a weird conda dependency resolution issue, we need to install
# dependencies in two separate calls, otherwise we sometimes get version
# incompatible with the installed version of numpy leading to crashes. This
# seems to have to do with differences between conda-forge and defaults.
conda install -c conda-forge \
    numpy=$NUMPY \
    pandas=$PANDAS \
    bcolz \
    blosc \
    chest \
    coverage \
    cytoolz \
    h5py \
    ipython \
    partd \
    psutil \
    pytables \
    pytest \
    scikit-learn \
    scipy \
    toolz

# Specify numpy/pandas here to prevent upgrade/downgrade
conda install -c conda-forge \
    numpy=$NUMPY \
    pandas=$PANDAS \
    distributed \
    cloudpickle \
    bokeh \

pip install git+https://github.com/dask/zict --upgrade --no-deps
pip install git+https://github.com/dask/distributed --upgrade --no-deps

if [[ $PYTHONOPTIMIZE != '2' ]]; then
    conda install -c conda-forge numba cython
    pip install git+https://github.com/dask/fastparquet
fi

if [[ $PYTHON == '2.7' ]]; then
    pip install backports.lzma mock
fi

pip install \
    cachey \
    graphviz \
    moto \
    --upgrade --no-deps

pip install \
    flake8 \
    pandas_datareader \
    pytest-xdist

# Install dask
pip install --no-deps -e .[complete]
