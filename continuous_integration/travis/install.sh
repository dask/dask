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

# Create conda environment
conda create -q -n test-environment python=$PYTHON
source activate test-environment

# Install dependencies.
# XXX: Due to a weird conda dependency resolution issue, we need to install
# dependencies in two separate calls, otherwise we sometimes get version
# incompatible with the installed version of numpy leading to crashes. This
# seems to have to do with differences between conda-forge and defaults.
conda install -q -c conda-forge \
    numpy=$NUMPY \
    pandas=$PANDAS \
    bcolz \
    blosc \
    chest \
    coverage \
    cytoolz \
    graphviz \
    h5py \
    ipython \
    partd \
    psutil \
    pytables \
    pytest \
    scikit-image \
    scikit-learn \
    scipy \
    sqlalchemy \
    toolz

# Specify numpy/pandas here to prevent upgrade/downgrade
conda install -q -c conda-forge \
    numpy=$NUMPY \
    pandas=$PANDAS \
    distributed \
    cloudpickle \
    bokeh \

pip install -q git+https://github.com/dask/zict --upgrade --no-deps
pip install -q git+https://github.com/dask/distributed --upgrade --no-deps

if [[ $PYTHONOPTIMIZE != '2' ]]; then
    conda install -q -c conda-forge numba cython
    pip install -q git+https://github.com/dask/fastparquet
fi

if [[ $PYTHON == '2.7' ]]; then
    pip install -q backports.lzma mock
fi

pip install -q \
    cachey \
    graphviz \
    moto \
    --upgrade --no-deps

pip install -q \
    flake8 \
    pandas_datareader \
    pytest-xdist

# Install dask
pip install -q --no-deps -e .[complete]
