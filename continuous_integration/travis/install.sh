# Install conda
wget http://repo.continuum.io/miniconda/Miniconda-latest-Linux-x86_64.sh -O miniconda.sh
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
    bcolz \
    blosc \
    chest \
    coverage \
    cython \
    cytoolz \
    h5py \
    ipython \
    numba \
    partd \
    psutil \
    pytables \
    pytest \
    scikit-learn \
    scipy \
    toolz

conda install -c conda-forge \
    pandas=$PANDAS \
    distributed \
    cloudpickle \
    bokeh \
    sortedcollections

pip install git+https://github.com/dask/zict --upgrade --no-deps
pip install git+https://github.com/dask/distributed --upgrade --no-deps

if [[ $PYTHON == '2.7' ]]; then
    pip install backports.lzma mock
    pip install git+https://github.com/Blosc/castra
fi

if [[ $PYTHON == '3.5' ]]; then
    pip install git+https://github.com/dask/fastparquet
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
