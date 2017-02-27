# Install conda
wget http://repo.continuum.io/miniconda/Miniconda-latest-Linux-x86_64.sh -O miniconda.sh
bash miniconda.sh -b -p $HOME/miniconda
export PATH="$HOME/miniconda/bin:$PATH"
conda config --set always_yes yes --set changeps1 no

# Create conda environment
conda create -n test-environment python=$PYTHON
source activate test-environment

# Install dependencies
conda install -c conda-forge \
    numpy=$NUMPY \
    pandas=$PANDAS \
    bcolz \
    blosc \
    bokeh \
    chest \
    cloudpickle \
    coverage \
    cython \
    cytoolz \
    distributed \
    h5py \
    ipython \
    numba \
    partd \
    psutil \
    pytables \
    pytest \
    scipy \
    sortedcollections \
    toolz

# Due to a weird conda dependency resolution issue, we need to install
# scikit-learn in a separate call, otherwise we get a version incompatible with
# the installed version of numpy leading to crashes
conda install -c conda-forge scikit-learn

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
