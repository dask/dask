# Install conda
wget http://repo.continuum.io/miniconda/Miniconda-latest-Linux-x86_64.sh -O miniconda.sh
bash miniconda.sh -b -p $HOME/miniconda
export PATH="$HOME/miniconda/bin:$PATH"
conda config --set always_yes yes --set changeps1 no
conda config --add channels conda-forge
conda create -n test-environment python=$PYTHON
source activate test-environment


# Install dependencies
conda install \
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
    scikit-learn \
    scipy \
    sortedcollections \
    toolz

pip install \
    cachey \
    flake8 \
    graphviz \
    pandas_datareader \
    pytest-xdist \
    moto --upgrade --no-deps

pip install git+https://github.com/dask/zict --upgrade --no-deps
pip install git+https://github.com/dask/distributed --upgrade --no-deps

if [[ $PYTHON == '2.7' ]]; then
    pip install backports.lzma mock
    pip install git+https://github.com/Blosc/castra
fi

if [[ $PYTHON == '3.5' ]]; then
    pip install git+https://github.com/dask/fastparquet
fi

# Install dask
pip install --no-deps -e .[complete]
