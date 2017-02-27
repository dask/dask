# Install conda
wget http://repo.continuum.io/miniconda/Miniconda-latest-Linux-x86_64.sh -O miniconda.sh
bash miniconda.sh -b -p $HOME/miniconda
export PATH="$HOME/miniconda/bin:$PATH"
conda config --set always_yes yes --set changeps1 no

# Install dependencies
conda create -n test-environment python=$PYTHON
source activate test-environment
conda install -c conda-forge numpy=$NUMPY scipy pytables h5py bcolz pytest coverage toolz scikit-learn cytoolz chest blosc cython psutil ipython numba
conda install -c conda-forge pandas=$PANDAS distributed cloudpickle bokeh sortedcollections
pip install git+https://github.com/dask/zict --upgrade --no-deps
pip install git+https://github.com/dask/distributed --upgrade --no-deps
if [[ $PYTHON == '2.7' ]]; then pip install backports.lzma mock; fi
if [[ $PYTHON == '3.5' ]]; then pip install git+https://github.com/dask/fastparquet; fi
pip install partd cachey blosc graphviz moto --upgrade --no-deps
pip install flake8 pandas_datareader
if [[ $PYTHON < '3' ]]; then pip install git+https://github.com/Blosc/castra; fi
# For parallel testing (`-n` argument in XTRATESTARGS)
pip install pytest-xdist

# Install dask
pip install --no-deps -e .[complete]
