# Install conda
case "$(uname -s)" in
    'Darwin')
        MINICONDA_FILENAME="Miniconda3-4.3.21-MacOSX-x86_64.sh"
        ;;
    'Linux')
        MINICONDA_FILENAME="Miniconda3-4.3.21-Linux-x86_64.sh"
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

# Pin matrix items
# Please see PR ( https://github.com/dask/dask/pull/2185 ) for details.
touch $CONDA_PREFIX/conda-meta/pinned
echo "numpy $NUMPY" >> $CONDA_PREFIX/conda-meta/pinned
echo "pandas $PANDAS" >> $CONDA_PREFIX/conda-meta/pinned

# Install dependencies.
conda install -q -c conda-forge \
    numpy \
    pandas \
    bcolz \
    blosc \
    bokeh \
    boto3 \
    chest \
    cloudpickle \
    coverage \
    cytoolz \
    distributed \
    graphviz \
    h5py \
    ipython \
    partd \
    psutil \
    "pytest<=3.1.1" \
    scikit-image \
    scikit-learn \
    scipy \
    sqlalchemy \
    toolz

# install pytables from defaults for now
conda install -q pytables

pip install -q git+https://github.com/dask/partd --upgrade --no-deps
pip install -q git+https://github.com/dask/zict --upgrade --no-deps
pip install -q git+https://github.com/dask/distributed --upgrade --no-deps
pip install -q git+https://github.com/mrocklin/sparse --upgrade --no-deps
pip install -q git+https://github.com/dask/s3fs --upgrade --no-deps

if [[ $PYTHONOPTIMIZE != '2' ]] && [[ $NUMPY > '1.11.0' ]] && [[ $NUMPY < '1.13.0' ]]; then
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
    pyarrow \
    --upgrade --no-deps

pip install -q \
    cityhash \
    flake8 \
    mmh3 \
    pandas_datareader \
    pytest-xdist \
    xxhash \
    pycodestyle

# Install dask
pip install -q --no-deps -e .[complete]
echo conda list
conda list
