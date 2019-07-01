set -xe

#!/usr/bin/env bash
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
export BOTO_CONFIG=/dev/null
conda config --set always_yes yes --set changeps1 no --set remote_max_retries 10

# Create conda environment
conda create -q -n test-environment python=$PYTHON
source activate test-environment

# Pin matrix items
# Please see PR ( https://github.com/dask/dask/pull/2185 ) for details.
touch $CONDA_PREFIX/conda-meta/pinned
if ! [[ ${UPSTREAM_DEV} ]]; then
    echo "Pinning NumPy $NUMPY, pandas $PANDAS"
    echo "numpy $NUMPY" >> $CONDA_PREFIX/conda-meta/pinned
    echo "pandas $PANDAS" >> $CONDA_PREFIX/conda-meta/pinned
fi;

# Install dependencies.
conda install -q -c conda-forge \
    numpy \
    pandas \
    bcolz \
    blosc \
    bokeh \
    boto3 \
    botocore \
    httpretty \
    chest \
    cloudpickle \
    coverage \
    crick \
    cytoolz \
    distributed \
    graphviz \
    h5py \
    ipython \
    lz4 \
    numba \
    partd \
    psutil \
    pytables \
    pytest \
    requests \
    scikit-image \
    scikit-learn \
    scipy \
    sqlalchemy \
    toolz \
    tiledb-py \
    zarr

pip install --quiet --upgrade codecov

pip install --quiet --upgrade --no-deps locket git+https://github.com/dask/partd
pip install --quiet --upgrade --no-deps git+https://github.com/dask/zict
pip install --quiet --upgrade --no-deps git+https://github.com/dask/distributed

if [[ $PYTHONOPTIMIZE != '2' ]] && [[ $NUMPY > '1.11.0' ]] && [[ $NUMPY < '1.14.0' ]]; then
    conda install -q -c conda-forge fastparquet python-snappy cython
    conda remove --force fastparquet
    pip install --quiet --no-deps git+https://github.com/dask/fastparquet
fi

if [[ $NUMPY > '1.13.0' ]]; then
    if [[ ${UPSTREAM_DEV} ]]; then
        pip install --quiet --upgrade git+https://github.com/pydata/sparse
    else
        pip install --quiet sparse
    fi
fi

pip install --quiet --upgrade --no-deps \
    cachey \
    graphviz

pip install --quiet --upgrade \
    cityhash \
    mmh3 \
    pytest-xdist \
    xxhash \
    pandas_datareader

if [[ ${UPSTREAM_DEV} ]]; then
    echo "Installing PyArrow dev"
    conda install -q -c twosigma \
          arrow-cpp \
          parquet-cpp \
          pyarrow
else
    echo "Installing PyArrow"
    conda install -q -c conda-forge \
          arrow-cpp \
          parquet-cpp \
          pyarrow

fi;

if [[ ${UPSTREAM_DEV} ]]; then
    echo "Installing NumPy and Pandas dev"
    conda uninstall -y --force numpy pandas
    PRE_WHEELS="https://7933911d6844c6c53a7d-47bd50c35cd79bd838daf386af554a83.ssl.cf2.rackcdn.com"
    pip install --quiet --pre --no-deps --upgrade --timeout=60 -f $PRE_WHEELS numpy pandas
fi;


# Install dask
pip install --quiet --no-deps -e .[complete]
echo conda list
conda list

set +xe
