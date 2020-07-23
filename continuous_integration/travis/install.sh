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


# Install miniconda
wget https://repo.continuum.io/miniconda/$MINICONDA_FILENAME -O miniconda.sh
bash miniconda.sh -b -p $HOME/miniconda
export PATH="$HOME/miniconda/bin:$PATH"
export BOTO_CONFIG=/dev/null
conda config --set always_yes yes --set changeps1 no --set remote_max_retries 10

# Create conda environment
conda env create -q -n test-environment -f $ENV_FILE
source activate test-environment

# We don't have a conda-forge package for cityhash
# We don't include it in the conda environment.yaml, since that may
# make things harder for contributors that don't have a C++ compiler
python -m pip install --no-deps cityhash

if [[ ${UPSTREAM_DEV} ]]; then
    conda uninstall --force numpy pandas
    python -m pip install --no-deps --pre \
        -i https://pypi.anaconda.org/scipy-wheels-nightly/simple \
        numpy
    python -m pip install --pre pandas==1.1.0rc0
    python -m pip install \
        --upgrade \
        locket \
        git+https://github.com/pydata/sparse \
        git+https://github.com/dask/s3fs \
        git+https://github.com/intake/filesystem_spec \
        git+https://github.com/dask/partd \
        git+https://github.com/dask/zict \
        git+https://github.com/dask/distributed
fi

# Install dask
python -m pip install --quiet --no-deps -e .[complete]
echo conda list
conda list

set +xe
