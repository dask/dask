set -xe

# TODO: Add cityhash back
# We don't have a conda-forge package for cityhash
# We don't include it in the conda environment.yaml, since that may
# make things harder for contributors that don't have a C++ compiler
# python -m pip install --no-deps cityhash

# TEMP install pyarrow nightly for all envs
conda update -y -c arrow-nightlies pyarrow

if [[ ${UPSTREAM_DEV} ]]; then
    # update pyarrow first with conda, otherwise this would downgrade the
    # nightly numpy/pandas again
    conda update -y -c arrow-nightlies pyarrow

    conda uninstall --force pandas
    python -m pip install --no-deps --pre \
        -i https://pypi.anaconda.org/scipy-wheels-nightly/simple \
        pandas

    python -m pip install \
        --upgrade \
        locket \
        git+https://github.com/pydata/sparse \
        git+https://github.com/dask/s3fs \
        git+https://github.com/intake/filesystem_spec \
        git+https://github.com/dask/partd \
        git+https://github.com/dask/zict \
        git+https://github.com/dask/distributed \
        git+https://github.com/zarr-developers/zarr-python
fi


# Install dask
python -m pip install --quiet --no-deps -e .[complete]
echo conda list
conda list

set +xe
