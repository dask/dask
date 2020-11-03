set -xe

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
