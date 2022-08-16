set -xe

# TODO: Add cityhash back
# We don't have a conda-forge package for cityhash
# We don't include it in the conda environment.yaml, since that may
# make things harder for contributors that don't have a C++ compiler
# python -m pip install --no-deps cityhash

if [[ ${UPSTREAM_DEV} ]]; then
    # FIXME workaround for https://github.com/mamba-org/mamba/issues/1682
    arr=($(mamba search --override-channels -c arrow-nightlies pyarrow | tail -n 1))
    export PYARROW_VERSION=${arr[1]}
    mamba install -y -c arrow-nightlies "pyarrow=$PYARROW_VERSION"

    # FIXME https://github.com/mamba-org/mamba/issues/412
    # mamba uninstall --force ...
    conda uninstall --force numpy pandas fastparquet bokeh scipy

    mamba install -y -c bokeh/label/dev bokeh

    python -m pip install --no-deps --pre --retries 10 \
        -i https://pypi.anaconda.org/scipy-wheels-nightly/simple \
        numpy \
        pandas \
        scipy

    python -m pip install \
        --upgrade \
        locket \
        git+https://github.com/pydata/sparse \
        git+https://github.com/dask/s3fs \
        git+https://github.com/intake/filesystem_spec \
        git+https://github.com/dask/partd \
        git+https://github.com/dask/zict \
        git+https://github.com/dask/distributed \
        git+https://github.com/dask/fastparquet \
        git+https://github.com/zarr-developers/zarr-python
fi

# Install dask
python -m pip install --quiet --no-deps -e .[complete]
echo mamba list
mamba list

# For debugging
echo -e "--\n--Conda Environment (re-create this with \`conda env create --name <name> -f <output_file>\`)\n--"
mamba env export | grep -E -v '^prefix:.*$' > env.yaml
cat env.yaml

set +xe
