set -xe

if [[ ${UPSTREAM_DEV} ]]; then

    # NOTE: `dask/tests/test_ci.py::test_upstream_packages_installed` should up be
    # updated when packages here are updated.

    # Pick up https://github.com/mamba-org/mamba/pull/2903
    mamba install -n base 'mamba>=1.5.2'

    mamba uninstall --force bokeh
    mamba install -y -c bokeh/label/dev bokeh

    mamba uninstall --force pyarrow pyarrow-core
    python -m pip install --no-deps \
        --extra-index-url https://pypi.anaconda.org/scientific-python-nightly-wheels/simple \
        --prefer-binary --pre pyarrow

    python -m pip install \
        --upgrade \
        locket \
        git+https://github.com/fsspec/s3fs \
        git+https://github.com/dask/partd \
        git+https://github.com/dask/zict \
        git+https://github.com/dask/distributed \
        git+https://github.com/zarr-developers/zarr-python
    # NOTE: Dev version of `fsspec` needs to be installed after the dev version of
    # `s3fs` to avoid dependency conflicts
    python -m pip install --upgrade git+https://github.com/fsspec/filesystem_spec
    # TODO: Add nightly `scikit-image` back once it's available
    mamba uninstall --force numpy pandas scipy numexpr numba sparse scikit-image numbagg
    python -m pip install --no-deps --pre --retries 10 \
        -i https://pypi.anaconda.org/scientific-python-nightly-wheels/simple \
        numpy \
        pandas \
        scipy \
        h5py
        # scikit-image

    # Used when automatically opening an issue when the `upstream` CI build fails
    mamba install pytest-reportlog

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
