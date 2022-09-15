set -xe

# TODO: Add cityhash back
# We don't have a conda-forge package for cityhash
# We don't include it in the conda environment.yaml, since that may
# make things harder for contributors that don't have a C++ compiler
# python -m pip install --no-deps cityhash

if [[ ${UPSTREAM_DEV} ]]; then
    conda uninstall --force pandas
    python -m pip install --no-deps --pre --retries 10 -i https://pypi.anaconda.org/scipy-wheels-nightly/simple pandas
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
