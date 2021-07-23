##############################################
# Dask GPU build and test script for CI      #
##############################################
set -e
NUMARGS=$#
ARGS=$*

# Arg parsing function
function hasArg {
    (( ${NUMARGS} != 0 )) && (echo " ${ARGS} " | grep -q " $1 ")
}

# Set path and build parallel level
export PATH=/opt/conda/bin:/usr/local/cuda/bin:$PATH
export PARALLEL_LEVEL=${PARALLEL_LEVEL:-4}

# Set home to the job's workspace
export HOME="$WORKSPACE"

# Switch to project root; also root of repo checkout
cd "$WORKSPACE"

# Determine CUDA release version
export CUDA_REL=${CUDA_VERSION%.*}

################################################################################
# SETUP - Check environment
################################################################################

gpuci_logger "Check environment variables"
env

gpuci_logger "Check GPU usage"
nvidia-smi

gpuci_logger "Activate conda env"
. /opt/conda/etc/profile.d/conda.sh
conda activate rapids

gpuci_logger "Install dask dependencies"
gpuci_conda_retry env update --name rapids --file "$WORKSPACE/continuous_integration/environment-3.8.yaml"

gpuci_logger "Install testing dependencies"
gpuci_mamba_retry install -y \
                  cudf=21.08 \
                  cupy \
                  cudatoolkit=$CUDA_REL

gpuci_logger "Install dask"
python setup.py install

gpuci_logger "Check compiler versions"
python --version
$CC --version
$CXX --version

gpuci_logger "Check conda environment"
conda info
conda config --show-sources
conda list --show-channel-urls

gpuci_logger "Python py.test for dask"
py.test $WORKSPACE -n 4 -v -m gpu --junitxml="$WORKSPACE/junit-dask.xml" --cov-config="$WORKSPACE/.coveragerc" --cov=dask --cov-report=xml:"$WORKSPACE/dask-coverage.xml" --cov-report term
