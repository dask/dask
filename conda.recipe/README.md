## Building conda packages

Conda packages for dask can be built on linux-64 (tested on Ubuntu 14.04) using
the following commands:

```
export CONDA_DIR=~/miniconda2

sudo apt-get update
sudo apt-get install git -y

curl http://repo.continuum.io/miniconda/Miniconda-latest-Linux-x86_64.sh -o ~/miniconda.sh
bash ~/miniconda.sh -b -p $CONDA_DIR
$CONDA_DIR/bin/conda install conda-build anaconda-client -y

git clone https://github.com/dask/dask.git ~/dask
cd ~/dask
$CONDA_DIR/bin/conda build conda.recipe --python 2.6 --python 2.7 --python 3.4 --python 3.5

cd $CONDA_DIR/conda-bld/linux-64
$CONDA_DIR/bin/conda convert --platform osx-64 *.tar.bz2 -o ../
$CONDA_DIR/bin/conda convert --platform win-64 *.tar.bz2 -o ../

$CONDA_DIR/bin/anaconda login
$CONDA_DIR/bin/anaconda upload $CONDA_DIR/conda-bld/*/*.tar.bz2 -u dask
```
