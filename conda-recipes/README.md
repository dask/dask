## Building conda packages

Conda packages for distributed and its dependencies can be built using the
following commands (tested on Ubuntu 14.04):

```
export CONDA_DIR=~/miniconda2

sudo apt-get update
sudo apt-get install -y -q git

curl http://repo.continuum.io/miniconda/Miniconda-latest-Linux-x86_64.sh -o ~/miniconda.sh
bash ~/miniconda.sh -b -p $CONDA_DIR
$CONDA_DIR/bin/conda install conda-build anaconda-client -y

git clone https://github.com/blaze/distributed.git ~/distributed
cd ~/distributed/conda-recipes
$CONDA_DIR/bin/conda build distributed --python 2.7 --python 3.4 --python 3.5

cd ~/$CONDA_DIR/conda-bld/linux-64
~/miniconda2/bin/conda convert --platform osx-64 *.tar.bz2 -o ../
~/miniconda2/bin/conda convert --platform win-64 *.tar.bz2 -o ../

$CONDA_DIR/bin/anaconda login
$CONDA_DIR/bin/anaconda upload $CONDA_DIR/conda-bld/linux-64/*.tar.bz2 -u blaze
$CONDA_DIR/bin/anaconda upload $CONDA_DIR/conda-bld/osx-64/*.tar.bz2 -u blaze
$CONDA_DIR/bin/anaconda upload $CONDA_DIR/conda-bld/win-64/*.tar.bz2 -u blaze
```
