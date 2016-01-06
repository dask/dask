To build a local copy of the dask docs:

```
git clone git@github.com:blaze/dask.git
cd dask/docs
conda create -n daskdocs --file requirements-docs.txt
source activate daskdocs
make html
```
