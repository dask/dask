To build a local copy of the dask docs, run the following commands::

  git clone git@github.com:dask/dask.git
  cd dask/docs
  conda create -n daskdocs --file requirements-docs.txt
  source activate daskdocs
  make html
  open build/html/index.html
