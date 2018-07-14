To build a local copy of the dask docs, install the programs in
requirements-docs.txt and run 'make html'. If you use the conda package manager
these commands suffice::

  git clone git@github.com:dask/dask.git
  cd dask/docs
  make html
  open build/html/index.html
