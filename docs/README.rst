To build a local copy of the dask docs, install the programs in
requirements-docs.txt and run 'make html'. If you use the conda package manager
these commands suffice::

  git clone git@github.com:dask/dask.git
  cd dask/docs
  conda create -n daskdocs --file requirements-docs.txt
  source activate daskdocs
  Update the conf.py file with the code path (e.g. sys.path.insert(0, os.path.abspath('../../')) )
  make html
  open build/html/index.html
