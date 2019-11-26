To build a local copy of the Dask documentation, install the packages in
``requirements-docs.txt`` and run ``make html``. These dependencies can be
installed with ``pip``::

  pip install -r requirements-docs.txt

or ``conda``::

  conda create -n daskdocs -c conda-forge --file requirements-docs.txt
  conda activate daskdocs

After running ``make html`` the generated HTML documentation can be found in
the ``build/html`` directory. Open ``build/html/index.html`` to view the home
page for the documentation.
