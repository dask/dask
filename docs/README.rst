To build a local copy of the Dask documentation, install the packages in
``requirements-docs.txt`` and run ``make html``.

Optionally create and activate a ``conda`` environment first::

  conda create -n daskdocs -c conda-forge python=3.11
  conda activate daskdocs

Install the dependencies with ``pip``::

  python -m pip install -r requirements-docs.txt

After running ``make html`` the generated HTML documentation can be found in
the ``build/html`` directory. Open ``build/html/index.html`` to view the home
page for the documentation.
