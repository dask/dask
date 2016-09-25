There are a variety of other projects related to dask that are often
co-released.  We may want to check their status while releasing


Release per project:

*   Tag commit

        git tag -a x.x.x -m 'Version x.x.x'

*   and push to github

        git push dask master --tags

*  Upload to PyPI

        git clean -xfd
        python setup.py register sdist bdist_wheel --universal
        twine upload dist/*

*   Update anaconda recipe.

    This should happen automatically within a day or two.

*   Update conda recipe feedstock on `conda-forge <https://conda-forge.github.io/>`_.
