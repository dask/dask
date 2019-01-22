There are a variety of other projects related to dask that are often
co-released.  We may want to check their status while releasing


Release per project:

*   Update release notes in docs/source/changelog.rst

*   Commit

        git commit -a -m "bump version to x.x.x"

*   Tag commit

        git tag -a x.x.x -m 'Version x.x.x'

*   Push to github

        git push dask master --tags

*   Upload to PyPI

        git clean -xfd
        python setup.py sdist bdist_wheel --universal
        twine upload dist/*

*   Update `dask-core` feedstock on [conda-forge](https://conda-forge.github.io)

    Typically this is handled by a conda-forge bot within an hour or two,
    but in some cases you have to modify version or build numbers.

    If for some reason you have to do this manually, then follow these steps:

    *  Update conda-smithy and run conda-smithy rerender

            git clone git@github.com:conda-forge/dask-core-feedstock
            cd dask-core-feedstock
            conda install conda-smithy
            conda-smithy rerender

    *  Get sha256 hash from pypi.org
    *  Update version number and hash in recipe
    *  Check dependencies
    *  Do the same for the dask-feedstock meta-package

*   Update `dask` feedstock after `dask-core` feedstock has been merged and is
    available through conda.

    Usually this only means updating the version in the top line, and then
    submitting a PR.

*   Automated systems internal to Anaconda Inc then handle updating the
    Anaconda defaults channel
