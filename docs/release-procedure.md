There are a variety of other projects related to dask that are often
co-released.  We may want to check their status while releasing


Release per project:

*   Raise an issue in the https://github.com/dask/dask issue tracker signalling
    your intent to release and the motivation.  Let that issue collect comments
    for a day to ensure that other maintainers are comfortable with releasing.

*   Update release notes in docs/source/changelog.rst

*   Commit

        git commit -a -m "bump version to x.x.x"

*   Tag commit

        git tag -a x.x.x -m 'Version x.x.x'

*   Push to github

        git push dask master --tags

*   Upload to PyPI

        git clean -xfd
        python setup.py sdist bdist_wheel
        twine upload dist/*

*   Wait for [conda-forge](https://conda-forge.github.io) bots to track the
    change to PyPI

    This will typically happen in an hour or two.  There will be two PRs, one
    to `dask-core`, which you will likely be able to merge after tests pass,
    and another to `dask`, for which you might have to change version numbers
    if other packages (like `distributed`) are changing at the same time.

    In some cases you may also have to zero out build numbers.

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

*   Automated systems internal to Anaconda Inc then handle updating the
    Anaconda defaults channel
