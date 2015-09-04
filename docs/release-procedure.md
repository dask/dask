There are a variety of other projects related to dask that are often
co-released.  We may want to check their status while releasing


Release per project:

*   Update version number in __init__.py, setup.py,
    and conda.recipe/meta.yaml (if present).  Commit.

*   Tag commit

        git tag -a x.x.x -m 'Version x.x.x'

*   and push to github

        git push blaze master
        git push blaze --tags

*  Upload to PyPI

        python setup.py register sdist upload

*   Update anaconda recipe.

    The anaconda recipe lives in github.com:ContinuumIO/anaconda.git under the
    packages/ directory.  The recipe is based off of PyPI so we need to update
    the download link and md5 hash which look like the following:

    https://pypi.python.org/packages/source/d/dask/dask-0.7.0.tar.gz
    02fb312264241f3e9f7cca5579a24664

    Move the directory to update the version and update the meta.yaml file with
    the information above.  Push this as a branch to the ContinuumIO fork and
    issue a PR.
