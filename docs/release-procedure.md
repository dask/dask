There are a variety of other projects related to dask that are often
co-released.  We may want to check their status while releasing


Releasing dask and distributed:

*   Check the release issue on https://github.com/dask/community to see if there
    are any remaining blockers. If not, comment on the issue signalling that you
    are starting the release


*   Update release notes in docs/source/changelog.rst
    Start by using this script to autogenerate some of the changelog entries:

        git log $(git describe --tags --abbrev=0)..HEAD --pretty=format:"- %s \`%an\`_"  > change.md && sed -i -e 's/(#/(:pr:`/g' change.md && sed -i -e 's/) `/`) `/g' change.md

    Replace single backticks with double backticks.

    Sort the entries into subsections and render docs (``make html``) to make
    sure that the changelog renders properly. In particular watch warnings for
    new contributors who don't yet have a github link at the end of the file.

    Add any new contributors' github links to the end of the file
    (``gh pr view --json author <PR>`` is helpful for getting their usernames).

*   Update the versions in all pyproject.toml
    *   distributed version in pyproject.toml of dask/dask
    *   dask version in distributed/pyproject.toml
    *   pins should be >= the current version but < the next version to allow 
        for development installs to resolve correctly

*   Commit

        git commit -a -m "Version YYYY.M.X"

*   Tag commit

        git tag -a YYYY.M.X -m 'Version YYYY.M.X'

*   Push to GitHub

        git push https://github.com/dask/dask main --tags
        git push https://github.com/dask/distributed main --tags

*   Upload to PyPI

        git clean -xfd
        pip install build twine
        pyproject-build
        twine upload dist/*

*   AUTOMATED PATH: Wait for [conda-forge](https://conda-forge.github.io) bots to track the
    change to PyPI. This will typically happen in an hour or two.

    SEMI-AUTOMATED PATH: Trigger the bot to open the PR by creating an issue with the title
    `@conda-forge-admin, please update version` on each of the `dask-core`, `distributed`, 
    and `dask` feedstocks. See more documentation 
    [here](https://conda-forge.org/docs/maintainer/infrastructure/#conda-forge-admin-please-update-version).

    MANUAL PATH: If you don't want to wait for the bots, then follow these steps:
    *  Update conda-smithy and run conda-smithy rerender

            git clone git@github.com:conda-forge/dask-core-feedstock
            cd dask-core-feedstock
            conda install conda-smithy
            conda-smithy rerender

    *  Get sha256 hash from pypi.org
    *  Update version number and hash in recipe
    *  Check dependencies
    *  Do the same for the dask-feedstock meta-package and distributed-feedstock

    There should be three PRs, one to `dask-core`, another to `distributed`, and the
    last to `dask`. You should be able to merge these after tests pass. In some cases
    though you may have to zero out build numbers or update dependencies.

    The packages are interdependent so the tests will pass in a cascade. For instance
    the  `distributed` PR depends on the availability of `dask-core` on conda-forge, so
    its tests won't pass until some time (about an hour) after the `dask-core` PR is merged.
    You can check availability on conda-forge using

        conda search 'conda-forge::dask-core=YYYY.M.X'

    Once `dask-core` is available, you can restart the `distributed` tests by commenting
    on the PR:

        @conda-forge-admin, please restart CI

    `dask` is similar but it depends on `dask-core` _and_ `distributed`.

*   [dask-docker](https://github.com/dask/dask-docker) PRs should be automatically created,
    but they might need a small modification. Check by grepping for the old release string.

*   Automated systems internal to Anaconda Inc then handle updating the
    Anaconda defaults channel

*   Raise an issue (using the release template) in the https://github.com/dask/community
    issue tracker signaling when the next release will occur (usually in 2 weeks).
    Let that issue collect comments to ensure that other maintainers are comfortable
    with releasing.
