Dask Contrib
============

In addition to the primary `Dask GitHub Organization <https://github.com/dask>`_ there is also a secondary
`Dask Contrib GitHub Organization <https://github.com/dask-contrib/>`_ which contains community supported
projects which integrate with or enhance the Dask experience.

These projects are owned and maintained by folks outside of the core Dask maintainer group and therefore
the quality and stability of these projects may be different to that of projects in the
Dask GitHub organization. However these projects are
expected to conform to the `Dask Code of Conduct <https://github.com/dask/governance/blob/main/code-of-conduct.md>`_.

The Dask Contrib organization exists to improve discoverability of user submitted community projects.

Contrib package criteria
------------------------

If you are working on a project which you think could be a great addition to the Dask Contrib organziation then please
feel free to raise an issue on the `Dask Community repository <https://github.com/dask/community>`_ requesting a transfer.

For a project to be accepted into Dask Contrib it must meet the following criteria:

- Packages must complement or enhance Dask in some way, maybe it contains plugins, adds integration with some other library, or just generally improves the Dask experience for users.
- Packages must have at least one active maintainer.
- Packages must be installable both from PyPI with ``pip`` and conda-forge with ``conda``.
- Packages must have documentation.

Graduation to the Dask organization
------------------------------------

In some cases it may be appropriate to graduate a project to the main Dask Organization, or perhaps merge the code/functionality
into an existing Dask project.

This will be assessed on a case-by-case basis by the Dask maintenance team.

For some projects it may make sense to graduate their content into other repositories. The now archived ``dask-lightgbm`` project was integrated into
LightGBM for example.

Generally for a project to make it into the Dask GitHub organization it should meet the following criteria:

- Packages much have an active community.
- Packages must be maintained by one or more people who are employed to do so and will dedicate a minimum of 1 day per week to Dask.
- Maintainers must engage in providing support for the package on Stack Overflow.
- Packages must have documentation which use `Dask's sphinx theme <https://github.com/dask/dask-sphinx-theme>`_.
- Packages must have tests which are run via CI and clear acceptance criteria for PRs to enable other maintainers assist.
