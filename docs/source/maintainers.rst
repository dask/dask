=====================
Maintainer Guidelines
=====================

This page describes best practices for Dask maintainers.

Thank you for helping make Dask a useful library and fostering a
welcoming community.

Merging pull requests
=====================

Pull requests should be reviewed
--------------------------------

Pull requests from non-maintainers should be reviewed and approved by at
least one maintainer before being merged.

Ideally, pull requests from maintainers should also be reviewed before
merging. However, because reviewer bandwidth is limited, maintainers will
sometimes self-merge their own pull requests if the content of the pull
request is viewed as uncontroversial (e.g. typo fix). If a maintainer has
a substantial pull request which hasn't received a review, they should ping at
least one team or individual maintainer who might be interested in the proposed
change. If after some time there is no response and the maintainer is confident
in the proposed changes, they should post a final comment along the lines of
"Merging in 48 hours if no further response" and then self-merge if no further
comments are left within the specified time period. Please leave at least
48 hours to allow maintainers in other timezones to respond.

No maintainer should merge a pull request that they're not comfortable
supporting in the future.

CI should pass
--------------

Before merging a pull request, maintainers should make every effort to ensure
that CI passes. Often this will require looking into the logs of a failed run
to see what went wrong, and alerting the pull request author. Ideally, no
pull request should be merged if there are CI failures, as broken CI in ``main``
can easily mask problems with other PRs, and a consistently broken CI can be
demoralizing for maintainers.

However, in practice, there are occasionally flaky tests, broken upstream
dependencies, and failures that are otherwise obviously not related to the PR
at hand. If that is the case, a maintainer may merge a PR with failing tests,
but they should be prepared to follow up with any failures that result from
such an unsafe operation.

Squash merge pull requests
--------------------------

Use squash merging when merging pull requests, as opposed to other merging
strategies like rebase merging. To streamline this process, all non-squash
merging strategies have been disabled in repository GitHub settings.

Use clean merge commits
-----------------------

Dask aims to have a straightforward, yet meaningful, ``git log``. To
accomplish this, maintainers ensure that both the squashed merge commit
title and (optional) message meaningfully reflect the content of the pull
request before merging. For example, a merge commit title of "fix typo" should
be updated to something along the lines of "Fix typo in ``Array.max`` docstring".

Note that when squash merging pull requests GitHub will, by default, pre-populate
the squashed commit message by concatenating all the individual commit messages
in the corresponding pull request. Depending on how careful pull request authors
are during development, this default message may be quite verbose and not very
descriptive. For example::

   * Fix DataFrame.head bug

   * Handle merge conflicts

   * Oops, fix typo

If this is the case, the maintainer merging the pull request should either:

1. Write their own merge commit message that describes the changes in pull request
   (if they have the available bandwidth).
2. Leave the merge commit message blank (again, making sure the commit title is
   meaningful). 
