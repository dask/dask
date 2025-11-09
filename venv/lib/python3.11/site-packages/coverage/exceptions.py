# Licensed under the Apache License: http://www.apache.org/licenses/LICENSE-2.0
# For details: https://github.com/nedbat/coveragepy/blob/master/NOTICE.txt

"""Exceptions coverage.py can raise."""

from __future__ import annotations

from typing import Any


class CoverageException(Exception):
    """The base class of all exceptions raised by Coverage.py."""

    def __init__(
        self,
        *args: Any,
        slug: str | None = None,
        skip_tests: bool = False,
    ) -> None:
        """Create an exception.

        Args:
            slug: A short string identifying the exception, will be used for
                linking to documentation.
            skip_tests: If True, raising this exception will skip the test it
                is raised in.  This is used for shutting off large numbers of
                tests that we know will not succeed because of a configuration
                mismatch.
        """

        super().__init__(*args)
        self.slug = slug
        self.skip_tests = skip_tests


class ConfigError(CoverageException):
    """A problem with a config file, or a value in one."""

    pass


class DataError(CoverageException):
    """An error in using a data file."""

    pass


class NoDataError(CoverageException):
    """We didn't have data to work with."""

    pass


class NoSource(CoverageException):
    """We couldn't find the source for a module."""

    pass


class NoCode(NoSource):
    """We couldn't find any code at all."""

    pass


class NotPython(CoverageException):
    """A source file turned out not to be parsable Python."""

    pass


class PluginError(CoverageException):
    """A plugin misbehaved."""

    pass


class _ExceptionDuringRun(CoverageException):
    """An exception happened while running customer code.

    Construct it with three arguments, the values from `sys.exc_info`.

    """

    pass


class CoverageWarning(Warning):
    """A warning from Coverage.py."""

    pass
