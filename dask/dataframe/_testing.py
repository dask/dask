from __future__ import annotations

import os
import sys

PKG = os.path.dirname(os.path.dirname(__file__))


def test_dataframe(
    extra_args: list[str] | None = None, run_doctests: bool = False
) -> None:
    """
    Run the pandas test suite using pytest.

    By default, runs with the marks -m "not slow and not network and not db"

    Parameters
    ----------
    extra_args : list[str], default None
        Extra marks to run the tests.
    run_doctests : bool, default False
        Whether to only run the Python and Cython doctests. If you would like to run
        both doctests/regular tests, just append "--doctest-modules"/"--doctest-cython"
        to extra_args.

    Examples
    --------
    >>> pd.test()  # doctest: +SKIP
    running: pytest...
    """
    import pytest

    cmd = PKG + "/dataframe"
    print(f"running: pytest {cmd}")
    sys.exit(pytest.main(["-n 4"] + [cmd]))


__all__ = ["test_dataframe"]
