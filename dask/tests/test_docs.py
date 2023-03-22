import os
import sys
from pathlib import Path

import pytest


# borrowing from https://github.com/pypa/pip/blob/0964c9797c1e8de901b045f66fe4d91502cc9877/pip/utils/__init__.py
def is_editable_install():
    """Is distribution an editable install?"""
    for path_item in sys.path:
        egg_link = os.path.join(path_item, "dask.egg-link")
        if os.path.isfile(egg_link):
            return True
    return False


@pytest.mark.skipif(
    not is_editable_install(),
    reason="Requires tests to be ran from an editable install",
)
def test_development_guidelines_matches_ci():
    """When the environment.yaml changes in CI, make sure to change it in the docs as well"""
    root_dir = Path(__file__).parent.parent.parent

    development_doc_file = root_dir / "docs" / "source" / "develop.rst"
    additional_ci_file = root_dir / ".github" / "workflows" / "additional.yml"
    upstream_ci_file = root_dir / ".github" / "workflows" / "upstream.yml"
    latest_env = "environment-3.10.yaml"

    for filename in [development_doc_file, additional_ci_file, upstream_ci_file]:
        with open(filename, encoding="utf8") as f:
            assert any(
                latest_env in line for line in f
            ), f"{latest_env} not found in {filename}"
