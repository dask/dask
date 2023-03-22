from pathlib import Path

import pytest


def test_development_guidelines_matches_ci():
    """When the environment.yaml changes in CI, make sure to change it in the docs as well"""
    root_dir = Path(__file__).parent.parent.parent

    if not (root_dir / ".github" / "workflows").exists():
        pytest.skip("Test can only be run on an editable install")

    development_doc_file = root_dir / "docs" / "source" / "develop.rst"
    additional_ci_file = root_dir / ".github" / "workflows" / "additional.yml"
    upstream_ci_file = root_dir / ".github" / "workflows" / "upstream.yml"
    latest_env = "environment-3.10.yaml"

    for filename in [development_doc_file, additional_ci_file, upstream_ci_file]:
        with open(filename, encoding="utf8") as f:
            assert any(
                latest_env in line for line in f
            ), f"{latest_env} not found in {filename}"
