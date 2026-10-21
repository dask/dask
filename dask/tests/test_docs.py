from __future__ import annotations

import subprocess
import sys
from html.parser import HTMLParser
from pathlib import Path

import pytest

ROOT_DIR = Path(__file__).parent.parent.parent


@pytest.mark.parametrize("custom_favicon", [False, True])
def test_favicon_links(tmp_path: Path, custom_favicon: bool) -> None:
    pytest.importorskip("sphinx", minversion="8.0")
    pytest.importorskip("dask_sphinx_theme", minversion="4.0")

    source = tmp_path / "source"
    source.mkdir()
    config = (
        "html_theme = 'dask_sphinx_theme'\n"
        f"templates_path = [{str(ROOT_DIR / 'docs/source/_templates')!r}]\n"
        f"html_static_path = [{str(ROOT_DIR / 'docs/source/_static')!r}]\n"
    )
    if custom_favicon:
        (source / "custom.svg").write_text(
            '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16"/>',
            encoding="utf8",
        )
        config += "html_favicon = 'custom.svg'\n"
    (source / "conf.py").write_text(config, encoding="utf8")
    (source / "index.rst").write_text(
        "Index\n=====\n\n.. toctree::\n\n   generated/api\n   how-to/deeper/page\n",
        encoding="utf8",
    )
    for name in ["generated/api", "how-to/deeper/page"]:
        page = source / f"{name}.rst"
        page.parent.mkdir(parents=True, exist_ok=True)
        page.write_text("Page\n====\n", encoding="utf8")

    output = tmp_path / "html"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "sphinx",
            "-W",
            "-n",
            "-b",
            "html",
            str(source),
            str(output),
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr

    class IconLinks(HTMLParser):
        def __init__(self) -> None:
            super().__init__()
            self.hrefs: list[str] = []

        def handle_starttag(
            self, tag: str, attrs: list[tuple[str, str | None]]
        ) -> None:
            attributes = dict(attrs)
            if tag == "link" and "icon" in (attributes.get("rel") or "").split():
                href = attributes.get("href")
                assert href is not None
                self.hrefs.append(href)

    asset = "custom.svg" if custom_favicon else "images/favicon.svg"
    for name, prefix in [
        ("index.html", ""),
        ("generated/api.html", "../"),
        ("how-to/deeper/page.html", "../../"),
    ]:
        page = output / name
        links = IconLinks()
        links.feed(page.read_text(encoding="utf8"))
        assert links.hrefs == [f"{prefix}_static/{asset}"]
        target = (page.parent / links.hrefs[0]).resolve()
        assert target == (output / "_static" / asset).resolve()
        assert target.is_file()


@pytest.mark.skipif(
    sys.version_info < (3, 11), reason="Requires Python 3.11+ for tomllib"
)
@pytest.mark.skipif(
    not (ROOT_DIR / "pixi.toml").exists(),
    reason="Test can only be run while the project is deployed from git",
)
def test_development_guidelines_matches_ci():
    """When pixi environments change, make sure to update the docs as well."""
    import tomllib

    with open(ROOT_DIR / "pixi.toml", "rb") as f:
        pixi_toml = tomllib.load(f)
    environments = set(pixi_toml["environments"])

    with open(ROOT_DIR / "docs/source/develop.rst", encoding="utf8") as f:
        docs = f.read()

    missing = environments - {"default", "build", "spark"} - set(docs.split())
    assert not missing, (
        "The following pixi environments are not listed in docs/source/develop.rst: "
        f"{missing}"
    )
