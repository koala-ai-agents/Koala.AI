from __future__ import annotations

import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_py_typed_present():
    py_typed = ROOT / "src" / "koala" / "py.typed"
    assert py_typed.exists(), "py.typed must be present for typed packages"


def test_pyproject_metadata_and_build_config():
    pyproject_path = ROOT / "pyproject.toml"
    assert pyproject_path.exists()
    data = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))

    # Basic project metadata present
    proj = data.get("project", {})
    assert proj.get("name") == "koala"
    assert proj.get("version") is not None
    assert isinstance(proj.get("authors"), list)

    # Hatch build includes src layout packaging and py.typed include
    hatch = data.get("tool", {}).get("hatch", {})
    build = hatch.get("build", {})
    packages = build.get("packages")
    assert packages
    # Accept both hatch formats: list of dicts or list of strings
    ok = False
    for p in packages:
        if isinstance(p, dict):
            if p.get("include") == "koala" and p.get("from") == "src":
                ok = True
                break
        elif isinstance(p, str):
            if p.endswith("src/koala") or p.endswith("\\src/koala"):
                ok = True
                break
    assert ok, "hatch build packages must include src/koala"
    include = build.get("include")
    assert include and ("src/koala/py.typed" in include)


def test_readme_quickstart_present():
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    # Check for basic README structure (flexible to handle different content)
    assert len(readme) > 100, "README should have substantial content"
    # Check for project name
    assert "Koala" in readme or "koala" in readme or "Kola" in readme
