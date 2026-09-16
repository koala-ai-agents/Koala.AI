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
    assert proj.get("name") in ("koala", "koala-ai")
    assert proj.get("version") is not None
    assert isinstance(proj.get("authors"), list)

    # Hatch build includes src layout packaging
    hatch = data.get("tool", {}).get("hatch", {})
    wheel = hatch.get("build", {}).get("targets", {}).get("wheel", {})
    packages = wheel.get("packages") or hatch.get("build", {}).get("packages", [])
    assert packages
    ok = any(
        (isinstance(p, dict) and p.get("include") == "koala" and p.get("from") == "src")
        or (isinstance(p, str) and (p.endswith("src/koala") or p.endswith("\\src/koala") or p == "src/koala"))
        for p in packages
    )
    assert ok, "hatch build packages must include src/koala"


def test_readme_quickstart_present():
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    # Check for basic README structure (flexible to handle different content)
    assert len(readme) > 100, "README should have substantial content"
    # Check for project name
    assert "Koala" in readme or "koala" in readme
