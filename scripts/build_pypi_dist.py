from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]

MODULE_SOURCES: tuple[tuple[Path, str], ...] = (
    (REPO_ROOT / "packages" / "axdata-sdk" / "axdata", "axdata"),
    (REPO_ROOT / "libs" / "axdata_core" / "axdata_core", "axdata_core"),
    (
        REPO_ROOT / "packages" / "axdata-source-tdx" / "src" / "axdata_source_tdx",
        "axdata_source_tdx",
    ),
    (
        REPO_ROOT / "packages" / "axdata-source-tdx-ext" / "src" / "axdata_source_tdx_ext",
        "axdata_source_tdx_ext",
    ),
    (
        REPO_ROOT / "packages" / "axdata-source-tencent" / "src" / "axdata_source_tencent",
        "axdata_source_tencent",
    ),
    (
        REPO_ROOT / "packages" / "axdata-source-cninfo" / "src" / "axdata_source_cninfo",
        "axdata_source_cninfo",
    ),
)

RUNTIME_DEPENDENCIES: tuple[str, ...] = (
    "duckdb>=0.9",
    "fastapi>=0.115.0",
    "lxml>=5.2.0",
    "packaging>=23",
    "pandas>=1.5.0",
    "pyarrow>=16.0.0",
    "pydantic>=2.7.0",
    "python-multipart>=0.0.9",
    "requests>=2.31.0",
    "uvicorn[standard]>=0.30.0",
    "websocket-client>=1.8.0",
)


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    output_dir = Path(args.output).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.work_dir:
        work_dir = Path(args.work_dir).expanduser().resolve()
        if work_dir.exists():
            shutil.rmtree(work_dir)
        work_dir.mkdir(parents=True)
        result = _build(args, work_dir, output_dir)
    else:
        with tempfile.TemporaryDirectory(prefix="axdata-pypi-build-") as tmp:
            result = _build(args, Path(tmp), output_dir)

    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(f"Built axdata distribution files into {output_dir}")
        for artifact in result["artifacts"]:
            print(f"- {artifact}")
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build the public PyPI axdata distribution. The repository keeps core "
            "and bundled providers in separate source trees, but the public PyPI "
            "wheel is intentionally a single package."
        )
    )
    parser.add_argument("--output", default="dist", help="Output directory for wheel and sdist.")
    parser.add_argument("--work-dir", help="Optional staging directory to keep for debugging.")
    parser.add_argument(
        "--python",
        default=sys.executable,
        help="Python executable used to run python -m build. Defaults to this interpreter.",
    )
    parser.add_argument("--json", action="store_true", help="Print a JSON summary.")
    return parser


def _build(args: argparse.Namespace, work_dir: Path, output_dir: Path) -> dict[str, object]:
    staging = work_dir / "axdata"
    staging.mkdir(parents=True)

    for source, package_name in MODULE_SOURCES:
        target = staging / package_name
        shutil.copytree(
            source,
            target,
            ignore=shutil.ignore_patterns(
                "__pycache__",
                "*.pyc",
                "*.pyo",
                "*.egg-info",
                "build",
                "dist",
            ),
        )

    shutil.copy2(REPO_ROOT / "packages" / "axdata-sdk" / "README.md", staging / "README.md")
    shutil.copy2(REPO_ROOT / "LICENSE", staging / "LICENSE")
    (staging / "pyproject.toml").write_text(_render_pyproject(), encoding="utf-8")

    before = {path.name for path in output_dir.iterdir() if path.is_file()}
    _run(
        [
            args.python,
            "-m",
            "build",
            "--sdist",
            "--wheel",
            "--outdir",
            str(output_dir),
            str(staging),
        ]
    )
    artifacts = sorted(
        path.name for path in output_dir.iterdir() if path.is_file() and path.name not in before
    )
    if not artifacts:
        raise RuntimeError(f"No artifacts were built in {output_dir}")
    return {
        "staging_dir": str(staging),
        "output_dir": str(output_dir),
        "version": _axdata_version(),
        "artifacts": artifacts,
        "bundled_modules": [package_name for _, package_name in MODULE_SOURCES],
    }


def _render_pyproject() -> str:
    version = _axdata_version()
    dependencies = "\n".join(f"    {_toml_string(item)}," for item in RUNTIME_DEPENDENCIES)
    return f"""[build-system]
requires = ["setuptools>=77", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "axdata"
version = {_toml_string(version)}
description = "Open-source quantitative database framework and Python SDK."
readme = "README.md"
requires-python = ">=3.11"
license = "Apache-2.0"
authors = [
    {{ name = "AxData" }},
]
classifiers = [
    "Development Status :: 3 - Alpha",
    "Intended Audience :: Developers",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
    "Programming Language :: Python :: 3.13",
]
dependencies = [
{dependencies}
]

[project.scripts]
axdata = "axdata_core.cli:main"

[project.entry-points."axdata.providers"]
tdx = "axdata_source_tdx.provider:provider"
tdx_ext = "axdata_source_tdx_ext.provider:provider"
tencent = "axdata_source_tencent.provider:provider"
cninfo = "axdata_source_cninfo.provider:provider"

[tool.setuptools.packages.find]
where = ["."]
include = [
    "axdata*",
    "axdata_core*",
    "axdata_source_tdx*",
    "axdata_source_tdx_ext*",
    "axdata_source_tencent*",
    "axdata_source_cninfo*",
]

[tool.setuptools.package-data]
axdata_core = [
    "_tdx_wire/py.typed",
    "adapters/tdx/resources/finance_maps/*.dat",
    "adapters/tdx/resources/finance_maps/*.cfg",
    "resources/*.json",
]
axdata_source_tdx = [
    "axdata-provider.json",
    "resources/*.json",
    "resources/finance_maps/*.dat",
    "resources/finance_maps/*.cfg",
    "_tdx_wire/py.typed",
]
axdata_source_tdx_ext = ["axdata-provider.json"]
axdata_source_tencent = ["axdata-provider.json"]
axdata_source_cninfo = ["axdata-provider.json"]
"""


def _axdata_version() -> str:
    data = tomllib.loads(
        (REPO_ROOT / "packages" / "axdata-sdk" / "pyproject.toml").read_text(encoding="utf-8")
    )
    return str(data["project"]["version"])


def _toml_string(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def _run(command: list[str]) -> None:
    result = subprocess.run(
        command,
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        if result.stdout:
            print(result.stdout, file=sys.stdout)
        if result.stderr:
            print(result.stderr, file=sys.stderr)
        raise subprocess.CalledProcessError(result.returncode, command)


if __name__ == "__main__":
    raise SystemExit(main())
