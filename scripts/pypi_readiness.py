from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.work_dir:
        work_dir = Path(args.work_dir).expanduser().resolve()
        work_dir.mkdir(parents=True, exist_ok=True)
        return _run_check(args, work_dir)

    with tempfile.TemporaryDirectory(prefix="axdata-pypi-readiness-") as tmp:
        return _run_check(args, Path(tmp))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build the public axdata PyPI distribution and install it in a fresh "
            "temporary venv. This script never uploads to PyPI."
        )
    )
    parser.add_argument(
        "--python",
        default=sys.executable,
        help="Python executable used to create temporary venvs. Defaults to this interpreter.",
    )
    parser.add_argument(
        "--work-dir",
        help="Keep temporary sources, dists, and venvs under this directory for debugging.",
    )
    parser.add_argument(
        "--skip-twine-check",
        action="store_true",
        help="Skip `twine check` for generated wheel/sdist files.",
    )
    parser.add_argument(
        "--upgrade-pip",
        action="store_true",
        help="Upgrade pip in temporary venvs before installing tools/packages.",
    )
    parser.add_argument("--json", action="store_true", help="Print a JSON summary instead of text.")
    return parser


def _run_check(args: argparse.Namespace, work_dir: Path) -> int:
    dist_dir = work_dir / "dist"
    staging_dir = work_dir / "staging"
    data_root = work_dir / "runtime" / "data"
    build_venv = work_dir / "build-venv"
    install_venv = work_dir / "install-venv"
    commands: list[str] = []

    _create_venv(args.python, build_venv)
    build_python = _venv_python(build_venv)
    build_pip = [str(build_python), "-m", "pip"]
    if args.upgrade_pip:
        _run([*build_pip, "install", "--upgrade", "pip"], commands=commands)
    _run([*build_pip, "install", "build>=1.2"], commands=commands)
    if not args.skip_twine_check:
        _run([*build_pip, "install", "twine>=5"], commands=commands)

    build_summary = _run_json(
        [
            str(build_python),
            str(REPO_ROOT / "scripts" / "build_pypi_dist.py"),
            "--output",
            str(dist_dir),
            "--work-dir",
            str(staging_dir),
            "--json",
        ],
        commands=commands,
    )
    artifacts = sorted(path for path in dist_dir.iterdir() if path.is_file())

    if not args.skip_twine_check:
        _run(
            [str(build_python), "-m", "twine", "check", *map(str, artifacts)],
            commands=commands,
        )

    _create_venv(args.python, install_venv)
    install_python = _venv_python(install_venv)
    install_pip = [str(install_python), "-m", "pip"]
    if args.upgrade_pip:
        _run([*install_pip, "install", "--upgrade", "pip"], commands=commands)

    wheel_path = _find_wheel(dist_dir)
    _run(
        [*install_pip, "install", "--find-links", str(dist_dir), str(wheel_path)],
        commands=commands,
    )

    import_summary = _verify_python_imports(install_python, commands=commands)
    axdata = _venv_console(install_venv, "axdata")
    _run_capture([str(axdata), "--help"], commands=commands)
    _run_json([str(axdata), "--data-root", str(data_root), "init", "--json"], commands=commands)
    doctor = _run_json(
        [str(axdata), "--data-root", str(data_root), "doctor", "--json"],
        commands=commands,
    )
    status = _run_json(
        [str(axdata), "--data-root", str(data_root), "status", "--json"],
        commands=commands,
    )
    providers = _run_json(
        [str(axdata), "--data-root", str(data_root), "plugin", "list", "--json"],
        commands=commands,
    )
    provider_summary = _summarize_providers(providers)
    _verify_provider_ids(provider_summary)
    _verify_default_enabled_providers(provider_summary)

    summary = {
        "work_dir": str(work_dir),
        "dist_dir": str(dist_dir),
        "data_root": str(data_root),
        "package": "axdata",
        "build": build_summary,
        "artifacts": [path.name for path in artifacts],
        "wheel_installed": wheel_path.name,
        "twine_check": "skipped" if args.skip_twine_check else "passed",
        "doctor_status": doctor.get("summary", {}).get("status") if isinstance(doctor, dict) else None,
        "runtime_status": status.get("summary", {}).get("status") if isinstance(status, dict) else None,
        "imports": import_summary,
        "providers": provider_summary,
        "commands": commands,
    }
    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    else:
        print("AxData PyPI readiness check passed")
        print(f"built package: {summary['package']}")
        print(f"twine_check={summary['twine_check']}")
        print(f"doctor_status={summary['doctor_status']}")
        print(f"runtime_status={summary['runtime_status']}")
        print(f"work_dir={work_dir}")
    return 0


def _find_wheel(dist_dir: Path) -> Path:
    matches = sorted(dist_dir.glob("axdata-*.whl"))
    if not matches:
        raise RuntimeError(f"No axdata wheel found in {dist_dir}")
    return matches[-1]


def _verify_python_imports(python: Path, *, commands: list[str]) -> dict[str, object]:
    code = """
import importlib
import importlib.metadata as metadata
import importlib.resources as resources
import json

modules = [
    "axdata",
    "axdata_core",
    "axdata_source_tdx",
    "axdata_source_tdx_ext",
    "axdata_source_tencent",
    "axdata_source_cninfo",
]
for module in modules:
    importlib.import_module(module)

resources_to_check = {
    "axdata_source_tdx": [
        "axdata-provider.json",
        "resources/tdx_quote_servers.json",
        "resources/finance_maps/incon.dat",
    ],
    "axdata_source_tdx_ext": ["axdata-provider.json"],
    "axdata_source_tencent": ["axdata-provider.json"],
    "axdata_source_cninfo": ["axdata-provider.json"],
}
for package, paths in resources_to_check.items():
    root = resources.files(package)
    for item in paths:
        assert root.joinpath(item).is_file(), f"missing {package}:{item}"

entry_points = {
    item.name: item.value
    for item in metadata.entry_points(group="axdata.providers")
}
required_entry_points = {"tdx", "tdx_ext", "tencent", "cninfo"}
missing = sorted(required_entry_points - set(entry_points))
if missing:
    raise AssertionError(f"missing provider entry points: {missing}")

requires = metadata.requires("axdata") or []
unexpected_split_deps = [
    requirement
    for requirement in requires
    if requirement.startswith("axdata-core") or requirement.startswith("axdata-source-")
]
if unexpected_split_deps:
    raise AssertionError(f"axdata should be a single public wheel: {unexpected_split_deps}")

print(json.dumps({
    "modules": modules,
    "provider_entry_points": entry_points,
    "axdata_dependencies": requires,
}, ensure_ascii=False))
""".strip()
    result = _run_capture([str(python), "-c", code], commands=commands)
    return json.loads(result.stdout)


def _summarize_providers(payload: object) -> dict[str, dict[str, object]]:
    if not isinstance(payload, list):
        raise RuntimeError(f"Expected provider list JSON, got: {payload!r}")
    summary: dict[str, dict[str, object]] = {}
    for item in payload:
        if not isinstance(item, dict):
            continue
        provider_id = item.get("provider_id")
        if not isinstance(provider_id, str):
            continue
        summary[provider_id] = {
            "status": item.get("status"),
            "enabled": item.get("enabled"),
            "interfaces": item.get("interfaces"),
        }
    return summary


def _verify_provider_ids(providers: dict[str, dict[str, object]]) -> None:
    required = (
        "axdata.source.tdx_external",
        "axdata.source.tdx_ext_external",
        "axdata.source.tencent_external",
        "axdata.source.cninfo_external",
    )
    missing = [provider_id for provider_id in required if provider_id not in providers]
    if missing:
        raise RuntimeError(f"Bundled provider(s) not discovered: {missing}")


def _verify_default_enabled_providers(providers: dict[str, dict[str, object]]) -> None:
    required_enabled = ("axdata.source.tdx_external", "axdata.source.tdx_ext_external")
    disabled = [
        provider_id
        for provider_id in required_enabled
        if providers.get(provider_id, {}).get("status") != "enabled"
        or providers.get(provider_id, {}).get("enabled") is not True
    ]
    if disabled:
        raise RuntimeError(f"Provider(s) should be enabled by default after install: {disabled}")


def _create_venv(python: str, venv_dir: Path) -> None:
    _run([python, "-m", "venv", str(venv_dir)], commands=[])


def _venv_python(venv_dir: Path) -> Path:
    if os.name == "nt":
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python"


def _venv_console(venv_dir: Path, name: str) -> Path:
    candidates = (
        [venv_dir / "Scripts" / f"{name}.exe", venv_dir / "Scripts" / name]
        if os.name == "nt"
        else [venv_dir / "bin" / name]
    )
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise RuntimeError(f"Console script {name!r} was not installed in {venv_dir}")


def _run_json(command: list[str], *, commands: list[str]) -> object:
    result = _run_capture(command, commands=commands)
    return json.loads(result.stdout)


def _run_capture(command: list[str], *, commands: list[str]) -> subprocess.CompletedProcess[str]:
    return _run(command, commands=commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def _run(
    command: list[str],
    *,
    commands: list[str],
    stdout: int | None = None,
    stderr: int | None = None,
) -> subprocess.CompletedProcess[str]:
    commands.append(_display_command(command))
    result = subprocess.run(
        command,
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE if stdout is None else stdout,
        stderr=subprocess.PIPE if stderr is None else stderr,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        if result.stdout:
            print(result.stdout, file=sys.stdout)
        if result.stderr:
            print(result.stderr, file=sys.stderr)
        raise subprocess.CalledProcessError(result.returncode, command, result.stdout, result.stderr)
    return result


def _display_command(command: list[str]) -> str:
    return " ".join(str(part) for part in command)


if __name__ == "__main__":
    raise SystemExit(main())
