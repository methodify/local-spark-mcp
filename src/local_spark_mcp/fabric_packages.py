"""Fabric's notebook-facing Python packages, per runtime profile.

A Fabric notebook assumes the runtime's environment (pandas, scikit-learn,
plotly, the azure-* clients, …). The profiles pin Spark and Delta exactly and
nothing else, so a cell that imports `sklearn` runs on Fabric and fails here.
This module carries a curated roster per profile at Fabric's exact versions,
taken from Microsoft's published environment files
(github.com/microsoft/synapse-spark-runtime, `Fabric-Python3xx-CPU.yml`), and
an opt-in installer: one resolution of the whole roster first, then package by
package for whatever did not resolve, never fatal, with a machine-readable
result. `status()` reads the environment's dist-info, so it stays honest after
any later pip activity.

Curation: every entry notebooks import that is a plain PyPI package with wheels
for Linux, Windows, and macOS. Left out on purpose: `pyspark` and `delta-spark`
(the profile's own pins), Fabric-only wheels (`notebookutils`, `synapseml*`,
`semantic-link-sempy`, `fabric-*`, `kqlmagiccustom`, `prose-*`), GPU and torch
stacks, conda-only system libraries, and 1.3 entries whose conda version is not
a PyPI version (`azure-identity=2023.12.01`, `msal`, `msal-extensions`).

    python -m local_spark_mcp.fabric_packages plan    [--profile fabric-2.0] [--json]
    python -m local_spark_mcp.fabric_packages status  [--profile …] [--json]
    python -m local_spark_mcp.fabric_packages install [--profile …] [--python <exe>] [--dry-run] [--json]
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys

SOURCE = {
    "repo": "https://github.com/microsoft/synapse-spark-runtime",
    "commit": "36097f9fa6be2437643262992c3b4a2990d0e0c8",
    "date": "2026-10-09",
    "files": {
        "fabric-2.0": "Fabric/Runtime 2.0 (Spark 4.1)/Fabric-Python313-CPU.yml",
        "fabric-1.3": "Fabric/Runtime 1.3 (Spark 3.5)/Fabric-Python311-CPU.yml",
    },
}

EXCLUDED = {
    "pyspark": "the profile's own pin (Fabric ships a Microsoft build)",
    "delta-spark": "the profile's own pin",
    "notebookutils": "Fabric-only wheel; the worker ships a shim",
    "synapseml": "Fabric-only wheel", "synapseml-internal": "Fabric-only wheel", "synapseml-mlflow": "Fabric-only wheel",
    "synapseml-utils": "Fabric-only wheel", "semantic-link-sempy": "Fabric-only wheel",
    "fabric-analytics-sdk": "Fabric-only", "fabric-analytics-notebook-plugin": "Fabric-only", "kqlmagiccustom": "Fabric-only",
    "prose-pandas2pyspark": "Fabric-only", "prose-suggestions": "Fabric-only", "powerbiclient": "Fabric-only",
    "spark-mssql-connector-fabric41": "Fabric-only", "spark-mssql-connector-fabric35": "Fabric-only",
    "pydeequ": "Fabric build (+msft)", "geoanalytics-fabric": "Fabric-only", "py4j": "comes with pyspark",
    "torch": "GPU/ML stack, several GB", "transformers": "pulls the torch stack at use time",
    "flaml": "Fabric post-releases (2.5.0.post4/.post6) are not on PyPI",
    "azure-core (fabric-1.3)": "1.30.2 conflicts with the worker's own azure-identity>=1.25.1 (needs azure-core>=1.31); the azure-* clients pull a compatible azure-core",
    "dask (fabric-1.3)": "2023.11.0 breaks inspect.signature on Python 3.11.9+ (dask#11016) and takes lightgbm's import down with it; Fabric runs an older 3.11 patch",
    "azure-identity (fabric-1.3)": "conda meta-version 2023.12.01 is not a PyPI version; the worker's own azure-identity>=1.25.1 serves",
    "msal (fabric-1.3)": "conda meta-version; comes with azure-identity", "msal-extensions (fabric-1.3)": "conda meta-version; comes with azure-identity",
}

# name -> exact version, as in the source file (conda or pip section).
ROSTERS: dict[str, dict[str, str]] = {
    "fabric-2.0": {
        "pandas": "2.3.3", "numpy": "2.4.1", "pyarrow": "24.0.0", "scipy": "1.18.0", "scikit-learn": "1.6.1",
        "statsmodels": "0.14.6", "xgboost": "2.1.4", "lightgbm": "4.6.0", "catboost": "1.2.10", "joblib": "1.4.2",
        "cloudpickle": "3.1.2", "matplotlib": "3.10.9", "seaborn": "0.13.2", "plotly": "6.7.0", "pillow": "12.3.0",
        "ipywidgets": "8.1.7", "requests": "2.34.2", "httpx": "0.28.1", "pyyaml": "6.0.3", "tqdm": "4.68.3",
        "jinja2": "3.1.6", "pydantic": "2.13.4", "openpyxl": "3.1.5", "xlrd": "2.0.2", "sqlalchemy": "2.0.51",
        "pyodbc": "5.3.0", "pandasql": "0.7.3", "python-dateutil": "2.9.0.post0", "pytz": "2026.2", "regex": "2026.5.9",
        "nbformat": "5.10.4", "jsonschema": "4.26.0", "tenacity": "9.1.4", "cachetools": "5.5.2", "protobuf": "5.29.6",
        "cryptography": "49.0.0", "pyjwt": "2.13.0", "psutil": "7.2.2", "boto3": "1.43.45", "sqlparse": "0.5.5",
        "azure-core": "1.39.0", "azure-identity": "1.25.3", "azure-storage-blob": "12.30.0",
        "azure-storage-file-datalake": "12.25.0", "azure-keyvault-secrets": "4.11.0", "azure-datalake-store": "0.0.53",
        "msal": "1.37.0", "msal-extensions": "1.3.1", "fsspec": "2026.6.0", "adlfs": "2025.8.0",
        "nltk": "3.10.0", "mlflow-skinny": "2.22.0", "prophet": "1.3.0", "holidays": "0.100", "optuna": "3.6.1",
        "setuptools": "83.0.0",
    },
    "fabric-1.3": {
        "pandas": "2.1.4", "numpy": "1.26.4", "pyarrow": "14.0.2", "scipy": "1.11.4", "scikit-learn": "1.2.2",
        "statsmodels": "0.14.0", "xgboost": "2.0.3", "lightgbm": "4.3.0", "catboost": "1.2.3", "joblib": "1.2.0",
        "cloudpickle": "2.2.1", "matplotlib": "3.8.0", "seaborn": "0.12.2", "plotly": "5.22.0", "bokeh": "3.3.4",
        "pillow": "10.2.0", "ipywidgets": "8.1.2", "requests": "2.31.0", "pyyaml": "6.0.1", "tqdm": "4.65.0",
        "jinja2": "3.1.3", "openpyxl": "3.0.10", "xlrd": "2.0.1", "xlsxwriter": "3.1.1", "sqlalchemy": "2.0.25",
        "pyodbc": "5.0.1", "pandasql": "0.7.3", "python-dateutil": "2.8.2", "pytz": "2023.3.post1", "regex": "2023.10.3",
        "nbformat": "5.9.2", "jsonschema": "4.19.2", "tenacity": "8.2.3", "protobuf": "3.20.3", "cryptography": "42.0.2",
        "pyjwt": "2.4.0", "psutil": "5.9.0", "sqlparse": "0.4.4", "beautifulsoup4": "4.12.2", "lxml": "4.9.3",
        "networkx": "3.1", "sympy": "1.12",
        "azure-storage-blob": "12.22.0", "azure-storage-file-datalake": "12.16.0",
        "azure-datalake-store": "0.0.53", "fsspec": "2024.6.1", "adlfs": "2024.2.0",
        "nltk": "3.8.1", "mlflow-skinny": "2.12.2", "prophet": "1.1.5", "holidays": "0.48", "optuna": "3.6.1",
        "shap": "0.42.1", "interpret": "0.6.0", "scikit-image": "0.22.0", "imageio": "2.33.1",
        "setuptools": "68.2.2",  # Fabric ships it; mlflow-skinny 2.12 imports pkg_resources, and a fresh venv has none
    },
}


def roster(profile: str) -> dict[str, str]:
    try:
        return dict(ROSTERS[profile])
    except KeyError:
        raise ValueError(f"unknown profile {profile!r}; known: {sorted(ROSTERS)}") from None


def requirements(profile: str) -> list[str]:
    return [f"{n}=={v}" for n, v in roster(profile).items()]


def source(profile: str) -> dict:
    return {"repo": SOURCE["repo"], "commit": SOURCE["commit"], "date": SOURCE["date"], "file": SOURCE["files"].get(profile),
            "url": f"{SOURCE['repo']}/blob/{SOURCE['commit']}/{SOURCE['files'].get(profile, '').replace(' ', '%20')}"}


def _installed_version(name: str) -> str | None:
    from importlib import metadata

    try:
        return metadata.version(name)
    except metadata.PackageNotFoundError:
        return None


def status(profile: str) -> dict:
    """What this interpreter has, against the roster: installed / mismatched / missing."""
    want = roster(profile)
    installed, mismatched, missing = {}, {}, []
    for name, ver in want.items():
        have = _installed_version(name)
        if have is None:
            missing.append(name)
        elif have == ver:
            installed[name] = have
        else:
            mismatched[name] = {"want": ver, "have": have}
    return {"profile": profile, "total": len(want), "installed": len(installed), "mismatched": mismatched, "missing": missing,
            "complete": not missing and not mismatched, "python": sys.executable}


def _installer(python: str) -> list[str]:
    uv = shutil.which("uv")
    if uv:
        return [uv, "pip", "install", "--python", python]
    return [python, "-m", "pip", "install"]


def install(profile: str, python: str | None = None, dry_run: bool = False, progress=None) -> dict:
    """Install the roster into ``python``'s environment: the whole set in one
    resolution first; if that fails, package by package, skipping (and naming)
    what does not resolve on this machine. Never raises for a package that
    cannot be installed; the result says which and why."""
    python = python or sys.executable
    reqs = requirements(profile)
    base = _installer(python)
    result = {"profile": profile, "python": python, "requested": len(reqs), "installed": [], "skipped": [], "mode": "batch",
              "dry_run": dry_run}
    if dry_run:
        result["command"] = base + reqs
        return result

    def run(args: list[str]) -> tuple[bool, str]:
        p = subprocess.run(args, capture_output=True, text=True)
        return p.returncode == 0, (p.stderr or p.stdout).strip()

    ok, out = run(base + reqs)
    if ok:
        result["installed"] = reqs
    else:
        result["mode"] = "per-package"
        result["batch_error"] = out.splitlines()[-1][:300] if out else ""
        for req in reqs:
            if progress:
                progress(req)
            ok, out = run(base + [req])
            if ok:
                result["installed"].append(req)
            else:
                result["skipped"].append({"requirement": req, "reason": _reason(out)})
    result["status"] = status(profile) if python == sys.executable else None
    return result


def _reason(out: str) -> str:
    """The resolver's explanation, one line: uv's text after `╰─▶`, else pip's ERROR line, else the last line."""
    lines = [ln.strip() for ln in out.splitlines() if ln.strip()]
    for i, ln in enumerate(lines):
        # uv: "╰─▶ Because …" (the arrow may not survive a Windows console's encoding, so match "Because" too)
        if ln.startswith("╰─▶") or ln.startswith("Because ") or " Because " in ln[:12]:
            text = ln.split("Because", 1)[1] if "Because" in ln else ln
            return ("Because " + " ".join(x for x in [text.strip()] + lines[i + 1:] if x))[:300]
    for ln in lines:
        if ln.startswith("ERROR:"):
            return ln[:300]
    return (lines[-1] if lines else "install failed")[:300]


def summary_line(st: dict) -> str:
    extra = []
    if st["mismatched"]:
        extra.append("other version: " + ", ".join(f"{n} {v['have']} (Fabric {v['want']})" for n, v in sorted(st["mismatched"].items())))
    if st["missing"]:
        extra.append("missing: " + ", ".join(st["missing"]))
    return f"Fabric packages ({st['profile']}): {st['installed']} of {st['total']} at Fabric's version" + (" · " + " · ".join(extra) if extra else "")


def main(argv: list[str] | None = None) -> int:
    from .profiles import detect_profile

    ap = argparse.ArgumentParser(prog="python -m local_spark_mcp.fabric_packages", description=__doc__.split("\n\n")[0])
    ap.add_argument("command", choices=["plan", "status", "install"])
    ap.add_argument("--profile", default=None, help="fabric-1.3 or fabric-2.0 (default: the installed pyspark's profile)")
    ap.add_argument("--python", default=None, help="install into this interpreter's environment (default: this one)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args(argv)
    profile = a.profile
    if profile is None:
        p = detect_profile()
        profile = p.name if p is not None else "fabric-2.0"
    if a.command == "plan":
        out = {"profile": profile, "source": source(profile), "packages": roster(profile), "excluded": EXCLUDED}
        print(json.dumps(out, indent=2) if a.json else "\n".join(requirements(profile)))
        return 0
    if a.command == "status":
        st = status(profile)
        print(json.dumps(st, indent=2) if a.json else summary_line(st))
        return 0 if st["complete"] else 1
    res = install(profile, a.python, a.dry_run, progress=(None if a.json else lambda r: print(f"  {r}", file=sys.stderr)))
    if a.json:
        print(json.dumps(res, indent=2))
    else:
        if res["dry_run"]:
            print(" ".join(res["command"]))
        else:
            print(f"installed {len(res['installed'])} of {res['requested']} ({res['mode']})"
                  + (": not installable here: " + ", ".join(s["requirement"] for s in res["skipped"]) if res["skipped"] else ""))
            if res.get("status"):
                print(summary_line(res["status"]))
    return 0 if not res.get("skipped") else 2


if __name__ == "__main__":
    sys.exit(main())
