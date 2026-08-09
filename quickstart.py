#!/usr/bin/env python3
"""One command to get the MedChain dashboard running after cloning the repo.

    python quickstart.py

It does not build anything. Bronze, Silver, Gold and the quality scorecard were
already computed on Azure Databricks and the dashboard's aggregates are sitting in
ADLS; this fetches those, builds the frontend and serves it. A full pipeline run
takes about an hour and costs money — there is no reason for a reader to repeat it,
so the default path does not.

Requirements: Python 3.9+, Node 18+, and the Azure CLI logged in to the subscription
that holds the platform. Nothing else — no Spark, no Databricks CLI, no Java.

Windows, macOS and Linux. Written in Python rather than shell precisely so there is
one script instead of a bash one and a PowerShell one that drift apart.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import platform
import shutil
import subprocess
import sys
import webbrowser
from pathlib import Path

ROOT = Path(__file__).resolve().parent
WEB = ROOT / "dashboards" / "web"
DATA = WEB / "public" / "data"

# Derived exactly as infra/config.sh derives it, so the two cannot disagree.
PROJECT = os.environ.get("PROJECT", "medchain")
ENVIRONMENT = os.environ.get("ENVIRONMENT", "dev")
RESOURCE_GROUP = os.environ.get("RESOURCE_GROUP", f"rg-{PROJECT}-{ENVIRONMENT}")

PANELS = ["headline", "clinical", "operational", "financial", "quality", "reference"]

IS_WINDOWS = platform.system() == "Windows"


# ----------------------------------------------------------------- output


class C:
    """ANSI colours, disabled when the terminal will not render them.

    Windows Terminal and PowerShell 7 handle ANSI; the legacy console does not, and
    NO_COLOR is honoured because some people ask for plain output on purpose.
    """

    _on = (
        sys.stdout.isatty()
        and os.environ.get("NO_COLOR") is None
        and (not IS_WINDOWS or os.environ.get("WT_SESSION") or os.environ.get("TERM"))
    )
    DIM = "\033[2m" if _on else ""
    BOLD = "\033[1m" if _on else ""
    BLUE = "\033[34m" if _on else ""
    GREEN = "\033[32m" if _on else ""
    RED = "\033[31m" if _on else ""
    YELLOW = "\033[33m" if _on else ""
    OFF = "\033[0m" if _on else ""


def step(n: int, total: int, message: str) -> None:
    print(f"\n{C.BLUE}[{n}/{total}]{C.OFF} {C.BOLD}{message}{C.OFF}")


def info(message: str) -> None:
    print(f"      {message}")


def ok(message: str) -> None:
    print(f"      {C.GREEN}OK{C.OFF}  {message}")


def warn(message: str) -> None:
    print(f"      {C.YELLOW}!{C.OFF}   {message}")


def die(message: str, *, fix: str | None = None) -> None:
    print(f"\n{C.RED}Stopped:{C.OFF} {message}")
    if fix:
        print(f"\n{C.BOLD}To fix:{C.OFF} {fix}")
    sys.exit(1)


# ----------------------------------------------------------------- process


def run(
    args: list[str], *, capture: bool = True, check: bool = True, cwd: Path | None = None
) -> subprocess.CompletedProcess:
    """Run a command portably.

    The Windows wrinkle: ``az`` and ``npm`` are ``.cmd`` shims, and since the
    CVE-2024-3566 fix Python refuses to launch a batch file without an explicit
    shell. Routing those through ``cmd /c`` keeps one code path for every platform.
    """
    if IS_WINDOWS and args[0].lower().endswith((".cmd", ".bat")):
        args = ["cmd", "/c", *args]
    return subprocess.run(
        args,
        cwd=str(cwd) if cwd else None,
        check=check,
        text=True,
        capture_output=capture,
        encoding="utf-8",
        errors="replace",
    )


def tool(name: str, *, required: bool = True, fix: str = "") -> str | None:
    """Resolve an executable, accounting for Windows' .cmd shims."""
    found = shutil.which(name)
    if not found and IS_WINDOWS:
        for suffix in (".cmd", ".exe", ".bat"):
            found = shutil.which(name + suffix)
            if found:
                break
    if not found and required:
        die(f"`{name}` is not installed or not on PATH.", fix=fix)
    return found


# ----------------------------------------------------------------- steps


def check_prerequisites(total: int) -> tuple[str, str, str]:
    step(1, total, "Checking prerequisites")

    # noqa: UP036 is deliberate. pyproject requires a newer Python for *developing*
    # the platform; this script only needs the standard library and runs on whatever
    # interpreter a visitor happens to have, before any environment is set up.
    if sys.version_info < (3, 9):  # noqa: UP036
        die(f"Python 3.9+ required, found {platform.python_version()}.")
    ok(f"Python {platform.python_version()}")

    az = tool(
        "az",
        fix="Install the Azure CLI: https://learn.microsoft.com/cli/azure/install-azure-cli\n"
        "        Windows:  winget install Microsoft.AzureCLI\n"
        "        macOS:    brew install azure-cli",
    )
    ok("Azure CLI")

    node = tool(
        "node",
        fix="Install Node 18 or newer: https://nodejs.org\n"
        "        Windows:  winget install OpenJS.NodeJS.LTS\n"
        "        macOS:    brew install node",
    )
    version = run([node, "--version"]).stdout.strip()
    major = int(version.lstrip("v").split(".")[0])
    if major < 18:
        die(f"Node 18+ required, found {version}.", fix="Upgrade Node: https://nodejs.org")
    ok(f"Node {version}")

    npm = tool("npm", fix="npm ships with Node; reinstall Node from https://nodejs.org")
    ok("npm")

    return az, node, npm


def ensure_logged_in(az: str, total: int) -> None:
    step(2, total, "Checking the Azure sign-in")

    result = run([az, "account", "show", "-o", "json"], check=False)
    if result.returncode != 0:
        warn("Not signed in.")
        info("A browser window will open for sign-in.")
        try:
            input("      Press Enter to continue, or Ctrl+C to cancel... ")
        except (EOFError, KeyboardInterrupt):
            die("Cancelled.", fix="Run `az login`, then run this script again.")
        # Not captured: the device-code prompt has to reach the terminal.
        if run([az, "login"], capture=False, check=False).returncode != 0:
            die("`az login` failed.", fix="Run `az login` manually, then re-run this script.")
        result = run([az, "account", "show", "-o", "json"], check=False)
        if result.returncode != 0:
            die("Still not signed in after `az login`.")

    account = json.loads(result.stdout)
    ok(f"Signed in as {account.get('user', {}).get('name', 'unknown')}")
    info(f"Subscription: {account.get('name')}")


def find_storage_account(az: str, override: str | None, total: int) -> str:
    """Locate the platform's storage account instead of hardcoding its name.

    The repository is public, so the account name is not written into it. It is also
    not a secret worth protecting on its own — anonymous access is disabled, so the
    name grants nothing without an authorised sign-in — but leaving deployment
    identifiers out of a public repo is the habit worth keeping, and discovering it
    has the better property anyway: redeploy the platform under a new name and this
    keeps working.
    """
    step(3, total, "Locating the data lake")

    if override:
        ok(f"Using {override} (from --storage-account)")
        return override
    if os.environ.get("STORAGE_ACCOUNT"):
        ok(f"Using {os.environ['STORAGE_ACCOUNT']} (from $STORAGE_ACCOUNT)")
        return os.environ["STORAGE_ACCOUNT"]

    result = run(
        # fmt: off
        [
            az,
            "storage",
            "account",
            "list",
            "-g",
            RESOURCE_GROUP,
            "--query",
            "[].name",
            "-o",
            "json",
        ],
        # fmt: on
        check=False,
    )
    if result.returncode != 0:
        die(
            f"Could not list storage accounts in resource group {RESOURCE_GROUP}.",
            fix="Check you are signed in to the right subscription:\n"
            "        az account show\n"
            "        az account set --subscription <name-or-id>\n"
            "      Or name the account directly:\n"
            "        python quickstart.py --storage-account <name>",
        )

    names = [n for n in json.loads(result.stdout or "[]") if n.startswith("st")]
    if not names:
        die(
            f"No storage account found in {RESOURCE_GROUP}.",
            fix="If the platform lives elsewhere, pass it explicitly:\n"
            "        python quickstart.py --storage-account <name>",
        )
    if len(names) > 1:
        warn(f"Several candidates ({', '.join(names)}); using the first.")
    ok(f"Found {names[0]} in {RESOURCE_GROUP}")
    return names[0]


def fetch_dashboard_data(az: str, storage_account: str, total: int) -> None:
    """Download the pre-computed aggregates. No layer is rebuilt."""
    step(4, total, "Fetching the dashboard data from ADLS")
    DATA.mkdir(parents=True, exist_ok=True)

    result = run(
        [
            az,
            "storage",
            "blob",
            "download-batch",
            "--source",
            "gold",
            "--pattern",
            "_web/*",
            "--destination",
            str(DATA),
            "--account-name",
            storage_account,
            "--auth-mode",
            "login",
            "--overwrite",
            "-o",
            "none",
        ],
        check=False,
    )
    if result.returncode != 0:
        # Match against the whole of stderr, not one line of it. The Azure CLI puts
        # its diagnosis first and a generic suggestion last, so reading only the last
        # line turns "you lack permissions" into an unrelated note about --auth-mode.
        blob = result.stderr or ""
        low = blob.lower()
        lines = [ln.strip() for ln in blob.strip().splitlines() if ln.strip()]
        meaningful = [ln for ln in lines if ln.rstrip(": ").upper() != "ERROR"]
        hint = meaningful[0] if meaningful else "no detail returned"

        # Guessing one cause for every failure sends people down the wrong path. The
        # three that actually happen look nothing alike, so they are told apart.
        if "name or service not known" in low or "failed to resolve" in low:
            fix = (
                f"No storage account named '{storage_account}' exists — the host name\n"
                "      does not resolve. Check the spelling, or drop --storage-account\n"
                "      and let the script discover it:\n"
                "        python quickstart.py"
            )
        elif (
            "required permissions" in low
            or "authorizationpermissionmismatch" in low
            or "not authorized" in low
            or "403" in low
        ):
            fix = (
                "You are signed in, but this identity cannot read the container.\n"
                "      It needs 'Storage Blob Data Reader' on the storage account:\n"
                "        az role assignment create --role 'Storage Blob Data Reader' \\\n"
                "          --assignee <your-email> \\\n"
                f"          --scope /subscriptions/<sub-id>/resourceGroups/{RESOURCE_GROUP}"
                f"/providers/Microsoft.Storage/storageAccounts/{storage_account}\n"
                "      Role changes can take a minute to take effect."
            )
        elif "containernotfound" in low or "specified container does not exist" in low:
            fix = (
                "The storage account exists but has no 'gold' container, so this is\n"
                "      probably not the platform's account. Let discovery pick it:\n"
                "        python quickstart.py"
            )
        else:
            fix = (
                "Check the sign-in and the subscription:\n"
                "        az account show\n"
                "        az account set --subscription <name-or-id>"
            )
        die(f"Download failed: {hint}", fix=fix)

    # download-batch keeps the _web/ prefix as a directory; the frontend fetches
    # /data/*.json, so flatten it.
    nested = DATA / "_web"
    if nested.is_dir():
        for path in nested.glob("*.json"):
            path.replace(DATA / path.name)
        nested.rmdir()

    missing = [p for p in PANELS if not (DATA / f"{p}.json").exists()]
    if missing:
        die(
            f"Expected 6 panel files, missing: {', '.join(missing)}.",
            fix="The cluster export may not have run. Someone with workspace access\n"
            "      can run it with: make run-azure",
        )

    total_kb = sum((DATA / f"{p}.json").stat().st_size for p in PANELS) / 1024
    ok(f"6 panels, {total_kb:.0f} KB")

    source = json.loads((DATA / "headline.json").read_text(encoding="utf-8"))["source"]
    info(f"Computed by {source['engine']} on {source['store']}")
    info(f"Exported     {source['generated_at']}")


def build_frontend(npm: str, total: int) -> None:
    step(5, total, "Building the dashboard")

    if not (WEB / "package.json").exists():
        die(f"{WEB} not found — run this from inside the cloned repository.")

    # `npm ci` is exact and reproducible but needs the lockfile to match; fall back
    # rather than fail a first-time setup on a lockfile drift nobody caused.
    install = ["ci"] if (WEB / "package-lock.json").exists() else ["install"]
    info(f"npm {install[0]} (this takes a minute the first time)")
    result = run([npm, *install], cwd=WEB, check=False)
    if result.returncode != 0 and install == ["ci"]:
        warn("npm ci failed; retrying with npm install")
        result = run([npm, "install"], cwd=WEB, check=False)
    if result.returncode != 0:
        die(f"Dependency install failed:\n{(result.stderr or '')[-1500:]}")
    ok("Dependencies installed")

    info("npm run build")
    result = run([npm, "run", "build"], cwd=WEB, check=False)
    if result.returncode != 0:
        die(f"Build failed:\n{(result.stdout or '')[-800:]}\n{(result.stderr or '')[-800:]}")
    ok("Built dashboards/web/dist")


def serve(npm: str, port: int, open_browser: bool, total: int) -> None:
    step(6, total, "Serving")
    url = f"http://localhost:{port}"
    print()
    print(f"      {C.BOLD}{url}{C.OFF}")
    print(f"      {C.DIM}Ctrl+C to stop{C.OFF}")
    print()

    if open_browser:
        # A headless machine has no browser to open; that is not a failure.
        with contextlib.suppress(Exception):
            webbrowser.open(url)

    # Vite picks the next free port if this one is taken and says so on stdout, so
    # the child's output is left attached rather than captured.
    args = [npm, "run", "preview", "--", "--port", str(port), "--strictPort"]
    if IS_WINDOWS and args[0].lower().endswith((".cmd", ".bat")):
        args = ["cmd", "/c", *args]
    try:
        subprocess.run(args, cwd=str(WEB), check=False)
    except KeyboardInterrupt:
        print("\n      Stopped.")


# ----------------------------------------------------------------- entry


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Set up and run the MedChain dashboard from a fresh clone.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  python quickstart.py                        fetch, build and serve
  python quickstart.py --no-serve             set up only
  python quickstart.py --port 8080            serve on another port
  python quickstart.py --storage-account st…  skip discovery
""",
    )
    parser.add_argument("--storage-account", help="Skip discovery and use this account")
    parser.add_argument("--port", type=int, default=4173, help="Port to serve on (default 4173)")
    parser.add_argument("--no-serve", action="store_true", help="Set up but do not serve")
    parser.add_argument("--no-browser", action="store_true", help="Do not open a browser")
    parser.add_argument("--skip-data", action="store_true", help="Keep existing data/*.json")
    args = parser.parse_args(argv)

    print(f"{C.BOLD}MedChain Analytics — dashboard setup{C.OFF}")
    print(f"{C.DIM}Reads the Gold layer that Azure already built. Nothing is recomputed.{C.OFF}")

    total = 6
    az, _node, npm = check_prerequisites(total)

    if args.skip_data:
        step(2, total, "Skipping the data fetch (--skip-data)")
        if not (DATA / "headline.json").exists():
            die("--skip-data was given but no data is present.", fix="Run without --skip-data.")
        ok("Using the data already in dashboards/web/public/data")
    else:
        ensure_logged_in(az, total)
        account = find_storage_account(az, args.storage_account, total)
        fetch_dashboard_data(az, account, total)

    build_frontend(npm, total)

    if args.no_serve:
        step(6, total, "Done")
        info(f"Serve it with: cd dashboards/web && npm run preview -- --port {args.port}")
        return 0

    serve(npm, args.port, not args.no_browser, total)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nCancelled.")
        raise SystemExit(130) from None
