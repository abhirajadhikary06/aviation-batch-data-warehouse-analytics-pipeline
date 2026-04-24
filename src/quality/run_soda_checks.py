from __future__ import annotations

import argparse
import os
import shutil
import subprocess
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _soda_command() -> list[str]:
    local_soda = _repo_root() / ".venv" / "Scripts" / "soda.exe"
    if local_soda.exists():
        return [str(local_soda)]
    system_soda = shutil.which("soda")
    if system_soda:
        return [system_soda]
    raise RuntimeError(
        "Soda CLI not found. Install Soda first with: "
        "python -m pip install -i https://pypi.cloud.soda.io soda-postgres"
    )


def _is_connection_issue(output: str) -> bool:
    text = output.lower()
    patterns = [
        "unable to connect",
        "could not connect",
        "connection refused",
        "timed out",
        "timeout",
        "network is unreachable",
        "temporary failure in name resolution",
        "name or service not known",
    ]
    return any(pattern in text for pattern in patterns)


def _is_soda_v4_cli_mismatch(output: str) -> bool:
    text = output.lower()
    return "soda v3 commands are not supported" in text or "soda v4 was run" in text


def _resolve_supabase_db_host() -> str:
    explicit_host = os.getenv("SUPABASE_DB_HOST", "").strip()
    if explicit_host:
        return explicit_host

    host = os.getenv("SUPABASE_HOST", "").strip()
    if host:
        if host.startswith("db."):
            return host
        if host.endswith(".supabase.co") and not host.startswith("db."):
            return f"db.{host}"
        return host

    supabase_url = os.getenv("SUPABASE_URL", "").strip()
    if supabase_url:
        parsed = urlparse(supabase_url)
        hostname = parsed.hostname or ""
        if hostname.endswith(".supabase.co"):
            return f"db.{hostname}"

    return "localhost"


def run_soda_quality_checks(
    configuration_path: str | Path = "soda/configuration.yml",
    checks_path: str | Path = "soda/checks/warehouse.yml",
    data_source: str = "supabase_postgres",
) -> dict[str, str]:
    """
    Run Soda quality scans against Postgres warehouse tables.

    Raises:
        RuntimeError: when Soda scan fails or checks fail.
    """
    load_dotenv(dotenv_path=_repo_root() / ".env", override=True)

    env = os.environ.copy()
    env["SUPABASE_DB_HOST"] = env.get("SUPABASE_DB_HOST", "").strip() or _resolve_supabase_db_host()
    env["SUPABASE_PORT"] = env.get("SUPABASE_PORT", "5432")
    env["SUPABASE_SSLMODE"] = env.get("SUPABASE_SSLMODE", "require")

    config_file = Path(configuration_path).resolve()
    checks_file = Path(checks_path).resolve()
    if not config_file.exists():
        raise FileNotFoundError(f"Soda configuration file not found: {config_file}")
    if not checks_file.exists():
        raise FileNotFoundError(f"Soda checks file not found: {checks_file}")

    command = _soda_command() + [
        "scan",
        "-d",
        data_source,
        "-c",
        str(config_file),
        str(checks_file),
    ]

    result = subprocess.run(
        command,
        cwd=str(_repo_root()),
        capture_output=True,
        text=True,
        env=env,
    )
    output = f"{result.stdout}\n{result.stderr}".strip()

    if result.returncode == 0:
        return {
            "status": "success",
            "message": "Soda checks passed",
            "checks_file": str(checks_file),
        }

    if _is_soda_v4_cli_mismatch(output):
        raise RuntimeError(
            "Soda CLI mismatch detected: your environment is running Soda v4, but this project "
            "executes Soda v3 checks YAML scans.\n"
            "Use a Python 3.12 environment and install Soda v3 package:\n"
            "python -m pip install soda-core-postgres==3.5.6"
        )

    if _is_connection_issue(output):
        raise RuntimeError(f"Soda scan failed due to database connection issue:\n{output[:1500]}")

    raise RuntimeError(f"Soda scan failed:\n{output[:1500]}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Soda Core quality checks.")
    parser.add_argument(
        "--config",
        default="soda/configuration.yml",
        help="Path to Soda configuration.yml",
    )
    parser.add_argument(
        "--checks",
        default="soda/checks/warehouse.yml",
        help="Path to Soda checks file",
    )
    args = parser.parse_args()

    scan_result = run_soda_quality_checks(configuration_path=args.config, checks_path=args.checks)
    print(scan_result)
