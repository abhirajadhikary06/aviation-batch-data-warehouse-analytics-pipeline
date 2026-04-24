from __future__ import annotations

import base64
import os
from pathlib import Path

import requests
from dotenv import load_dotenv


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _env_flag(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _build_auth_header() -> str:
    explicit = os.getenv("OPENOBSERVE_AUTHORIZATION", "").strip()
    if explicit:
        return explicit

    user = os.getenv("OPENOBSERVE_USER", os.getenv("OPENOBSERVE_USERNAME", "")).strip()
    password = os.getenv("OPENOBSERVE_PASSWORD", "").strip()
    if user and password:
        token = base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")
        return f"Basic {token}"
    return ""


def run_openobserve_health_check() -> dict[str, str]:
    """
    Validate OpenObserve endpoint reachability before pipeline execution.

    If OPENOBSERVE_ENABLED is false, returns a skipped status.
    """
    load_dotenv(dotenv_path=_repo_root() / ".env", override=True)

    if not _env_flag("OPENOBSERVE_ENABLED", False):
        return {"status": "skipped", "message": "OPENOBSERVE_ENABLED is false"}

    endpoint = os.getenv("OPENOBSERVE_OTLP_ENDPOINT", "http://localhost:5080/api/default/v1/traces").strip()
    base_url = endpoint.split("/api/", 1)[0]
    streams_url = f"{base_url}/api/default/streams"

    headers: dict[str, str] = {}
    auth = _build_auth_header()
    if auth:
        headers["Authorization"] = auth

    try:
        response = requests.get(streams_url, headers=headers, timeout=10)
    except requests.RequestException as exc:
        raise RuntimeError(f"OpenObserve unreachable at {streams_url}: {exc}") from exc

    if response.status_code >= 400:
        raise RuntimeError(
            f"OpenObserve health check failed ({response.status_code}) at {streams_url}: "
            f"{response.text[:300]}"
        )

    return {"status": "success", "message": "OpenObserve is reachable", "endpoint": streams_url}

