from __future__ import annotations

import base64
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from dotenv import load_dotenv

try:
    from opentelemetry import trace
    from opentelemetry.trace import Status, StatusCode
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    OTEL_AVAILABLE = True
except ImportError:
    OTEL_AVAILABLE = False


_CONFIGURED = False


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _env_flag(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _build_auth_header() -> str:
    existing = os.getenv("OPENOBSERVE_AUTHORIZATION", "").strip()
    if existing:
        return existing

    user = os.getenv("OPENOBSERVE_USER", os.getenv("OPENOBSERVE_USERNAME", "")).strip()
    password = os.getenv("OPENOBSERVE_PASSWORD", "").strip()
    if user and password:
        encoded = base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")
        return f"Basic {encoded}"
    return ""


def configure_observability(service_name: str = "aviation-batch-pipeline") -> bool:
    """
    Configure OpenTelemetry tracing export to OpenObserve via OTLP/HTTP.

    Returns:
        True if telemetry was configured, False if disabled/unavailable.
    """
    global _CONFIGURED
    if _CONFIGURED:
        return True

    load_dotenv(dotenv_path=_repo_root() / ".env", override=False)

    if not OTEL_AVAILABLE:
        return False

    if not _env_flag("OPENOBSERVE_ENABLED", default=False):
        return False

    endpoint = os.getenv(
        "OPENOBSERVE_OTLP_ENDPOINT",
        "http://localhost:5080/api/default/v1/traces",
    ).strip()
    if not endpoint:
        return False

    headers: dict[str, str] = {}
    auth_header = _build_auth_header()
    if auth_header:
        headers["Authorization"] = auth_header

    stream_name = os.getenv("OPENOBSERVE_STREAM_NAME", "default").strip()
    if stream_name:
        headers["stream-name"] = stream_name

    provider = TracerProvider(
        resource=Resource.create(
            {
                "service.name": service_name,
                "deployment.environment": os.getenv("ENVIRONMENT", "local"),
            }
        )
    )
    processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint, headers=headers))
    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)
    _CONFIGURED = True
    return True


def set_span_attributes(span: Any, attributes: dict[str, Any]) -> None:
    """Safely set span attributes when tracing is enabled."""
    if not span or not attributes:
        return
    for key, value in attributes.items():
        if value is None:
            continue
        if isinstance(value, (str, int, float, bool)):
            span.set_attribute(key, value)
        else:
            span.set_attribute(key, str(value))


@contextmanager
def traced_span(name: str, attributes: dict[str, Any] | None = None) -> Iterator[Any]:
    """
    Create a tracing span when observability is enabled.
    """
    if not _CONFIGURED or not OTEL_AVAILABLE:
        yield None
        return

    tracer = trace.get_tracer("aviation-batch-pipeline")
    with tracer.start_as_current_span(name) as span:
        set_span_attributes(span, attributes or {})
        try:
            yield span
        except Exception as exc:
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR, str(exc)))
            raise
