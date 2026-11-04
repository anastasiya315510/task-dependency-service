import ipaddress
from typing import Literal

from pydantic import SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")

    mongodb_url: SecretStr = SecretStr("mongodb://localhost:27017")
    mongodb_db_name: str = "taskdb"
    redis_url: SecretStr = SecretStr("redis://localhost:6379")
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "tasks.events"
    kafka_consumer_group: str = "task-dependency-service"

    # Seconds to wait for an in-flight Kafka message to finish processing during
    # graceful shutdown. Increase this if MongoDB upserts can take longer than
    # the default under load (e.g. during a replica-set election).
    shutdown_timeout_seconds: int = 30

    cache_chain_ttl_seconds: int = 3600  # complete chains: 1 hour
    cache_incomplete_chain_ttl_seconds: int = 30  # incomplete chains: 30 seconds

    # API key for endpoint authentication. Set via API_KEY env var.
    # When unset (default), all endpoints are publicly accessible — suitable for
    # deployments where network-level controls (VPC, service-mesh mTLS) enforce
    # the exposure boundary. For external-facing deployments, set this to a
    # strong random value and pass it in the X-API-Key request header.
    api_key: SecretStr | None = None

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    # Set to "DEBUG" locally for verbose tracing; use "INFO" or higher in production.
    # Set to "json" in production for structured log ingestion (ELK, CloudWatch, etc.)
    log_format: Literal["text", "json"] = "text"

    # Storage URI for the rate limiter.
    # Defaults to in-process memory — counters are not shared across replicas
    # but no external dependency is needed (safe for CI and local development).
    # For production cluster-wide enforcement, set to the same Redis instance:
    #   RATE_LIMIT_STORAGE_URI=redis://localhost:6379
    rate_limit_storage_uri: str = "memory://"

    @field_validator("rate_limit_storage_uri")
    @classmethod
    def validate_rate_limit_uri(cls, v: str) -> str:
        allowed = ("memory://", "redis://", "rediss://", "redis+sentinel://")
        if not any(v.startswith(p) for p in allowed):
            raise ValueError(
                f"rate_limit_storage_uri must start with one of {allowed}, got {v!r}"
            )
        return v

    # IPs of trusted reverse proxies that may set X-Forwarded-For.
    # Leave empty (default) for direct deployments. Set to your load-balancer's
    # IP when running behind a proxy so rate limiting uses the real client IP.
    # Example env var: TRUSTED_PROXY_IPS='["10.0.0.1", "10.0.0.2"]'
    trusted_proxy_ips: set[str] = set()

    @field_validator("trusted_proxy_ips", mode="after")
    @classmethod
    def validate_proxy_ips(cls, v: set[str]) -> set[str]:
        for ip in v:
            try:
                ipaddress.ip_address(ip)
            except ValueError as exc:
                raise ValueError(
                    f"Invalid IP in trusted_proxy_ips: {ip!r} — must be a valid IPv4/IPv6 address"
                ) from exc
        return v


try:
    settings = Settings()
except Exception as _exc:  # pydantic ValidationError or env-parse error
    raise SystemExit(f"Configuration error — fix the environment and restart:\n{_exc}") from _exc
