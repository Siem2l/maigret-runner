"""Runtime configuration. All knobs come from env vars so the
container image stays the same across deploys."""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Top-level config. Loaded once at startup; mutated nowhere."""

    model_config = SettingsConfigDict(env_prefix="", env_file=None, extra="ignore")

    # ── Server ────────────────────────────────────────────────────────
    host: str = "0.0.0.0"
    port: int = 8080
    public_host: str = "maigret.siem2l.nl"

    # ── Storage ───────────────────────────────────────────────────────
    # On-disk path for the SQLite job index. The HTML reports
    # themselves live in S3, not on disk.
    data_dir: str = "/data"

    # ── Garage S3 ─────────────────────────────────────────────────────
    s3_endpoint_url: str = "http://garage:3900"
    s3_bucket: str = "maigret-reports"
    s3_region: str = "garage"  # garage is region-agnostic but boto requires a value
    aws_access_key_id: str = ""
    aws_secret_access_key: str = ""

    # ── FlareSolverr ──────────────────────────────────────────────────
    flaresolverr_url: str | None = Field(
        default=None,
        description=(
            "Base URL of the FlareSolverr proxy, e.g. http://flaresolverr:8191/v1. "
            "If empty, every scan runs direct (Maigret's own user-agent rotation only) "
            "and Cloudflare-protected targets will show as 'request blocked'."
        ),
    )

    # ── Maigret behaviour ─────────────────────────────────────────────
    # Cap how many sites a single scan can hit. Maigret's default is
    # ~3000 sites which is fine for an investigation but ~5 min per
    # scan. Bump or drop per taste.
    maigret_top_sites: int = 500
    maigret_timeout_per_site: int = 10  # seconds


settings = Settings()
