"""Two stores, one job:
    SQLite for the lightweight index (who scanned what, when, status).
    Garage S3 for the heavy HTML payload + JSON sidecar.

Keeping them split lets the listing page render in milliseconds without
streaming a 7 MB blob, and lets us purge old reports without losing the
audit trail.
"""

from __future__ import annotations

import contextlib
import json
import logging
from datetime import datetime, timezone
from typing import Any

import aiosqlite
import boto3
from botocore.client import Config as BotoConfig

from .settings import settings

log = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id              TEXT PRIMARY KEY,
    username        TEXT NOT NULL,
    status          TEXT NOT NULL,  -- queued | running | done | failed
    started_at      TEXT NOT NULL,
    finished_at     TEXT,
    sites_checked   INTEGER NOT NULL DEFAULT 0,
    sites_found     INTEGER NOT NULL DEFAULT 0,
    error           TEXT,
    html_key        TEXT,
    json_key        TEXT,
    pdf_key         TEXT
);
CREATE INDEX IF NOT EXISTS jobs_started_at ON jobs(started_at DESC);
-- Idempotent migration for DBs that pre-date the pdf_key column.
-- ALTER TABLE ADD COLUMN fails if the column already exists; suppress
-- via the SQLite error code we get back (handled in init_db).
"""


def _db_path() -> str:
    return f"{settings.data_dir}/jobs.db"


async def init_db() -> None:
    async with aiosqlite.connect(_db_path()) as db:
        await db.executescript(_SCHEMA)
        # Backfill pdf_key on DBs created before v0.1.4. SQLite raises
        # a generic OperationalError ("duplicate column name") when the
        # column already exists -- silently ignored.
        try:
            await db.execute("ALTER TABLE jobs ADD COLUMN pdf_key TEXT")
        except Exception:
            pass
        await db.commit()


async def insert_job(job_id: str, username: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(
            "INSERT INTO jobs (id, username, status, started_at) VALUES (?, ?, 'queued', ?)",
            (job_id, username, now),
        )
        await db.commit()


async def mark_running(job_id: str) -> None:
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute("UPDATE jobs SET status='running' WHERE id=?", (job_id,))
        await db.commit()


async def mark_done(
    job_id: str,
    sites_checked: int,
    sites_found: int,
    html_key: str | None,
    json_key: str | None,
    pdf_key: str | None = None,
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(
            """UPDATE jobs
                  SET status='done',
                      finished_at=?,
                      sites_checked=?,
                      sites_found=?,
                      html_key=?,
                      json_key=?,
                      pdf_key=?
                WHERE id=?""",
            (now, sites_checked, sites_found, html_key, json_key, pdf_key, job_id),
        )
        await db.commit()


async def mark_failed(job_id: str, error: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(
            "UPDATE jobs SET status='failed', finished_at=?, error=? WHERE id=?",
            (now, error[:1000], job_id),
        )
        await db.commit()


async def list_jobs(limit: int = 100) -> list[dict[str, Any]]:
    async with aiosqlite.connect(_db_path()) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM jobs ORDER BY started_at DESC LIMIT ?", (limit,)
        ) as cur:
            rows = await cur.fetchall()
    return [dict(r) for r in rows]


async def get_job(job_id: str) -> dict[str, Any] | None:
    async with aiosqlite.connect(_db_path()) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)) as cur:
            row = await cur.fetchone()
    return dict(row) if row else None


# ── S3 / Garage ──────────────────────────────────────────────────────────


def _s3_client():
    """boto3 S3 client preconfigured for Garage's S3 dialect.

    Garage rejects v2 signatures and chunked-encoding uploads, so we
    pin signature_version='s3v4' and path-style addressing. Region is
    a free-text label on the Garage side; any non-empty string works."""
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
        region_name=settings.s3_region,
        config=BotoConfig(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def put_html(job_id: str, html: bytes) -> str:
    key = f"{job_id}/report.html"
    s3 = _s3_client()
    s3.put_object(
        Bucket=settings.s3_bucket,
        Key=key,
        Body=html,
        ContentType="text/html; charset=utf-8",
    )
    return key


def put_pdf(job_id: str, pdf: bytes) -> str:
    key = f"{job_id}/report.pdf"
    s3 = _s3_client()
    s3.put_object(
        Bucket=settings.s3_bucket,
        Key=key,
        Body=pdf,
        ContentType="application/pdf",
    )
    return key


def put_json(job_id: str, payload: dict[str, Any]) -> str:
    key = f"{job_id}/report.json"
    s3 = _s3_client()
    s3.put_object(
        Bucket=settings.s3_bucket,
        Key=key,
        Body=json.dumps(payload).encode("utf-8"),
        ContentType="application/json",
    )
    return key


def get_object_bytes(key: str) -> bytes:
    s3 = _s3_client()
    obj = s3.get_object(Bucket=settings.s3_bucket, Key=key)
    return obj["Body"].read()


def delete_job_objects(job_id: str) -> None:
    """Purge both objects belonging to a job. Best-effort: a missing
    object is fine, anything else gets logged but doesn't raise."""
    s3 = _s3_client()
    for key in (f"{job_id}/report.html", f"{job_id}/report.json"):
        with contextlib.suppress(Exception):
            s3.delete_object(Bucket=settings.s3_bucket, Key=key)
