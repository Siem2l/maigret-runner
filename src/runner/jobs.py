"""Background job runner.

A scan is one in-process asyncio task. We don't reach for Celery or
Redis because:

 - The operator is one person investigating a handful of usernames.
 - Scans run for minutes, not hours; in-process is fine.
 - Cancelling / restarting Maigret is trivial without a broker.

If concurrent scans ever become a real workload, swap the executor
for a queue (huey, rq, …). The storage layer doesn't care.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from typing import Any

from prometheus_client import Counter, Histogram

from . import storage
from .settings import settings

log = logging.getLogger(__name__)

# ── Prometheus metrics (exposed at /api/metrics) ─────────────────────────
SCANS_STARTED = Counter("maigret_scans_started_total", "Total scans started")
SCANS_FAILED = Counter("maigret_scans_failed_total", "Total scans that errored out")
SCANS_DURATION = Histogram(
    "maigret_scan_duration_seconds",
    "Wall-clock duration of a full Maigret scan",
    buckets=(5, 15, 30, 60, 120, 300, 600, 1200),
)
SITES_FOUND = Histogram(
    "maigret_sites_found",
    "Number of sites where a hit was reported, per scan",
    buckets=(0, 1, 5, 10, 25, 50, 100, 250),
)


# Module-global queue of running tasks. Dict[job_id, asyncio.Task].
_TASKS: dict[str, asyncio.Task] = {}


async def submit_scan(username: str) -> str:
    """Insert a new job row and kick off the background task. Returns the job id."""
    job_id = uuid.uuid4().hex
    await storage.insert_job(job_id, username)
    SCANS_STARTED.inc()
    _TASKS[job_id] = asyncio.create_task(_run(job_id, username))
    return job_id


async def _run(job_id: str, username: str) -> None:
    """Drive a single scan from queued → running → done/failed."""
    started = time.monotonic()
    await storage.mark_running(job_id)
    try:
        html, results = await _run_maigret(username)
        sites_checked = len(results)
        sites_found = sum(1 for site_data in results.values() if _is_hit(site_data))
        html_key = storage.put_html(job_id, html)
        json_key = storage.put_json(
            job_id,
            {"username": username, "results": _trim_results(results)},
        )
        await storage.mark_done(job_id, sites_checked, sites_found, html_key, json_key)
        SITES_FOUND.observe(sites_found)
        log.info(
            "scan done id=%s user=%s checked=%d found=%d",
            job_id,
            username,
            sites_checked,
            sites_found,
        )
    except Exception as exc:  # noqa: BLE001 — surface every failure path to the operator
        SCANS_FAILED.inc()
        log.exception("scan failed id=%s user=%s", job_id, username)
        await storage.mark_failed(job_id, repr(exc))
    finally:
        SCANS_DURATION.observe(time.monotonic() - started)
        _TASKS.pop(job_id, None)


async def _run_maigret(username: str) -> tuple[bytes, dict[str, Any]]:
    """Invoke Maigret as a library and return (html_bytes, results_dict).

    The public scan function in upstream Maigret is `maigret.maigret.maigret`
    (async), NOT `search` -- the CLI's `main()` calls it as
    `await maigret(...)` and accumulates results into a list of
    `(username, id_type, results)` tuples before passing them to
    `generate_report_context` + `save_html_report`. We replicate the
    same shape for a single-username scan.
    """
    # Lazy import: maigret pulls in a lot of stuff at module load
    # (aiohttp, mock_db, etc.) — keep the import out of cold-start path.
    from maigret.maigret import (  # type: ignore[import-not-found]
        generate_report_context,
        maigret as maigret_search,
        save_html_report,
    )
    from maigret.sites import MaigretDatabase  # type: ignore[import-not-found]
    import logging as _logging
    import tempfile
    from pathlib import Path

    db = MaigretDatabase().load_from_path(_resolve_sites_db_path())
    sites = db.ranked_sites_dict(top=settings.maigret_top_sites)

    log.info("maigret search start username=%s sites=%d", username, len(sites))
    # Upstream's maigret() takes a logger as a required positional arg.
    # Reuse our existing module logger with a child namespace so the
    # library's debug spew lands in the same uvicorn log stream.
    lib_logger = _logging.getLogger("maigret.lib")
    results = await maigret_search(
        username=username,
        site_dict=sites,
        logger=lib_logger,
        timeout=settings.maigret_timeout_per_site,
        is_parsing_enabled=False,
        id_type="username",
        debug=False,
        no_progressbar=True,  # we're headless; the TTY progressbar trashes the log
    )

    # generate_report_context expects a list of (username, id_type, results)
    # tuples even when there's only one username -- mirrors how main()
    # accumulates them (maigret.py:855).
    context = generate_report_context([(username, "username", results)])
    with tempfile.NamedTemporaryFile(suffix=".html", delete=False) as tmp:
        save_html_report(tmp.name, context)
        html_bytes = Path(tmp.name).read_bytes()
    return html_bytes, results


def _resolve_sites_db_path() -> str:
    """The default Maigret sites JSON ships inside the package. Locate it
    without hard-coding a path that changes between maigret versions."""
    import maigret  # type: ignore[import-not-found]

    pkg_dir = Path(maigret.__file__).parent
    return str(pkg_dir / "resources" / "data.json")


def _is_hit(site_data: dict[str, Any]) -> bool:
    """Maigret reports a status object per site; only `Claimed` is a real
    'username exists here' hit. The others are noise (Unknown, Available)."""
    status = site_data.get("status", {})
    return getattr(status, "status", None) == "Claimed" or (
        isinstance(status, dict) and status.get("status") == "Claimed"
    )


def _trim_results(results: dict[str, Any]) -> dict[str, Any]:
    """The full Maigret results dict embeds full HTML response bodies
    per site — we already saved the rendered report, so for the JSON
    sidecar drop the bulkiest fields. The list view only needs
    site URL + status + a thumb-size summary."""
    trimmed = {}
    for site, data in results.items():
        status = data.get("status", {})
        status_dict = (
            status if isinstance(status, dict) else {"status": getattr(status, "status", None)}
        )
        trimmed[site] = {
            "url_user": data.get("url_user"),
            "url_main": data.get("url_main"),
            "status": status_dict,
        }
    return trimmed


# Imports kept at the bottom so type checkers don't see them during the
# top-level module read. Avoids circular references when storage is
# refactored later.
from pathlib import Path  # noqa: E402, F811 — see comment above
