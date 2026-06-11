"""FlareSolverr client adapter.

FlareSolverr accepts a single POST endpoint at /v1 and returns the
rendered HTML + cookies for a given URL after solving any Cloudflare
challenge in a headless Chrome instance.

We use this only as a fallback path inside the Maigret scan when a
direct request comes back with a Cloudflare challenge signature.
For most sites Maigret's own requests work fine, and bouncing every
request through Chrome would balloon the per-scan latency from a few
seconds to several minutes.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

log = logging.getLogger(__name__)


class FlareSolverrUnavailable(RuntimeError):
    """Raised when no flaresolverr_url is configured."""


async def request_get(
    flaresolverr_url: str | None,
    target_url: str,
    max_timeout_ms: int = 60_000,
) -> dict[str, Any]:
    """Fetch a single URL through FlareSolverr.

    Returns the raw FlareSolverr response dict. The interesting bits
    live under `.solution.response` (HTML body) and `.solution.cookies`.
    """
    if not flaresolverr_url:
        raise FlareSolverrUnavailable("FLARESOLVERR_URL not configured")

    payload = {
        "cmd": "request.get",
        "url": target_url,
        "maxTimeout": max_timeout_ms,
    }

    # Chrome boot inside the FlareSolverr container takes a few
    # seconds; pad the client timeout above maxTimeout so we never
    # race FlareSolverr's own deadline.
    async with httpx.AsyncClient(timeout=(max_timeout_ms / 1000) + 15) as client:
        resp = await client.post(flaresolverr_url, json=payload)
        resp.raise_for_status()
        return resp.json()


def looks_like_cloudflare_block(status_code: int, body: bytes | str) -> bool:
    """Quick heuristic: should we retry this fetch through FlareSolverr?

    Cloudflare's challenge page is served as a 403 (sometimes 503) and
    always references either 'cf-ray' in the header echo or one of
    'Just a moment' / 'Checking your browser' in the body. We can only
    see the body here, so check the obvious markers.
    """
    if status_code not in (403, 503):
        return False
    needle = body.decode("utf-8", "ignore") if isinstance(body, bytes) else body
    return any(
        marker in needle
        for marker in (
            "Just a moment",
            "Checking your browser",
            "Cloudflare Ray ID",
            "challenge-platform",
        )
    )
