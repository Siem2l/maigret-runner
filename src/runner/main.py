"""FastAPI surface. Three pages + four JSON endpoints.

Routes:
    GET  /              → scan list (Jinja)
    GET  /new           → new-scan form (Jinja)
    POST /              → start a scan, redirect to detail
    GET  /scans/{id}    → scan detail (Jinja)
    GET  /api/scans     → JSON list
    GET  /api/scans/{id}/html  → stream HTML report from Garage
    GET  /api/scans/{id}/json  → stream JSON sidecar from Garage
    GET  /api/metrics   → Prometheus exposition
    GET  /healthz       → liveness probe

Static assets piggyback off Tailwind's Play CDN — there's no /static
directory because the whole UI is style classes inside the templates.
"""

from __future__ import annotations

import logging
import re
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from . import jobs, storage
from .settings import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("maigret-runner")

USERNAME_RE = re.compile(r"^[A-Za-z0-9._\-]{1,64}$")

_templates_dir = Path(__file__).resolve().parent.parent.parent / "templates"
templates = Jinja2Templates(directory=str(_templates_dir))


@asynccontextmanager
async def lifespan(app: FastAPI):
    Path(settings.data_dir).mkdir(parents=True, exist_ok=True)
    await storage.init_db()
    log.info("maigret-runner ready on %s:%d", settings.host, settings.port)
    yield


app = FastAPI(
    title="maigret-runner",
    version="0.1.0",
    lifespan=lifespan,
    docs_url="/api/docs",
)


# ── Pages ────────────────────────────────────────────────────────────────


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> Response:
    rows = await storage.list_jobs()
    return templates.TemplateResponse(
        "scans.html",
        {"request": request, "jobs": rows, "public_host": settings.public_host},
    )


@app.get("/new", response_class=HTMLResponse)
async def new_form(request: Request) -> Response:
    return templates.TemplateResponse(
        "new.html",
        {"request": request, "public_host": settings.public_host},
    )


@app.post("/", response_class=HTMLResponse)
async def submit(username: str = Form(...)) -> Response:
    username = username.strip()
    if not USERNAME_RE.match(username):
        raise HTTPException(
            status_code=400,
            detail="usernames must be 1-64 chars of [A-Za-z0-9._-]",
        )
    job_id = await jobs.submit_scan(username)
    return RedirectResponse(url=f"/scans/{job_id}", status_code=303)


@app.get("/scans/{job_id}", response_class=HTMLResponse)
async def detail(request: Request, job_id: str) -> Response:
    row = await storage.get_job(job_id)
    if row is None:
        raise HTTPException(status_code=404, detail="not found")
    return templates.TemplateResponse(
        "detail.html",
        {"request": request, "job": row, "public_host": settings.public_host},
    )


# ── JSON / report streaming ──────────────────────────────────────────────


@app.get("/api/scans")
async def api_list_scans() -> list[dict]:
    return await storage.list_jobs()


@app.get("/api/scans/{job_id}")
async def api_get_scan(job_id: str) -> dict:
    row = await storage.get_job(job_id)
    if row is None:
        raise HTTPException(status_code=404, detail="not found")
    return row


@app.get("/api/scans/{job_id}/html")
async def api_html(job_id: str) -> Response:
    row = await storage.get_job(job_id)
    if row is None or not row.get("html_key"):
        raise HTTPException(status_code=404, detail="report not ready")
    body = storage.get_object_bytes(row["html_key"])
    return Response(content=body, media_type="text/html; charset=utf-8")


@app.get("/api/scans/{job_id}/json")
async def api_json(job_id: str) -> Response:
    row = await storage.get_job(job_id)
    if row is None or not row.get("json_key"):
        raise HTTPException(status_code=404, detail="report not ready")
    body = storage.get_object_bytes(row["json_key"])
    return Response(content=body, media_type="application/json")


# ── Ops endpoints ────────────────────────────────────────────────────────


@app.get("/api/metrics")
def metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}
