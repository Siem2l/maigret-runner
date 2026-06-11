"""Smoke tests for the SQLite layer. The S3 paths require live Garage
credentials and are exercised by docker-compose tests instead."""

from __future__ import annotations

import os
import tempfile

import pytest

from runner import storage


@pytest.fixture(autouse=True)
def isolated_data_dir(monkeypatch):
    with tempfile.TemporaryDirectory() as tmp:
        monkeypatch.setattr(storage.settings, "data_dir", tmp)
        yield


@pytest.mark.asyncio
async def test_insert_then_list():
    await storage.init_db()
    await storage.insert_job("abc123", "soxoj")
    rows = await storage.list_jobs()
    assert len(rows) == 1
    assert rows[0]["username"] == "soxoj"
    assert rows[0]["status"] == "queued"


@pytest.mark.asyncio
async def test_status_transitions():
    await storage.init_db()
    await storage.insert_job("abc123", "soxoj")
    await storage.mark_running("abc123")
    row = await storage.get_job("abc123")
    assert row is not None
    assert row["status"] == "running"

    await storage.mark_done("abc123", 100, 7, "abc123/report.html", "abc123/report.json")
    row = await storage.get_job("abc123")
    assert row["status"] == "done"
    assert row["sites_found"] == 7
    assert row["finished_at"] is not None


@pytest.mark.asyncio
async def test_failure_path():
    await storage.init_db()
    await storage.insert_job("xyz", "neo")
    await storage.mark_failed("xyz", "boom")
    row = await storage.get_job("xyz")
    assert row["status"] == "failed"
    assert "boom" in row["error"]
