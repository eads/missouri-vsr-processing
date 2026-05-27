"""Tests for the appelson 287g mirror fetcher (assets.program_287g)."""
from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest

from missouri_vsr.assets import program_287g
from missouri_vsr.assets.program_287g import _fetch_latest_appelson_snapshot


FAKE_XLSX_BYTES = b"PK\x03\x04 fake xlsx contents for testing"
LATEST_FOLDER = "sheets_20260520_175436"
LATEST_FILENAME = "participatingAgencies05192026am.xlsx"
LATEST_DOWNLOAD_URL = (
    "https://raw.githubusercontent.com/appelson/Tracking_287g/main/sheets/"
    f"{LATEST_FOLDER}/{LATEST_FILENAME}"
)


class _StubLog:
    def __init__(self):
        self.messages: list[str] = []

    def info(self, msg, *args):
        self.messages.append(msg % args if args else msg)


def _make_transport() -> httpx.MockTransport:
    """Mock the three HTTP calls the fetcher makes: list sheets, list folder, download xlsx."""

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url == program_287g.APPELSON_SHEETS_API:
            return httpx.Response(
                200,
                json=[
                    {"name": "sheets_20260101_000000", "type": "dir"},
                    {"name": LATEST_FOLDER, "type": "dir"},
                    {"name": "sheets_20260301_120000", "type": "dir"},
                    {"name": "README.md", "type": "file"},
                ],
            )
        if url == f"{program_287g.APPELSON_SHEETS_API}/{LATEST_FOLDER}":
            return httpx.Response(
                200,
                json=[
                    {
                        "name": LATEST_FILENAME,
                        "type": "file",
                        "size": len(FAKE_XLSX_BYTES),
                        "download_url": LATEST_DOWNLOAD_URL,
                    }
                ],
            )
        if url == LATEST_DOWNLOAD_URL:
            return httpx.Response(200, content=FAKE_XLSX_BYTES)
        raise AssertionError(f"Unexpected request: {url}")

    return httpx.MockTransport(handler)


@pytest.fixture
def patched_httpx(monkeypatch):
    transport = _make_transport()
    real_client = httpx.Client

    def make_client(*args, **kwargs):
        kwargs["transport"] = transport
        return real_client(*args, **kwargs)

    monkeypatch.setattr(program_287g.httpx, "Client", make_client)


def test_fetches_latest_folder_and_writes_file(tmp_path: Path, patched_httpx):
    log = _StubLog()
    result = _fetch_latest_appelson_snapshot(tmp_path, log)

    assert result["filename"] == LATEST_FILENAME
    assert result["appelson_folder"] == LATEST_FOLDER
    assert result["downloaded"] is True
    target = tmp_path / LATEST_FILENAME
    assert target.exists()
    assert target.read_bytes() == FAKE_XLSX_BYTES

    log_path = tmp_path / "snapshots.jsonl"
    assert log_path.exists()
    entries = [json.loads(line) for line in log_path.read_text().splitlines() if line]
    assert len(entries) == 1
    entry = entries[0]
    assert entry["filename"] == LATEST_FILENAME
    assert entry["snapshot_date"] == "2026-05-19"
    assert entry["snapshot_period"] == "am"
    assert entry["fetched_by"] == "appelson_mirror"
    assert entry["sha256"] == result["sha256"]


def test_idempotent_when_file_already_present(tmp_path: Path, patched_httpx):
    target = tmp_path / LATEST_FILENAME
    target.write_bytes(b"pre-existing different bytes")
    log_path = tmp_path / "snapshots.jsonl"
    log_path.write_text(json.dumps({"filename": LATEST_FILENAME, "marker": "pre-existing"}) + "\n")

    log = _StubLog()
    result = _fetch_latest_appelson_snapshot(tmp_path, log)

    assert result["filename"] == LATEST_FILENAME
    assert result["downloaded"] is False
    # File on disk must be untouched (proves no overwrite)
    assert target.read_bytes() == b"pre-existing different bytes"
    # No new jsonl entry
    entries = [json.loads(line) for line in log_path.read_text().splitlines() if line]
    assert len(entries) == 1
    assert entries[0]["marker"] == "pre-existing"


def test_picks_lexically_latest_folder(tmp_path: Path, monkeypatch):
    """Verify folder sort picks the newest timestamp even when the API returns them out of order."""
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        calls.append(url)
        if url == program_287g.APPELSON_SHEETS_API:
            # Newest is sheets_20260520_175436; deliberately list out of order.
            return httpx.Response(
                200,
                json=[
                    {"name": "sheets_20260520_175436", "type": "dir"},
                    {"name": "sheets_20250101_000000", "type": "dir"},
                    {"name": "sheets_20260101_120000", "type": "dir"},
                ],
            )
        if url == f"{program_287g.APPELSON_SHEETS_API}/sheets_20260520_175436":
            return httpx.Response(
                200,
                json=[
                    {
                        "name": LATEST_FILENAME,
                        "type": "file",
                        "size": len(FAKE_XLSX_BYTES),
                        "download_url": LATEST_DOWNLOAD_URL,
                    }
                ],
            )
        if url == LATEST_DOWNLOAD_URL:
            return httpx.Response(200, content=FAKE_XLSX_BYTES)
        raise AssertionError(f"Unexpected request: {url}")

    transport = httpx.MockTransport(handler)
    real_client = httpx.Client
    monkeypatch.setattr(
        program_287g.httpx,
        "Client",
        lambda *a, **kw: real_client(*a, transport=transport, **kw),
    )

    log = _StubLog()
    result = _fetch_latest_appelson_snapshot(tmp_path, log)
    assert result["appelson_folder"] == "sheets_20260520_175436"
    # Folder-listing call targeted the newest folder, not an older one.
    assert f"{program_287g.APPELSON_SHEETS_API}/sheets_20260520_175436" in calls
