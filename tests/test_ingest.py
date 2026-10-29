"""Tests for the ingestion service. The database and HDFS are replaced by fakes."""
import pytest

import ingest
from conftest import SAMPLE


@pytest.fixture
def client(tmp_path, monkeypatch):
    landing = tmp_path / "landing"
    landing.mkdir()
    (landing / "BGL_2005-06.log").write_bytes(SAMPLE.read_bytes())
    (landing / "BGL_2005-07.log").write_text("this is not a log file\n" * 50)

    known = {}                                            # fake ops.ingested_files
    monkeypatch.setattr(ingest, "LANDING_DIR", landing)
    monkeypatch.setattr(ingest, "RAW_TARGET", str(tmp_path / "raw"))
    monkeypatch.setattr(ingest, "already_ingested", lambda name: known.get(name))
    monkeypatch.setattr(ingest, "record", lambda name, month, info, path: known.update({name: info["sha256"]}))
    ingest.app.config["TESTING"] = True
    return ingest.app.test_client(), tmp_path, known


def test_inspect_file_counts_real_sample():
    info = ingest.inspect_file(SAMPLE)
    assert info["line_count"] == 2000
    assert info["valid_share"] == 1.0
    assert len(info["sha256"]) == 64


def test_ingest_copies_file_to_raw_zone(client):
    http, tmp_path, known = client
    response = http.post("/ingest", json={"month": "2005-06"})
    assert response.status_code == 201
    assert response.json["line_count"] == 2000
    stored = tmp_path / "raw" / "month=2005-06" / "BGL_2005-06.log"
    assert stored.read_bytes() == SAMPLE.read_bytes()
    assert "BGL_2005-06.log" in known


def test_same_file_twice_is_skipped(client):
    http, _, _ = client
    http.post("/ingest", json={"month": "2005-06"})
    again = http.post("/ingest", json={"month": "2005-06"})
    assert again.status_code == 200
    assert again.json["status"] == "already_ingested"


def test_changed_file_is_refused(client):
    http, tmp_path, _ = client
    http.post("/ingest", json={"month": "2005-06"})
    (tmp_path / "landing" / "BGL_2005-06.log").write_bytes(SAMPLE.read_bytes() + b"\nextra line")
    assert http.post("/ingest", json={"month": "2005-06"}).status_code == 409


def test_wrong_format_is_refused(client):
    http, _, _ = client
    assert http.post("/ingest", json={"month": "2005-07"}).status_code == 422


def test_missing_file_and_bad_month(client):
    http, _, _ = client
    assert http.post("/ingest", json={"month": "2005-08"}).status_code == 404
    assert http.post("/ingest", json={"month": "June"}).status_code == 400
