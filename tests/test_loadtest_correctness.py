"""Load-test success criteria and durable result output regressions."""
from __future__ import annotations

import csv
import json

import httpx
import pytest

from scripts import loadtest


def test_payload_does_not_claim_an_arbitrary_buyer_identity():
    assert "buyer_id" not in loadtest._request_payload(1)


class FakeResponse:
    def __init__(self, status_code, body=None, *, json_error=False):
        self.status_code = status_code
        self._body = body
        self._json_error = json_error

    def json(self):
        if self._json_error:
            raise ValueError("not json")
        return self._body


class FakeClient:
    def __init__(self, response):
        self.response = response

    async def post(self, *_args, **_kwargs):
        return self.response


@pytest.mark.asyncio
async def test_http_200_with_error_final_text_is_not_a_success():
    ok, latency = await loadtest._one_intent(
        FakeClient(FakeResponse(200, {"final_text": "[error] provider unavailable"})),
        0,
    )
    assert ok is False
    assert latency >= 0


@pytest.mark.asyncio
async def test_2xx_with_nonempty_final_text_is_a_success():
    ok, _ = await loadtest._one_intent(
        FakeClient(FakeResponse(201, {"final_text": "已找到合适商品"})),
        0,
    )
    assert ok is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response,category",
    [
        (FakeResponse(200, {"final_text": "  "}), "missing_final_text"),
        (FakeResponse(200, {"other": "reply"}), "missing_final_text"),
        (FakeResponse(503, {"detail": "down"}), "http_5xx"),
        (FakeResponse(200, None, json_error=True), "invalid_json"),
    ],
)
async def test_request_failure_categories_are_distinct(response, category):
    result = await loadtest._one_intent_record(FakeClient(response), 1)
    assert result.success is False
    assert result.failure_category == category


def test_repetitions_require_at_least_three():
    with pytest.raises(ValueError, match="at least 3"):
        loadtest.validate_repetitions(2)
    assert loadtest.validate_repetitions(3) == 3


def test_result_writer_emits_json_csv_and_runtime_metadata(tmp_path):
    metadata = loadtest.build_metadata(
        base_url="http://localhost:8000",
        stages=[2],
        repetitions=3,
        model_kind="synthetic",
    )
    records = [
        loadtest.IntentResult(
            request_index=0,
            repetition=1,
            concurrency=2,
            success=True,
            latency_seconds=0.25,
            status_code=200,
            failure_category=None,
            final_text="ok",
            request={"raw_query": "query"},
            response={"final_text": "ok"},
        ),
    ]

    paths = loadtest.write_results(tmp_path, metadata, records)
    written_json = json.loads(paths["json"].read_text(encoding="utf-8"))
    assert written_json["metadata"]["model_kind"] == "synthetic"
    assert written_json["metadata"]["script_sha256"]
    assert written_json["metadata"]["python_version"]
    assert written_json["metadata"]["os"]
    assert written_json["requests"][0]["success"] is True
    with paths["csv"].open(newline="", encoding="utf-8") as handle:
        row = next(csv.DictReader(handle))
    assert row["concurrency"] == "2"
    assert row["success"] == "True"
    assert row["script_sha256"] == metadata["script_sha256"]
    assert row["model_kind"] == "synthetic"


@pytest.mark.asyncio
async def test_cli_rejects_fewer_than_three_repetitions(monkeypatch, tmp_path):
    monkeypatch.setattr(
        "sys.argv",
        ["loadtest", "--stages", "1", "--repetitions", "2", "--output-dir", str(tmp_path)],
    )
    with pytest.raises(SystemExit) as raised:
        await loadtest.main()
    assert raised.value.code == 2


@pytest.mark.asyncio
async def test_cli_returns_nonzero_when_any_business_result_fails(monkeypatch, tmp_path):
    async def failed_stage(_base_url, concurrency, *, repetition, request_index_start):
        return [
            loadtest.IntentResult(
                request_index=request_index_start,
                repetition=repetition,
                concurrency=concurrency,
                success=False,
                latency_seconds=0.1,
                status_code=200,
                failure_category="business_error",
                final_text="[error] unavailable",
                request={"raw_query": "query"},
                response={"final_text": "[error] unavailable"},
            ),
        ]

    async def no_sleep(_seconds):
        return None

    monkeypatch.setattr(loadtest, "_run_stage", failed_stage)
    monkeypatch.setattr(loadtest.asyncio, "sleep", no_sleep)
    monkeypatch.setattr(
        "sys.argv",
        [
            "loadtest",
            "--stages",
            "1",
            "--repetitions",
            "3",
            "--output-dir",
            str(tmp_path),
            "--model-kind",
            "synthetic",
        ],
    )

    assert await loadtest.main() == 1
    assert list(tmp_path.glob("loadtest-*.json"))
    assert list(tmp_path.glob("loadtest-*.csv"))
