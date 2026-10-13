# scripts/loadtest.py
# 依赖：httpx（uv add --group dev httpx）
# 运行：uv run python scripts/loadtest.py --base-url http://localhost:8000 --stages 5,10,20,40
"""Repeated concurrency load test for the synchronous intent endpoint.

Each request is successful only when it returns HTTP 2xx and a non-empty
``final_text`` without the orchestrator's ``[error]`` marker. The default run
preserves JSON and CSV evidence under ``artifacts/loadtest`` and exits nonzero
when any request fails its business-result check.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import json
import os
import platform
import statistics
import subprocess
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import httpx

# 混合负载：检索型（只读、快）与交易型（含确认、慢）按 8:2 取样
SEARCH_QUERIES = [
    "预算300元，抗造又不塑料的旅行三件套",
    "找几个适合长途飞行的颈枕，要小众设计",
    "登机箱，铝框、静音轮，1000 以内",
]
TRADE_QUERIES = [
    "刚才那个军绿色的收纳套，帮我下单一件寄到美国",
    "查一下我上一笔订单到哪了",
]


@dataclass(frozen=True)
class IntentResult:
    request_index: int
    repetition: int
    concurrency: int
    success: bool
    latency_seconds: float
    status_code: Optional[int]
    failure_category: Optional[str]
    final_text: Optional[str]
    request: dict[str, Any]
    response: Any
    error: Optional[str] = None


def _pick_query(i: int) -> str:
    return TRADE_QUERIES[i % len(TRADE_QUERIES)] if i % 5 == 0 else SEARCH_QUERIES[i % len(SEARCH_QUERIES)]


def _request_payload(i: int) -> dict[str, str]:
    return {
        "raw_query": _pick_query(i),
        "locale": "zh-CN",
        "currency": "CNY",
    }


def _response_failure(status_code: Optional[int], body: Any) -> tuple[Optional[str], Optional[str]]:
    if status_code is None or not 200 <= status_code < 300:
        if status_code is None:
            return "http_error", None
        if 400 <= status_code < 500:
            return "http_4xx", None
        if status_code >= 500:
            return "http_5xx", None
        return "http_non_2xx", None
    if not isinstance(body, dict):
        return "invalid_json", None
    final_text = body.get("final_text")
    if not isinstance(final_text, str) or not final_text.strip():
        return "missing_final_text", final_text if isinstance(final_text, str) else None
    if "[error]" in final_text.casefold():
        return "business_error", final_text
    return None, final_text


async def _one_intent_record(
    client: httpx.AsyncClient,
    i: int,
    *,
    repetition: int = 1,
    concurrency: int = 1,
) -> IntentResult:
    payload = _request_payload(i)
    started = time.monotonic()
    try:
        response = await client.post("/commerce/intents", json=payload)
        status_code = response.status_code
    except (httpx.TimeoutException, asyncio.TimeoutError) as err:
        return IntentResult(
            request_index=i,
            repetition=repetition,
            concurrency=concurrency,
            success=False,
            latency_seconds=time.monotonic() - started,
            status_code=None,
            failure_category="timeout",
            final_text=None,
            request=payload,
            response=None,
            error=f"{type(err).__name__}: {err}",
        )
    except httpx.HTTPError as err:
        return IntentResult(
            request_index=i,
            repetition=repetition,
            concurrency=concurrency,
            success=False,
            latency_seconds=time.monotonic() - started,
            status_code=None,
            failure_category="transport_error",
            final_text=None,
            request=payload,
            response=None,
            error=f"{type(err).__name__}: {err}",
        )

    try:
        body = response.json()
    except (ValueError, json.JSONDecodeError) as err:
        body = {"raw_text": getattr(response, "text", "")}
        failure = "invalid_json" if 200 <= status_code < 300 else _response_failure(status_code, None)[0]
        final_text = None
        error = f"{type(err).__name__}: {err}"
    else:
        failure, final_text = _response_failure(status_code, body)
        error = None
    return IntentResult(
        request_index=i,
        repetition=repetition,
        concurrency=concurrency,
        success=failure is None,
        latency_seconds=time.monotonic() - started,
        status_code=status_code,
        failure_category=failure,
        final_text=final_text,
        request=payload,
        response=body,
        error=error,
    )


async def _one_intent(client: httpx.AsyncClient, i: int) -> tuple[bool, float]:
    """Compatibility wrapper returning the historical ``(ok, latency)`` tuple."""
    result = await _one_intent_record(client, i)
    return result.success, result.latency_seconds


async def _run_stage(
    base_url: str,
    concurrency: int,
    *,
    repetition: int = 1,
    request_index_start: Optional[int] = None,
) -> list[IntentResult]:
    # timeout 要大于单意图端到端最坏耗时，否则测的是客户端超时不是服务端能力
    limits = httpx.Limits(max_connections=concurrency + 10, max_keepalive_connections=concurrency)
    if request_index_start is None:
        request_index_start = (repetition - 1) * concurrency
    token = os.getenv("GLOBEX_ACCESS_TOKEN", "")
    headers = {"Authorization": "Bearer " + token} if token else {}
    async with httpx.AsyncClient(base_url=base_url, timeout=120.0, limits=limits, headers=headers) as client:
        wall_start = time.monotonic()
        results = await asyncio.gather(
            *[
                _one_intent_record(
                    client,
                    request_index_start + local_index,
                    repetition=repetition,
                    concurrency=concurrency,
                )
                for local_index in range(concurrency)
            ],
        )
        wall = time.monotonic() - wall_start

    latencies = [result.latency_seconds for result in results if result.success]
    failures = [result for result in results if not result.success]
    if not latencies:
        print(
            f"[重复 {repetition} 并发 {concurrency:>3}] 全部失败（{len(failures)} 个）"
            f" | 失败类别 {_failure_summary(failures)}"
        )
        return results

    latencies.sort()
    p = lambda q: latencies[min(len(latencies) - 1, int(len(latencies) * q))]
    throughput = len(latencies) / wall * 60
    failure_summary = _failure_summary(failures)
    suffix = f" | 失败类别 {failure_summary}" if failure_summary else ""
    print(
        f"[重复 {repetition} 并发 {concurrency:>3}] 成功 {len(latencies):>3} 失败 {len(failures):>2} | "
        f"P50 {statistics.median(latencies):5.1f}s P95 {p(0.95):5.1f}s P99 {p(0.99):5.1f}s | "
        f"吞吐 {throughput:5.1f} intents/min{suffix}"
    )
    return results


def _failure_summary(results: list[IntentResult]) -> str:
    counts: dict[str, int] = {}
    for result in results:
        if result.failure_category:
            counts[result.failure_category] = counts.get(result.failure_category, 0) + 1
    return ", ".join(f"{kind}={count}" for kind, count in sorted(counts.items()))


def validate_repetitions(value: int) -> int:
    value = int(value)
    if value < 3:
        raise ValueError("repetitions must be at least 3")
    return value


def _parse_repetitions(value: str) -> int:
    try:
        return validate_repetitions(int(value))
    except ValueError as err:
        raise argparse.ArgumentTypeError(str(err)) from err


def _parse_stages(value: str) -> list[int]:
    try:
        stages = [int(item.strip()) for item in value.split(",") if item.strip()]
    except ValueError as err:
        raise argparse.ArgumentTypeError("stages must be comma-separated positive integers") from err
    if not stages or any(stage <= 0 for stage in stages):
        raise argparse.ArgumentTypeError("stages must be comma-separated positive integers")
    return stages


def _model_kind(argument: Optional[str]) -> str:
    if argument:
        return argument
    configured = os.getenv("LLM_MODE", "").strip().lower()
    if configured == "mock":
        return "synthetic"
    if configured == "live":
        return "live"
    return "unspecified"


def _git_sha() -> Optional[str]:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=Path(__file__).resolve().parents[1],
            check=True,
            capture_output=True,
            text=True,
            timeout=2,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return result.stdout.strip() or None


def build_metadata(
    *,
    base_url: str,
    stages: list[int],
    repetitions: int,
    model_kind: str,
) -> dict[str, Any]:
    script_sha256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    repository = Path(__file__).resolve().parents[1]
    source_digest = hashlib.sha256()
    try:
        status = subprocess.run(["git", "status", "--porcelain"], cwd=repository,
            capture_output=True, text=True, check=True, timeout=3).stdout
        paths = subprocess.run(["git", "ls-files", "-co", "--exclude-standard"], cwd=repository,
            capture_output=True, text=True, check=True, timeout=3).stdout.splitlines()
        for path in sorted(set(paths)):
            source = repository / path
            if source.is_file() and (path.startswith(("app/", "scripts/", "frontend/src/"))
                or path in {"pyproject.toml", "uv.lock", "frontend/package-lock.json"}):
                source_digest.update(path.encode() + b"\0" + source.read_bytes() + b"\0")
        source_hash = source_digest.hexdigest()
        dirty = bool(status)
    except (OSError, subprocess.SubprocessError):
        source_hash, dirty = None, None
    return {
        "run_id": datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ"),
        "git_sha": _git_sha(),
        "git_dirty": dirty,
        "workspace_source_sha256": source_hash,
        "script_sha256": script_sha256,
        "python_version": platform.python_version(),
        "os": platform.platform(),
        "os_name": platform.system(),
        "base_url": base_url,
        "stages": stages,
        "concurrency_levels": stages,
        "repetitions": repetitions,
        "model_kind": model_kind,
        "success_criterion": "HTTP 2xx, non-empty final_text, and no [error] marker",
        "sample_size_warning": "Small load-test samples do not establish production SLOs.",
    }


def write_results(
    output_dir: str | Path,
    metadata: dict[str, Any],
    records: list[IntentResult],
) -> dict[str, Path]:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    stem = f"loadtest-{metadata['run_id']}"
    json_path = output_path / f"{stem}.json"
    csv_path = output_path / f"{stem}.csv"
    request_data = [asdict(record) for record in records]
    json_path.write_text(
        json.dumps({"metadata": metadata, "requests": request_data}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    fields = [
        "run_id",
        "git_sha",
        "script_sha256",
        "python_version",
        "os",
        "model_kind",
        "request_index",
        "repetition",
        "concurrency",
        "success",
        "latency_seconds",
        "status_code",
        "failure_category",
        "final_text",
        "request",
        "response",
        "error",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for record in records:
            row = asdict(record)
            for field in ("run_id", "git_sha", "script_sha256", "python_version", "os", "model_kind"):
                row[field] = metadata.get(field)
            row["request"] = json.dumps(row["request"], ensure_ascii=False)
            row["response"] = json.dumps(row["response"], ensure_ascii=False)
            writer.writerow(row)
    return {"json": json_path, "csv": csv_path}


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--stages", default="5,10,20,40", type=_parse_stages, help="逗号分隔的并发阶梯")
    parser.add_argument("--repetitions", default=3, type=_parse_repetitions)
    parser.add_argument("--output-dir", default="artifacts/loadtest")
    parser.add_argument("--model-kind", choices=("live", "synthetic", "unspecified"), default=None)
    args = parser.parse_args()

    model_kind = _model_kind(args.model_kind)
    metadata = build_metadata(
        base_url=args.base_url,
        stages=args.stages,
        repetitions=args.repetitions,
        model_kind=model_kind,
    )
    records: list[IntentResult] = []
    request_index = 0
    for repetition in range(1, args.repetitions + 1):
        for stage_index, stage in enumerate(args.stages):
            records.extend(
                await _run_stage(
                    args.base_url,
                    stage,
                    repetition=repetition,
                    request_index_start=request_index,
                ),
            )
            request_index += stage
            if repetition < args.repetitions or stage_index < len(args.stages) - 1:
                await asyncio.sleep(5)  # 阶梯间歇，让队列排空、限流窗口复位

    paths = write_results(args.output_dir, metadata, records)
    failures = [record for record in records if not record.success]
    print(f"原始记录：{paths['json']} / {paths['csv']}")
    print(f"总计：{len(records)} 请求，{len(failures)} 失败")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
