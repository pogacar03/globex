"""Provider token usage and explicitly priced cost estimates.

Provider usage is evidence about tokens consumed. Price-based costs are estimates
tagged with the configured price version; they are never represented as billed
amounts because billing data is not available from the model response.
"""
from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from typing import Any, Mapping, Optional


@dataclass(frozen=True)
class ModelUsageRecord:
    model: str
    usage_status: str
    input_tokens: Optional[int]
    output_tokens: Optional[int]
    total_tokens: Optional[int]
    estimated_cost_usd: Optional[float]
    pricing_version: Optional[str]
    billed_cost_usd: None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class ModelUsageLedger:
    """Collect provider usage and estimates from a versioned rate table.

    `rates` is keyed by model and each value may contain
    `input_usd_per_million_tokens` and `output_usd_per_million_tokens`. An
    estimate is emitted only when both provider counts and both configured rates
    are available.
    """

    def __init__(
        self,
        *,
        pricing_version: str = "",
        rates: Optional[Mapping[str, Mapping[str, float]]] = None,
    ) -> None:
        self.pricing_version = pricing_version.strip() or None
        self.rates = dict(rates or {})
        self.records: list[ModelUsageRecord] = []

    def record(self, response: Any, *, model: str) -> ModelUsageRecord:
        usage = _field(response, "usage") if response is not None else None
        input_tokens = _first_token_count(usage, "input_tokens", "prompt_tokens")
        output_tokens = _first_token_count(usage, "output_tokens", "completion_tokens")
        total_tokens = _first_token_count(usage, "total_tokens")
        if total_tokens is None and input_tokens is not None and output_tokens is not None:
            total_tokens = input_tokens + output_tokens

        actual = any(value is not None for value in (input_tokens, output_tokens, total_tokens))
        estimated = self._estimate(model, input_tokens, output_tokens) if actual else None
        record = ModelUsageRecord(
            model=model,
            usage_status="actual" if actual else "unknown",
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            total_tokens=total_tokens,
            estimated_cost_usd=estimated,
            pricing_version=self.pricing_version if estimated is not None else None,
            billed_cost_usd=None,
        )
        self.records.append(record)
        return record

    def _estimate(
        self,
        model: str,
        input_tokens: Optional[int],
        output_tokens: Optional[int],
    ) -> Optional[float]:
        if not self.pricing_version or input_tokens is None or output_tokens is None:
            return None
        rate = self.rates.get(model)
        if rate is None:
            return None
        input_rate = _valid_rate(rate.get("input_usd_per_million_tokens"))
        output_rate = _valid_rate(rate.get("output_usd_per_million_tokens"))
        if input_rate is None or output_rate is None:
            return None
        return (input_tokens * input_rate + output_tokens * output_rate) / 1_000_000


def _field(source: Any, name: str) -> Any:
    if isinstance(source, Mapping):
        return source.get(name)
    try:
        return getattr(source, name, None)
    except Exception:  # AgentScope usage objects may raise KeyError for absent fields.
        return None


def _first_token_count(source: Any, *names: str) -> Optional[int]:
    for name in names:
        value = _field(source, name)
        if value is None or isinstance(value, bool):
            continue
        try:
            count = int(value)
        except (TypeError, ValueError, OverflowError):
            continue
        if count >= 0 and count == value:
            return count
    return None


def _valid_rate(value: Any) -> Optional[float]:
    try:
        rate = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return rate if math.isfinite(rate) and rate >= 0 else None
