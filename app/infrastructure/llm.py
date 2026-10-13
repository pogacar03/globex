# -*- coding: utf-8 -*-
"""llm

统一创建 AgentScope 2.0 大模型对象。全项目只从这里拿 model，
主 / 子 Agent 各自持有独立实例。

2.0 的模型接入方式：OpenAICredential（携带 api_key + base_url，天然支持
OpenAI 兼容网关）→ OpenAIChatModel(credential=..., model=...)。

四期在此加两层框架不覆盖的东西：
    1. 配额闸门：闸门必须持有到「流耗尽」。流式调用返回的是异步生成器，
       若在 `async with slot()` 内直接 return，名额会在数据还没读完时释放，
       限流等于没做；
    2. 限流回退：实测网关配额池紧张时主模型单发也会 429，退避重试用尽后换用
       备用模型，并发 model.fallback 事件如实告知，不静默降级。
"""
from __future__ import annotations

import asyncio
import logging
import math
import time
from typing import Any, AsyncGenerator, Optional

from agentscope.credential import OpenAICredential
from agentscope.message import Msg, TextBlock
from agentscope.model import ChatResponse, OpenAIChatModel
from agentscope.tool import ToolChoice

from app.infrastructure.budget import current_tier, get_budget, minimal_mode_hint
from app.infrastructure.context import ShoppingContext
from app.infrastructure.eventbus import TradeEventBus
from app.infrastructure.model_usage import ModelUsageLedger
from app.infrastructure.settings import Settings
from app.infrastructure.task_routing import can_use_small_model, current_routing_decision
from app.infrastructure.throttle import GatewayThrottle
from app.infrastructure.transient import is_transient_error

logger = logging.getLogger(__name__)


class ThrottledChatModel(OpenAIChatModel):
    """OpenAI-compatible gateway with bounded calls, retries, and safe fallback."""

    def __init__(
        self,
        *,
        throttle: GatewayThrottle,
        fallback: Optional[OpenAIChatModel] = None,
        small_model: Optional[OpenAIChatModel] = None,
        max_transient_retries: int = 2,
        retry_base_seconds: float = 6.0,
        request_timeout_seconds: float = 30.0,
        total_timeout_seconds: float = 90.0,
        usage_recorder: Optional[ModelUsageLedger] = None,
        bus: Optional[TradeEventBus] = None,
        **kwargs: Any,
    ) -> None:
        # Retry policy belongs to this gateway so retries, deadlines, and model
        # promotion share one bounded budget instead of nesting SDK retries.
        kwargs["max_retries"] = 0
        super().__init__(**kwargs)
        if not math.isfinite(request_timeout_seconds) or request_timeout_seconds <= 0:
            raise ValueError("request_timeout_seconds must be a finite positive number")
        if not math.isfinite(total_timeout_seconds) or total_timeout_seconds <= 0:
            raise ValueError("total_timeout_seconds must be a finite positive number")
        self._throttle = throttle
        self._fallback = fallback
        self._small_model = small_model
        self._max_transient_retries = max_transient_retries
        self._retry_base_seconds = retry_base_seconds
        self._request_timeout_seconds = request_timeout_seconds
        self._total_timeout_seconds = total_timeout_seconds
        self._usage_recorder = usage_recorder if usage_recorder is not None else ModelUsageLedger()
        self.usage_ledger = self._usage_recorder
        self._bus = bus

    async def __call__(  # type: ignore[override]
        self,
        messages: list[Msg],
        tools: Optional[list[dict]] = None,
        tool_choice: Optional[ToolChoice] = None,
        **kwargs: Any,
    ) -> ChatResponse | AsyncGenerator[ChatResponse, None]:
        total_deadline = time.monotonic() + self._total_timeout_seconds
        result, slot, model_name, request_deadline, retries_left = await self._call_with_fallback(
            messages,
            tools,
            tool_choice,
            total_deadline=total_deadline,
            retries=self._max_transient_retries,
            **kwargs,
        )
        if not hasattr(result, "__aiter__"):
            try:
                await slot.__aexit__(None, None, None)
            finally:
                _charge_budget(result)
                self._record_usage(result, model_name)
            return result
        return self._release_after_stream(
            slot,
            result,
            model_name,
            request_deadline,
            total_deadline,
            retries_left,
            messages,
            tools,
            tool_choice,
            kwargs,
        )

    async def _release_after_stream(
        self,
        slot: Any,
        stream: Any,
        model_name: str,
        request_deadline: float,
        total_deadline: float,
        retries_left: int,
        messages: list[Msg],
        tools: Optional[list[dict]],
        tool_choice: Optional[ToolChoice],
        kwargs: dict[str, Any],
    ) -> AsyncGenerator[ChatResponse, None]:
        """Hold a slot through streaming, with retries only before output starts."""
        active_slot: Any = slot
        active_stream: Any = stream
        active_model = model_name
        active_request_deadline = request_deadline
        active_retries_left = retries_left
        last: Any = None
        attempt_recorded = False
        try:
            while True:
                emitted = False
                try:
                    async for chunk in self._timed_stream(
                        active_stream,
                        active_request_deadline,
                        total_deadline,
                    ):
                        last = chunk
                        emitted = True
                        yield chunk
                    self._charge_and_record(last, active_model)
                    attempt_recorded = True
                    return
                except BaseException as err:
                    await self._close_stream(active_stream)
                    if not attempt_recorded:
                        self._charge_and_record(last, active_model)
                        attempt_recorded = True
                    await active_slot.__aexit__(None, None, None)
                    active_slot = None

                    # Once any content reached the caller, retrying could splice
                    # answers from two models into one response.
                    if not isinstance(err, Exception) or emitted or _is_non_retryable_http_error(err):
                        raise
                    if not _is_retryable_gateway_error(err):
                        raise

                    if active_retries_left > 0:
                        result, active_slot, active_model, active_request_deadline, active_retries_left = (
                            await self._call_with_fallback(
                                messages,
                                tools,
                                tool_choice,
                                total_deadline=total_deadline,
                                retries=active_retries_left - 1,
                                **kwargs,
                            )
                        )
                    elif self._configured_route()[0] == "task_small":
                        result, active_slot, active_model, active_request_deadline, active_retries_left = (
                            await self._promote_small_to_main(
                                messages,
                                tools,
                                tool_choice,
                                total_deadline=total_deadline,
                                error=err,
                                **kwargs,
                            )
                        )
                    else:
                        raise
                    attempt_recorded = False
                    last = None
                    if not hasattr(result, "__aiter__"):
                        await active_slot.__aexit__(None, None, None)
                        active_slot = None
                        self._charge_and_record(result, active_model)
                        attempt_recorded = True
                        yield result
                        return
                    active_stream = result
        finally:
            if not attempt_recorded:
                self._charge_and_record(last, active_model)
            if active_stream is not None:
                await self._close_stream(active_stream)
            if active_slot is not None:
                await active_slot.__aexit__(None, None, None)

    async def _timed_stream(
        self,
        stream: Any,
        request_deadline: float,
        total_deadline: float,
    ) -> AsyncGenerator[ChatResponse, None]:
        iterator = stream.__aiter__()
        while True:
            try:
                chunk = await _bounded(
                    iterator.__anext__(),
                    request_deadline=request_deadline,
                    total_deadline=total_deadline,
                )
            except StopAsyncIteration:
                return
            yield chunk

    @staticmethod
    async def _close_stream(stream: Any) -> None:
        close = getattr(stream, "aclose", None)
        if close is None:
            return
        try:
            await close()
        except Exception:  # Cleanup must not hide the upstream error.
            pass

    def _charge_and_record(self, response: Any, model_name: str) -> None:
        _charge_budget(response)
        self._record_usage(response, model_name)

    def _record_usage(self, response: Any, model_name: str) -> None:
        if self._usage_recorder is None:
            return
        try:
            record = self._usage_recorder.record(response, model=model_name)
            logger.info(
                "model.usage model=%s usage_status=%s input_tokens=%s output_tokens=%s "
                "total_tokens=%s estimated_cost_usd=%s pricing_version=%s billing_status=unavailable",
                record.model,
                record.usage_status,
                record.input_tokens,
                record.output_tokens,
                record.total_tokens,
                record.estimated_cost_usd,
                record.pricing_version,
            )
        except Exception as err:  # Usage recording must never break an intent.
            logger.debug("模型 usage 记录跳过（不影响主链路）：%s", err)

    async def _invoke_upstream(
        self,
        messages: list[Msg],
        tools: Optional[list[dict]],
        tool_choice: Optional[ToolChoice],
        **kwargs: Any,
    ) -> ChatResponse | AsyncGenerator[ChatResponse, None]:
        """真正打上游的一跳。抽成方法便于替换与测试。

        同时是四档预算降级（16-4 章）的作用点：
        预算充足（main）走主模型；剩余不足时切到更便宜的备用模型，
        并发 model.fallback 事件如实告知——**降级不静默**。
        minimal 档额外注入简洁模式提示，压住 Think 长度。
        """
        tier = current_tier()
        if tier != "main":
            hint = minimal_mode_hint()
            if hint:
                messages = [
                    *messages,
                    Msg(name="system", content=[TextBlock(text=hint)], role="system"),
                ]
            if self._fallback is not None:
                logger.info("Token 预算档位 %s，切用备用模型 %s", tier, self._fallback.model)
                self._publish_budget_tier(tier)
                return await self._fallback(messages, tools, tool_choice, **kwargs)
            return await self._invoke_primary(messages, tools, tool_choice, **kwargs)
        decision = current_routing_decision()
        if self._small_model is not None and can_use_small_model(decision):
            logger.info(
                "任务路由策略=%s 原因=%s 置信度=%.2f，使用轻量模型 %s",
                decision.policy_version,
                decision.reason,
                decision.confidence,
                self._small_model.model,
            )
            return await self._small_model(messages, tools, tool_choice, **kwargs)
        return await self._invoke_primary(messages, tools, tool_choice, **kwargs)

    async def _invoke_primary(
        self,
        messages: list[Msg],
        tools: Optional[list[dict]],
        tool_choice: Optional[ToolChoice],
        **kwargs: Any,
    ) -> ChatResponse | AsyncGenerator[ChatResponse, None]:
        """Call the configured primary model without reapplying task routing."""
        return await super().__call__(messages, tools, tool_choice, **kwargs)

    def _publish_budget_tier(self, tier: str) -> None:
        if self._bus is None or self._fallback is None:
            return
        self._bus.publish(
            ShoppingContext.current_session_id(),
            "model.fallback",
            {
                "from": self.model,
                "to": self._fallback.model,
                "reason": f"Token 预算档位 {tier}",
                "budget_tier": tier,
            },
        )

    async def _call_with_fallback(
        self,
        messages: list[Msg],
        tools: Optional[list[dict]],
        tool_choice: Optional[ToolChoice],
        *,
        total_deadline: float,
        retries: int,
        **kwargs: Any,
    ) -> tuple[Any, Any, str, float, int]:
        retries_used = 0
        route_kind, route_model = self._configured_route()
        last_error: Optional[Exception] = None
        while True:
            request_deadline = time.monotonic() + self._request_timeout_seconds
            try:
                response, slot = await self._start_attempt(
                    lambda: self._invoke_upstream(messages, tools, tool_choice, **kwargs),
                    request_deadline,
                    total_deadline,
                )
                return response, slot, route_model, request_deadline, retries - retries_used
            except asyncio.CancelledError:
                self._record_usage(None, route_model)
                raise
            except Exception as err:
                self._record_usage(None, route_model)
                if not _is_retryable_gateway_error(err):
                    raise
                last_error = err
                if retries_used < retries:
                    # 指数退避：网关速率类限流对固定间隔重试不敏感
                    delay = self._retry_base_seconds * (3**retries_used)
                    retries_used += 1
                    logger.warning(
                        "模型 %s 遇上游瞬时故障，%.0fs 后重试（第 %d/%d 次）：%s",
                        route_model, delay, retries_used, retries, err,
                    )
                    await _bounded(asyncio.sleep(delay), total_deadline=total_deadline)
                    continue
                break

        if route_kind == "task_small":
            return await self._promote_small_to_main(
                messages,
                tools,
                tool_choice,
                total_deadline=total_deadline,
                error=last_error,
                **kwargs,
            )

        # Budget-routed requests already used the configured budget model. Do not
        # silently loop back to the primary or another fallback after it fails.
        if route_kind != "main" or self._fallback is None:
            raise last_error  # type: ignore[misc]

        logger.warning("模型 %s 重试用尽，回退到 %s：%s", self.model, self._fallback.model, last_error)
        self._publish_fallback(str(last_error))
        request_deadline = time.monotonic() + self._request_timeout_seconds
        response, slot = await self._start_attempt(
            lambda: self._fallback(messages, tools, tool_choice, **kwargs),
            request_deadline,
            total_deadline,
        )
        return response, slot, str(self._fallback.model), request_deadline, 0

    async def _promote_small_to_main(
        self,
        messages: list[Msg],
        tools: Optional[list[dict]],
        tool_choice: Optional[ToolChoice],
        *,
        total_deadline: float,
        error: BaseException,
        **kwargs: Any,
    ) -> tuple[Any, Any, str, float, int]:
        small_name = str(self._small_model.model) if self._small_model is not None else "small"
        logger.warning("轻量模型 %s 瞬时失败，在总 deadline 内升级到主模型：%s", small_name, error)
        self._publish_model_fallback(small_name, str(self.model), str(error))
        request_deadline = time.monotonic() + self._request_timeout_seconds
        response, slot = await self._start_attempt(
            lambda: self._invoke_primary(messages, tools, tool_choice, **kwargs),
            request_deadline,
            total_deadline,
        )
        return response, slot, str(self.model), request_deadline, 0

    def _configured_route(self) -> tuple[str, str]:
        if current_tier() != "main":
            if self._fallback is not None:
                return "budget", str(self._fallback.model)
            return "main", str(self.model)
        decision = current_routing_decision()
        if self._small_model is not None and can_use_small_model(decision):
            return "task_small", str(self._small_model.model)
        return "main", str(self.model)

    def _budget_route_model(self) -> str:
        """Compatibility helper returning the model selected for this context."""
        return self._configured_route()[1]

    async def _start_attempt(
        self,
        call: Any,
        request_deadline: float,
        total_deadline: float,
    ) -> tuple[Any, Any]:
        slot = self._throttle.slot()
        entered = False
        try:
            await _bounded(
                slot.__aenter__(),
                request_deadline=request_deadline,
                total_deadline=total_deadline,
            )
            entered = True
            response = await _bounded(
                call(),
                request_deadline=request_deadline,
                total_deadline=total_deadline,
            )
            return response, slot
        except BaseException as err:
            if entered:
                await slot.__aexit__(type(err), err, err.__traceback__)
            raise

    def _publish_fallback(self, reason: str) -> None:
        if self._bus is None or self._fallback is None:
            return
        self._publish_model_fallback(str(self.model), str(self._fallback.model), reason)

    def _publish_model_fallback(self, from_model: str, to_model: str, reason: str) -> None:
        if self._bus is None:
            return
        session_id = ShoppingContext.current_session_id()
        self._bus.publish(
            session_id,
            "model.fallback",
            {"from": from_model, "to": to_model, "reason": reason},
        )


def _safe_field(source: Any, name: str) -> Any:
    """宽容取字段。

    坑：ChatResponse.usage 的 `__getattr__` 对缺失字段抛 **KeyError**，
    而 `getattr(obj, name, default)` 只吃 AttributeError——直接用 getattr 带默认值
    依旧会把 KeyError 抛到主链路，把整轮对话搞成 [error]。实测踩过。
    """
    if isinstance(source, dict):
        return source.get(name)
    try:
        return getattr(source, name, None)
    except Exception:  # noqa: BLE001 —— 包含 KeyError 等非标准实现
        return None


async def _bounded(
    awaitable: Any,
    *,
    total_deadline: float,
    request_deadline: Optional[float] = None,
) -> Any:
    deadlines = [total_deadline]
    if request_deadline is not None:
        deadlines.append(request_deadline)
    remaining = min(deadlines) - time.monotonic()
    if remaining <= 0:
        close = getattr(awaitable, "close", None)
        if close is not None:
            close()
        raise TimeoutError("model request deadline exceeded")
    try:
        async with asyncio.timeout(remaining):
            return await awaitable
    except TimeoutError:
        raise TimeoutError("model request deadline exceeded") from None


def _is_non_retryable_http_error(error: BaseException) -> bool:
    status = _http_status_code(error)
    if status is None:
        return False
    return 400 <= status < 500 and status not in {408, 425, 429}


def _http_status_code(error: BaseException) -> Optional[int]:
    status = _safe_field(error, "status_code")
    if status is None:
        response = _safe_field(error, "response")
        status = _safe_field(response, "status_code") if response is not None else None
    try:
        return int(status)
    except (TypeError, ValueError, OverflowError):
        return None


def _is_retryable_gateway_error(error: BaseException) -> bool:
    if _is_non_retryable_http_error(error):
        return False
    status = _http_status_code(error)
    if status in {408, 425, 429} or (status is not None and 500 <= status < 600):
        return True
    return isinstance(error, TimeoutError) or is_transient_error(error)


def _charge_budget(response: Any) -> None:
    """把一次模型调用的 token 记进当前意图的预算账本。

    未启用预算（TOKEN_BUDGET_TOTAL=0）时直接返回，零开销。
    usage 字段各网关存在差异，取不到就不计——**记账失败绝不能影响主链路**。
    """
    try:
        budget = get_budget()
        if budget is None or response is None:
            return
        usage = _safe_field(response, "usage")
        if usage is None:
            return
        total = _safe_field(usage, "total_tokens")
        if total is None:
            prompt = _safe_field(usage, "input_tokens") or _safe_field(usage, "prompt_tokens") or 0
            completion = (
                _safe_field(usage, "output_tokens")
                or _safe_field(usage, "completion_tokens")
                or 0
            )
            total = int(prompt) + int(completion)
        budget.charge("llm", int(total))
    except Exception as err:  # noqa: BLE001
        logger.debug("Token 记账跳过（不影响主链路）：%s", err)


def create_chat_model(
    settings: Settings,
    stream: bool = True,
    throttle: Optional[GatewayThrottle] = None,
    bus: Optional[TradeEventBus] = None,
    usage_recorder: Optional[ModelUsageLedger] = None,
) -> Any:
    if getattr(settings, "llm_mode", "live") == "mock":
        from app.infrastructure.synthetic_model import SyntheticChatModel

        return SyntheticChatModel(context_size=settings.context_size,
            delay_seconds=getattr(settings, "mock_delay_seconds", 0.0))

    pricing_version = getattr(settings, "llm_pricing_version", "")
    pricing_rates = getattr(settings, "llm_pricing_rates", {})
    if usage_recorder is None:
        usage_recorder = ModelUsageLedger(
            pricing_version=pricing_version,
            rates=pricing_rates,
        )
    credential = OpenAICredential(
        api_key=settings.llm_api_key,
        base_url=settings.llm_base_url,
    )
    common = {
        "credential": credential,
        "stream": stream,
        # All retries are governed by ThrottledChatModel's request/total deadlines.
        "max_retries": 0,
        # 上下文窗口：压缩触发阈值（ContextConfig.trigger_ratio）按此值比例计算
        "context_size": settings.context_size,
    }
    fallback = (
        OpenAIChatModel(model=settings.llm_fallback_model, **common)
        if settings.llm_fallback_model and settings.llm_fallback_model != settings.llm_model
        else None
    )
    small_model_name = getattr(settings, "llm_small_model", "")
    small_model = (
        OpenAIChatModel(model=small_model_name, **common)
        if small_model_name and small_model_name != settings.llm_model
        else None
    )
    return ThrottledChatModel(
        model=settings.llm_model,
        throttle=throttle or GatewayThrottle(settings.llm_max_concurrency, settings.llm_min_interval_seconds),
        fallback=fallback,
        small_model=small_model,
        max_transient_retries=settings.llm_max_retries,
        request_timeout_seconds=getattr(settings, "llm_request_timeout_seconds", 30.0),
        total_timeout_seconds=getattr(settings, "llm_total_timeout_seconds", 90.0),
        usage_recorder=usage_recorder,
        bus=bus,
        **common,
    )
