# -*- coding: utf-8 -*-
"""presentation DTO

REST 请求 / 响应模型。shopping_session_id 缺省时由服务端生成。
"""
from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class SubmitIntentRequest(BaseModel):
    shopping_session_id: Optional[str] = Field(default=None, max_length=200, description="会话 ID，缺省则新建会话")
    buyer_id: Optional[str] = Field(default=None, min_length=1, description="兼容字段；必须与已认证 subject 相同")
    locale: str = Field(default="zh-CN")
    currency: str = Field(default="CNY")
    raw_query: str = Field(min_length=1, max_length=20000, description="买家自然语言购物意图")
    approval_id: Optional[str] = Field(default=None, pattern=r"^[0-9a-f]{32}$")


class SubmitIntentResponse(BaseModel):
    shopping_session_id: str
    final_text: str


class CancelOrderRequest(BaseModel):
    reason: str = Field(min_length=1, description="取消原因")
    shopping_session_id: str = Field(min_length=1, max_length=200)
    approval_id: Optional[str] = Field(default=None, pattern=r"^[0-9a-f]{32}$")
