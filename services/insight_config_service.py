"""Insight 配置业务逻辑层"""
from __future__ import annotations

from repositories import insight_config_repository

_DEFAULT_ROI = {"min": 0.1, "low": 0.8, "target": 1.2, "high": 2.0}


def get_roi_thresholds() -> dict:
    val = insight_config_repository.get_config("roi_thresholds")
    if not val:
        return dict(_DEFAULT_ROI)
    for k in ("min", "low", "target", "high"):
        if k not in val:
            val[k] = _DEFAULT_ROI[k]
    return val


def get_insight_config() -> dict:
    return {"roi": get_roi_thresholds()}


def update_insight_config(data: dict):
    roi = data.get("roi")
    if not roi:
        raise ValueError("缺少 roi 配置")
    for k in ("min", "low", "target", "high"):
        if k not in roi:
            raise ValueError(f"缺少字段: roi.{k}")
        v = roi[k]
        if not isinstance(v, (int, float)):
            raise ValueError(f"roi.{k} 必须是数字")
        if v < 0:
            raise ValueError(f"roi.{k} 不能为负数")
    if roi["high"] > 10:
        raise ValueError("roi.high 不能超过 10")
    if not (roi["min"] <= roi["low"] <= roi["target"] <= roi["high"]):
        raise ValueError("必须满足 min <= low <= target <= high")
    insight_config_repository.upsert_config("roi_thresholds", roi)
