"""操作日志业务逻辑层 — 统一日志写入入口 + 兼容旧字段"""
from __future__ import annotations

from loguru import logger as app_logger
from repositories import oplog_repository


def log_operation(
    *,
    username: str,
    action: str,
    target_type: str = "",
    target_id: str = "",
    platform: str = "",
    user_id: int | None = None,
    before_data: dict | None = None,
    after_data: dict | None = None,
    status: str = "success",
    error_message: str | None = None,
):
    """统一操作日志写入入口，所有写操作日志均应调用此方法"""
    try:
        oplog_repository.insert(
            username=username,
            action=action,
            target_type=target_type,
            target_id=target_id,
            platform=platform,
            user_id=user_id,
            before_data=before_data,
            after_data=after_data,
            status=status,
            error_message=error_message,
        )
    except Exception as e:
        app_logger.error(f"写操作日志失败: {e}")


def list_logs(page: int = 1, page_size: int = 30) -> tuple[list[dict], int]:
    """查询日志并映射为兼容旧前端的格式"""
    rows, total = oplog_repository.list_logs(page=page, page_size=page_size)
    compatible = []
    for row in rows:
        target_parts = []
        if row.get("platform"):
            target_parts.append(row["platform"].capitalize())
        if row.get("target_type"):
            type_map = {"campaign": "广告系列", "adgroup": "广告组", "ad": "广告"}
            target_parts.append(type_map.get(row["target_type"], row["target_type"]))
        if row.get("target_id"):
            target_parts.append(row["target_id"])
        target_str = " ".join(target_parts)

        created_at = row.get("created_at")
        if hasattr(created_at, "strftime"):
            time_str = created_at.strftime("%Y-%m-%d %H:%M:%S")
        else:
            time_str = str(created_at) if created_at else ""

        compatible.append({
            "time": time_str,
            "user": row.get("username", ""),
            "action": row.get("action", ""),
            "target": target_str,
            "detail": row.get("error_message") or "",
            "id": row.get("id"),
            "platform": row.get("platform", ""),
            "status": row.get("status", "success"),
        })
    return compatible, total
