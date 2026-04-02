"""同步日志数据访问层 — biz_sync_logs (adpilot_biz)"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from db import get_biz_conn


def create(*, task_name: str, platform: str = "",
           account_id: str = "", sync_date: str | None = None) -> int:
    """创建一条同步日志（status=running），返回 id"""
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO biz_sync_logs (task_name, platform, account_id, sync_date, status)
               VALUES (%s, %s, %s, %s, 'running')""",
            (task_name, platform, account_id, sync_date),
        )
        conn.commit()
        return cur.lastrowid


def finish(log_id: int, *, status: str = "success",
           message: str | None = None, rows_affected: int = 0):
    """标记同步完成/失败"""
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """UPDATE biz_sync_logs
               SET status = %s, message = %s, rows_affected = %s,
                   finished_at = %s
               WHERE id = %s""",
            (status, message, rows_affected,
             datetime.now().strftime("%Y-%m-%d %H:%M:%S"), log_id),
        )
        conn.commit()


def list_recent(task_name: str | None = None, limit: int = 50) -> list[dict]:
    with get_biz_conn() as conn:
        cur = conn.cursor()
        if task_name:
            cur.execute(
                "SELECT * FROM biz_sync_logs WHERE task_name = %s ORDER BY id DESC LIMIT %s",
                (task_name, limit),
            )
        else:
            cur.execute(
                "SELECT * FROM biz_sync_logs ORDER BY id DESC LIMIT %s",
                (limit,),
            )
        return cur.fetchall()
