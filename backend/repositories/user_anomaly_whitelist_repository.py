"""用户付费面板 · 异常用户白名单数据访问层

只能由审批通过的工单（biz_user_anomaly_application.status='approved'）写入。
"""
from __future__ import annotations

from typing import Optional

from db import get_biz_conn


def list_whitelist() -> list[dict]:
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT user_id, tag, reason, marked_by, marked_at, application_id
            FROM biz_user_anomaly_whitelist
            ORDER BY marked_at DESC
            """,
        )
        return list(cur.fetchall())


def list_whitelisted_user_ids(tag: Optional[str] = None) -> list[int]:
    """仅返回 user_id 数组，供 KPI 双口径剔除使用。"""
    sql = "SELECT user_id FROM biz_user_anomaly_whitelist"
    params: tuple = ()
    if tag:
        sql += " WHERE tag = %s"
        params = (tag,)
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        return [int(r["user_id"]) for r in cur.fetchall()]


def get_by_user(user_id: int) -> Optional[dict]:
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT user_id, tag, reason, marked_by, marked_at, application_id
            FROM biz_user_anomaly_whitelist WHERE user_id = %s
            """,
            (user_id,),
        )
        return cur.fetchone()


def upsert(
    *,
    user_id: int,
    tag: str,
    reason: str,
    marked_by: str,
    application_id: Optional[int] = None,
) -> int:
    """加入白名单。已存在则覆盖（保留最新审批信息）。"""
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO biz_user_anomaly_whitelist
                (user_id, tag, reason, marked_by, marked_at, application_id)
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, %s)
            ON DUPLICATE KEY UPDATE
                tag            = VALUES(tag),
                reason         = VALUES(reason),
                marked_by      = VALUES(marked_by),
                marked_at      = CURRENT_TIMESTAMP,
                application_id = VALUES(application_id)
            """,
            (user_id, tag, reason, marked_by, application_id),
        )
        conn.commit()
        return cur.rowcount


def delete(user_id: int) -> int:
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM biz_user_anomaly_whitelist WHERE user_id = %s",
            (user_id,),
        )
        conn.commit()
        return cur.rowcount
