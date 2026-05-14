"""用户付费面板 · 白名单申请工单数据访问层

状态机：pending → approved / rejected / withdrawn（终态）
- pending：新建后默认状态
- approved：审批人通过，service 层会同步写入 biz_user_anomaly_whitelist
- rejected：审批人拒绝
- withdrawn：申请人主动撤回（仅限 pending 状态）
"""
from __future__ import annotations

from typing import Any, Optional

from db import get_biz_conn

VALID_STATUS = ("pending", "approved", "rejected", "withdrawn")
VALID_ACTION = ("add", "remove")
VALID_TAG = ("whitelist", "blacklist", "internal_test")

_SELECT_COLUMNS = (
    "id", "target_user_id", "requested_tag", "action", "reason", "status",
    "applicant_user", "applied_at",
    "reviewer_user", "review_note", "reviewed_at",
    "created_at", "updated_at",
)


def create(
    *,
    target_user_id: int,
    requested_tag: str,
    action: str,
    reason: str,
    applicant_user: str,
) -> int:
    if requested_tag not in VALID_TAG:
        raise ValueError(f"requested_tag must be one of {VALID_TAG}")
    if action not in VALID_ACTION:
        raise ValueError(f"action must be one of {VALID_ACTION}")
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO biz_user_anomaly_application
                (target_user_id, requested_tag, action, reason, status, applicant_user, applied_at)
            VALUES (%s, %s, %s, %s, 'pending', %s, CURRENT_TIMESTAMP)
            """,
            (target_user_id, requested_tag, action, reason, applicant_user),
        )
        conn.commit()
        return int(cur.lastrowid)


def get(application_id: int) -> Optional[dict]:
    cols = ", ".join(_SELECT_COLUMNS)
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"SELECT {cols} FROM biz_user_anomaly_application WHERE id = %s",
            (application_id,),
        )
        return cur.fetchone()


def list_by(
    *,
    status: Optional[str] = None,
    target_user_id: Optional[int] = None,
    applicant_user: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
) -> list[dict]:
    where = ["1=1"]
    params: list[Any] = []
    if status:
        if status not in VALID_STATUS:
            raise ValueError(f"status must be one of {VALID_STATUS}")
        where.append("status = %s")
        params.append(status)
    if target_user_id is not None:
        where.append("target_user_id = %s")
        params.append(target_user_id)
    if applicant_user:
        where.append("applicant_user = %s")
        params.append(applicant_user)

    cols = ", ".join(_SELECT_COLUMNS)
    sql = (
        f"SELECT {cols} FROM biz_user_anomaly_application "
        f"WHERE {' AND '.join(where)} "
        f"ORDER BY applied_at DESC LIMIT %s OFFSET %s"
    )
    params.extend([limit, offset])
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, tuple(params))
        return list(cur.fetchall())


def count_pending() -> int:
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM biz_user_anomaly_application WHERE status = 'pending'"
        )
        row = cur.fetchone() or {}
        return int(row.get("cnt", 0))


def list_pending_target_user_ids() -> list[int]:
    """所有 pending 工单的 target_user_id 列表（用于在聚合表上挂 pending_whitelist 标签）。"""
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT target_user_id FROM biz_user_anomaly_application WHERE status = 'pending'"
        )
        return [int(r["target_user_id"]) for r in cur.fetchall()]


def has_pending_for_user(target_user_id: int) -> bool:
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM biz_user_anomaly_application "
            "WHERE target_user_id = %s AND status = 'pending' LIMIT 1",
            (target_user_id,),
        )
        return cur.fetchone() is not None


def update_status(
    *,
    application_id: int,
    new_status: str,
    reviewer_user: Optional[str] = None,
    review_note: str = "",
) -> int:
    if new_status not in VALID_STATUS:
        raise ValueError(f"status must be one of {VALID_STATUS}")
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE biz_user_anomaly_application
            SET status = %s,
                reviewer_user = %s,
                review_note = %s,
                reviewed_at = CASE WHEN %s IN ('approved','rejected') THEN CURRENT_TIMESTAMP
                                   ELSE reviewed_at END
            WHERE id = %s AND status = 'pending'
            """,
            (new_status, reviewer_user, review_note, new_status, application_id),
        )
        conn.commit()
        return cur.rowcount
