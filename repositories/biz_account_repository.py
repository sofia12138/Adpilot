"""广告账户数据访问层 — biz_ad_accounts (adpilot_biz)"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Optional

from db import get_biz_conn


def upsert(*, platform: str, account_id: str, account_name: str = "",
           currency: str = "USD", timezone: str = "UTC",
           status: str = "ACTIVE", access_token: str | None = None,
           app_id: str = "", app_secret: str = "",
           is_default: int = 0, raw_json: dict | None = None) -> int:
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO biz_ad_accounts
               (platform, account_id, account_name, currency, timezone, status,
                access_token, app_id, app_secret, is_default, raw_json)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON DUPLICATE KEY UPDATE
                 account_name = VALUES(account_name),
                 currency     = VALUES(currency),
                 timezone     = VALUES(timezone),
                 status       = VALUES(status),
                 access_token = VALUES(access_token),
                 app_id       = VALUES(app_id),
                 app_secret   = VALUES(app_secret),
                 is_default   = VALUES(is_default),
                 raw_json     = VALUES(raw_json)""",
            (platform, account_id, account_name, currency, timezone, status,
             access_token, app_id, app_secret, is_default,
             json.dumps(raw_json, ensure_ascii=False) if raw_json else None),
        )
        conn.commit()
        return cur.lastrowid


def update_by_id(row_id: int, **fields) -> int:
    if not fields:
        return 0
    allowed = {"account_name", "currency", "timezone", "status",
               "access_token", "app_id", "app_secret", "is_default"}
    sets = []
    vals = []
    for k, v in fields.items():
        if k in allowed:
            sets.append(f"{k} = %s")
            vals.append(v)
    if not sets:
        return 0
    vals.append(row_id)
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"UPDATE biz_ad_accounts SET {', '.join(sets)} WHERE id = %s", vals)
        conn.commit()
        return cur.rowcount


def delete_by_id(row_id: int) -> int:
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM biz_ad_accounts WHERE id = %s", (row_id,))
        conn.commit()
        return cur.rowcount


def get_by_id(row_id: int) -> Optional[dict]:
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM biz_ad_accounts WHERE id = %s", (row_id,))
        return cur.fetchone()


def get_by_platform_account(platform: str, account_id: str) -> Optional[dict]:
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM biz_ad_accounts WHERE platform = %s AND account_id = %s",
            (platform, account_id),
        )
        return cur.fetchone()


def list_by_platform(platform: str) -> list[dict]:
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM biz_ad_accounts WHERE platform = %s ORDER BY is_default DESC, id",
            (platform,),
        )
        return cur.fetchall()


def list_all() -> list[dict]:
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM biz_ad_accounts ORDER BY platform, is_default DESC, id")
        return cur.fetchall()


def list_active() -> list[dict]:
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM biz_ad_accounts WHERE status = 'ACTIVE' "
            "ORDER BY platform, is_default DESC, id"
        )
        return cur.fetchall()


def set_default(platform: str, row_id: int) -> None:
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE biz_ad_accounts SET is_default = 0 WHERE platform = %s", (platform,))
        cur.execute("UPDATE biz_ad_accounts SET is_default = 1 WHERE id = %s AND platform = %s",
                    (row_id, platform))
        conn.commit()


def update_last_synced(row_id: int) -> None:
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE biz_ad_accounts SET last_synced_at = %s WHERE id = %s",
                    (datetime.now(), row_id))
        conn.commit()
