"""用户数据访问层 — 封装 app_users 表的所有 SQL 操作"""
from __future__ import annotations

import json
from typing import Optional

from db import get_app_conn
from loguru import logger


def get_by_username(username: str) -> Optional[dict]:
    with get_app_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM app_users WHERE username = %s", (username,))
        row = cur.fetchone()
    if row and row.get("assigned_accounts"):
        if isinstance(row["assigned_accounts"], str):
            row["assigned_accounts"] = json.loads(row["assigned_accounts"])
    return row


def list_all() -> list[dict]:
    with get_app_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM app_users ORDER BY id")
        rows = cur.fetchall()
    for row in rows:
        if row.get("assigned_accounts") and isinstance(row["assigned_accounts"], str):
            row["assigned_accounts"] = json.loads(row["assigned_accounts"])
    return rows


def create(*, username: str, password_hash: str, role: str,
           display_name: str, assigned_accounts: list[str]) -> dict:
    with get_app_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO app_users (username, password_hash, role, display_name, assigned_accounts)
               VALUES (%s, %s, %s, %s, %s)""",
            (username, password_hash, role, display_name,
             json.dumps(assigned_accounts, ensure_ascii=False)),
        )
        conn.commit()
    return get_by_username(username)  # type: ignore


def update(username: str, **fields) -> Optional[dict]:
    if not fields:
        return get_by_username(username)
    set_parts = []
    values = []
    for k, v in fields.items():
        if k == "assigned_accounts" and isinstance(v, list):
            v = json.dumps(v, ensure_ascii=False)
        set_parts.append(f"{k} = %s")
        values.append(v)
    values.append(username)
    with get_app_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"UPDATE app_users SET {', '.join(set_parts)} WHERE username = %s",
            values,
        )
        conn.commit()
    return get_by_username(username)


def delete(username: str) -> bool:
    with get_app_conn() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM app_users WHERE username = %s", (username,))
        conn.commit()
        return cur.rowcount > 0


def count_by_role(role: str) -> int:
    with get_app_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS cnt FROM app_users WHERE role = %s", (role,))
        return cur.fetchone()["cnt"]
