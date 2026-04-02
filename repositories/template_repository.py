"""模板数据访问层 — 封装 app_templates 表的所有 SQL 操作"""
from __future__ import annotations

import json
from typing import Optional

from db import get_app_conn


def _row_to_dict(row: dict) -> dict:
    """将 DB 行转换为与旧 JSON 兼容的模板字典"""
    content = row.get("content", {})
    if isinstance(content, str):
        content = json.loads(content)
    result = {
        "id": row["tpl_id"],
        "name": row["name"],
        "platform": row["platform"],
        "created_at": row["created_at"].isoformat() if row.get("created_at") else "",
        "updated_at": row["updated_at"].isoformat() if row.get("updated_at") else "",
    }
    result.update(content)
    return result


def list_all() -> list[dict]:
    with get_app_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM app_templates ORDER BY id")
        return [_row_to_dict(r) for r in cur.fetchall()]


def get_by_tpl_id(tpl_id: str) -> Optional[dict]:
    with get_app_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM app_templates WHERE tpl_id = %s", (tpl_id,))
        row = cur.fetchone()
    return _row_to_dict(row) if row else None


def create(*, tpl_id: str, name: str, platform: str,
           content: dict, is_builtin: bool = False,
           created_by: str = "", created_at: str | None = None) -> dict:
    with get_app_conn() as conn:
        cur = conn.cursor()
        sql = """INSERT INTO app_templates (tpl_id, name, platform, is_builtin, content, created_by, created_at)
                 VALUES (%s, %s, %s, %s, %s, %s, %s)"""
        cur.execute(sql, (
            tpl_id, name, platform, int(is_builtin),
            json.dumps(content, ensure_ascii=False),
            created_by,
            created_at or None,
        ))
        conn.commit()
    return get_by_tpl_id(tpl_id)  # type: ignore


def update(tpl_id: str, *, name: str | None = None,
           platform: str | None = None, content: dict | None = None) -> Optional[dict]:
    set_parts = []
    values = []
    if name is not None:
        set_parts.append("name = %s")
        values.append(name)
    if platform is not None:
        set_parts.append("platform = %s")
        values.append(platform)
    if content is not None:
        set_parts.append("content = %s")
        values.append(json.dumps(content, ensure_ascii=False))
    if not set_parts:
        return get_by_tpl_id(tpl_id)
    values.append(tpl_id)
    with get_app_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"UPDATE app_templates SET {', '.join(set_parts)} WHERE tpl_id = %s",
            values,
        )
        conn.commit()
    return get_by_tpl_id(tpl_id)


def delete(tpl_id: str) -> bool:
    with get_app_conn() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM app_templates WHERE tpl_id = %s", (tpl_id,))
        conn.commit()
        return cur.rowcount > 0
