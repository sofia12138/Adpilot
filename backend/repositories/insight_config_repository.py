"""Insight 配置数据访问层"""
from __future__ import annotations

import json
from typing import Optional

from db import get_app_conn


def get_config(config_key: str) -> Optional[dict]:
    with get_app_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT config_value FROM app_insight_config WHERE config_key = %s",
            (config_key,),
        )
        row = cur.fetchone()
    if not row:
        return None
    val = row["config_value"]
    if isinstance(val, str):
        return json.loads(val)
    return val


def upsert_config(config_key: str, config_value: dict):
    raw = json.dumps(config_value, ensure_ascii=False)
    with get_app_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id FROM app_insight_config WHERE config_key = %s",
            (config_key,),
        )
        if cur.fetchone():
            cur.execute(
                "UPDATE app_insight_config SET config_value = %s WHERE config_key = %s",
                (raw, config_key),
            )
        else:
            cur.execute(
                "INSERT INTO app_insight_config (config_key, config_value) VALUES (%s, %s)",
                (config_key, raw),
            )
        conn.commit()
