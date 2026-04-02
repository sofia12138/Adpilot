"""面板权限数据访问层"""
from __future__ import annotations

from db import get_app_conn


def list_panels(enabled_only: bool = True) -> list[dict]:
    with get_app_conn() as conn:
        cur = conn.cursor()
        sql = "SELECT * FROM panel_definitions"
        if enabled_only:
            sql += " WHERE is_enabled = 1"
        sql += " ORDER BY sort_order"
        cur.execute(sql)
        return cur.fetchall()


def get_role_panels(role_key: str) -> list[str]:
    with get_app_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT panel_key FROM role_panel_permissions WHERE role_key = %s AND can_view = 1",
            (role_key,),
        )
        return [r["panel_key"] for r in cur.fetchall()]


def set_role_panels(role_key: str, panel_keys: list[str]):
    with get_app_conn() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM role_panel_permissions WHERE role_key = %s", (role_key,))
        for pk in panel_keys:
            cur.execute(
                "INSERT INTO role_panel_permissions (role_key, panel_key, can_view) VALUES (%s, %s, 1)",
                (role_key, pk),
            )
        conn.commit()


def get_user_panels(username: str) -> list[dict]:
    """返回用户个性化权限列表 [{panel_key, can_view}, ...]"""
    with get_app_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT panel_key, can_view FROM user_panel_permissions WHERE username = %s",
            (username,),
        )
        return cur.fetchall()


def set_user_panels(username: str, panel_keys: list[str]):
    with get_app_conn() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM user_panel_permissions WHERE username = %s", (username,))
        for pk in panel_keys:
            cur.execute(
                "INSERT INTO user_panel_permissions (username, panel_key, can_view) VALUES (%s, %s, 1)",
                (username, pk),
            )
        conn.commit()


def resolve_user_allowed_panels(username: str, role: str) -> list[str]:
    """权限合并：用户个性化 > 角色默认。若用户有配置则以用户为准，否则回退角色默认。"""
    user_overrides = get_user_panels(username)
    if user_overrides:
        return [r["panel_key"] for r in user_overrides if r["can_view"]]
    return get_role_panels(role)
