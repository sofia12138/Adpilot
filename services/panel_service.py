"""面板权限业务逻辑层"""
from __future__ import annotations

from repositories import panel_repository
from repositories import user_repository


def list_panels(enabled_only: bool = True) -> list[dict]:
    return panel_repository.list_panels(enabled_only)


def get_role_panels(role_key: str) -> list[str]:
    return panel_repository.get_role_panels(role_key)


def set_role_panels(role_key: str, panel_keys: list[str]):
    all_keys = {p["panel_key"] for p in panel_repository.list_panels(enabled_only=False)}
    invalid = set(panel_keys) - all_keys
    if invalid:
        raise ValueError(f"无效的面板key: {', '.join(invalid)}")
    panel_repository.set_role_panels(role_key, panel_keys)


def get_user_allowed_panels(username: str) -> dict:
    user = user_repository.get_by_username(username)
    if not user:
        raise KeyError(f"用户 {username} 不存在")
    role = user.get("role", "viewer")
    allowed = panel_repository.resolve_user_allowed_panels(username, role)
    has_override = bool(panel_repository.get_user_panels(username))
    return {
        "username": username,
        "role": role,
        "has_override": has_override,
        "allowed_panels": allowed,
    }


def set_user_panels(username: str, panel_keys: list[str]):
    user = user_repository.get_by_username(username)
    if not user:
        raise KeyError(f"用户 {username} 不存在")
    all_keys = {p["panel_key"] for p in panel_repository.list_panels(enabled_only=False)}
    invalid = set(panel_keys) - all_keys
    if invalid:
        raise ValueError(f"无效的面板key: {', '.join(invalid)}")
    panel_repository.set_user_panels(username, panel_keys)


def resolve_allowed_panels(username: str, role: str) -> list[str]:
    return panel_repository.resolve_user_allowed_panels(username, role)
