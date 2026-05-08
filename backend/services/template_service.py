"""模板业务逻辑层 — DB 作为唯一数据源"""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Optional

from repositories import template_repository
from services.delivery_language import normalize_template_languages

# 行级保留字段（不应进入 content JSON）
_RESERVED_KEYS = {
    "id", "name", "platform", "is_builtin", "is_system",
    "is_editable", "template_key", "parent_template_id",
    "created_at", "updated_at",
}


def list_templates() -> list[dict]:
    return template_repository.list_all()


def get_template(tpl_id: str) -> Optional[dict]:
    return template_repository.get_by_tpl_id(tpl_id)


def get_template_by_key(template_key: str) -> Optional[dict]:
    return template_repository.get_by_key(template_key)


def create_template(data: dict, created_by: str = "") -> dict:
    tpl_id = f"tpl_{uuid.uuid4().hex[:12]}"
    name = data.pop("name", "")
    platform = data.pop("platform", "tiktok")
    parent_template_id = data.pop("parent_template_id", None) or None
    # 用户手动创建的模板永远不是系统母版
    data.pop("is_system", None)
    data.pop("is_editable", None)
    data.pop("template_key", None)
    data.pop("is_builtin", None)
    created_at = datetime.now().isoformat()
    content = {k: v for k, v in data.items() if k not in _RESERVED_KEYS}
    normalize_template_languages(content)
    return template_repository.create(
        tpl_id=tpl_id,
        name=name,
        platform=platform,
        content=content,
        is_builtin=False,
        is_system=False,
        is_editable=True,
        parent_template_id=parent_template_id,
        created_by=created_by,
        created_at=created_at,
    )


def update_template(tpl_id: str, data: dict) -> Optional[dict]:
    existing = template_repository.get_by_tpl_id(tpl_id)
    if not existing:
        return None
    # 系统母版禁止编辑
    if existing.get("is_system") and not existing.get("is_editable", True):
        return None
    name = data.pop("name", None) or existing.get("name")
    platform = data.pop("platform", None) or existing.get("platform")
    # 行级字段不允许通过 update 改写
    for k in ("is_system", "is_editable", "template_key",
              "parent_template_id", "is_builtin"):
        data.pop(k, None)
    content = {k: v for k, v in data.items() if k not in _RESERVED_KEYS}
    if not content:
        existing_content = {k: v for k, v in existing.items() if k not in _RESERVED_KEYS}
        content = existing_content
    normalize_template_languages(content)
    return template_repository.update(tpl_id, name=name, platform=platform, content=content)


def delete_template(tpl_id: str) -> bool:
    existing = template_repository.get_by_tpl_id(tpl_id)
    if not existing:
        return False
    # 系统母版禁止删除（同时也会被 routes 层 builtin_ids 拦住）
    if existing.get("is_system"):
        return False
    return template_repository.delete(tpl_id)
