"""模板业务逻辑层 — DB 作为唯一数据源"""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Optional

from repositories import template_repository


def list_templates() -> list[dict]:
    return template_repository.list_all()


def get_template(tpl_id: str) -> Optional[dict]:
    return template_repository.get_by_tpl_id(tpl_id)


def create_template(data: dict, created_by: str = "") -> dict:
    tpl_id = f"tpl_{uuid.uuid4().hex[:12]}"
    name = data.pop("name", "")
    platform = data.pop("platform", "tiktok")
    created_at = datetime.now().isoformat()
    content = {k: v for k, v in data.items() if k not in ("id", "created_at", "updated_at")}
    return template_repository.create(
        tpl_id=tpl_id,
        name=name,
        platform=platform,
        content=content,
        is_builtin=False,
        created_by=created_by,
        created_at=created_at,
    )


def update_template(tpl_id: str, data: dict) -> Optional[dict]:
    existing = template_repository.get_by_tpl_id(tpl_id)
    if not existing:
        return None
    name = data.pop("name", None) or existing.get("name")
    platform = data.pop("platform", None) or existing.get("platform")
    content = {k: v for k, v in data.items()
               if k not in ("id", "name", "platform", "created_at", "updated_at")}
    if not content:
        existing_content = {k: v for k, v in existing.items()
                           if k not in ("id", "name", "platform", "created_at", "updated_at")}
        content = existing_content
    return template_repository.update(tpl_id, name=name, platform=platform, content=content)


def delete_template(tpl_id: str) -> bool:
    return template_repository.delete(tpl_id)
