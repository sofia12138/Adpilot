"""面板权限管理路由"""
from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from auth import User, get_current_user
from services import panel_service
from services.oplog_service import log_operation

router = APIRouter(prefix="/panels", tags=["面板权限"])


def _require_super_admin(user: User = Depends(get_current_user)) -> User:
    if user.role not in ("admin", "super_admin"):
        from fastapi import HTTPException
        raise HTTPException(status_code=403, detail="需要管理员权限")
    return user


class PanelKeysBody(BaseModel):
    panel_keys: list[str]


@router.get("/")
async def list_panels(_: User = Depends(get_current_user)):
    panels = panel_service.list_panels()
    return {"panels": panels}


@router.get("/roles/{role_key}")
async def get_role_panels(role_key: str, _: User = Depends(_require_super_admin)):
    panels = panel_service.get_role_panels(role_key)
    return {"role_key": role_key, "panel_keys": panels}


@router.put("/roles/{role_key}")
async def update_role_panels(role_key: str, body: PanelKeysBody,
                             admin: User = Depends(_require_super_admin)):
    panel_service.set_role_panels(role_key, body.panel_keys)
    from repositories import panel_repository
    cleared = panel_repository.clear_user_overrides_by_role(role_key)
    log_operation(
        username=admin.username,
        action=f"更新角色面板权限(清除{cleared}个用户覆盖)" if cleared else "更新角色面板权限",
        target_type="role",
        target_id=role_key,
    )
    return {"ok": True, "role_key": role_key, "panel_keys": body.panel_keys,
            "cleared_user_overrides": cleared}


@router.get("/users/{username}")
async def get_user_panels(username: str, _: User = Depends(_require_super_admin)):
    result = panel_service.get_user_allowed_panels(username)
    return result


@router.put("/users/{username}")
async def update_user_panels(username: str, body: PanelKeysBody,
                             admin: User = Depends(_require_super_admin)):
    panel_service.set_user_panels(username, body.panel_keys)
    log_operation(
        username=admin.username,
        action="更新用户面板权限",
        target_type="user_panel",
        target_id=username,
    )
    return {"ok": True, "username": username, "panel_keys": body.panel_keys}


@router.delete("/users/{username}")
async def reset_user_panels(username: str, admin: User = Depends(_require_super_admin)):
    """重置用户个性化权限，回退为角色默认"""
    panel_service.set_user_panels(username, [])
    from repositories import panel_repository
    panel_repository.set_user_panels(username, [])
    log_operation(
        username=admin.username,
        action="重置用户面板权限",
        target_type="user_panel",
        target_id=username,
    )
    return {"ok": True, "username": username, "message": "已重置为角色默认权限"}


@router.get("/my")
async def get_my_panels(user: User = Depends(get_current_user)):
    allowed = panel_service.resolve_allowed_panels(user.username, user.role)
    return {"username": user.username, "role": user.role, "allowed_panels": allowed}
