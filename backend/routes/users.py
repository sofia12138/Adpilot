"""用户认证 & 管理路由"""
from __future__ import annotations

from fastapi import APIRouter, Depends

from auth import (
    LoginRequest, CreateUserRequest, UpdateUserRequest, UserOut,
    User, get_user, verify_password, create_token,
    create_user, update_user, delete_user, list_users,
    get_current_user, require_admin,
)
from services.oplog_service import log_operation

router = APIRouter(prefix="/users", tags=["用户管理"])


@router.post("/login")
async def login(req: LoginRequest):
    user = get_user(req.username)
    if not user or not verify_password(req.password, user.hashed_password):
        return {"ok": False, "message": "用户名或密码错误"}
    token = create_token(user.username, user.role)
    from services.panel_service import resolve_allowed_panels
    allowed_panels = resolve_allowed_panels(user.username, user.role)
    return {
        "ok": True,
        "token": token,
        "user": {
            "username": user.username,
            "role": user.role,
            "display_name": user.display_name,
            "assigned_accounts": user.assigned_accounts,
        },
        "allowed_panels": allowed_panels,
    }


@router.get("/me")
async def me(user: User = Depends(get_current_user)):
    return {
        "username": user.username,
        "role": user.role,
        "display_name": user.display_name,
        "assigned_accounts": user.assigned_accounts,
    }


@router.get("/", response_model=list[UserOut])
async def get_users(_: User = Depends(require_admin)):
    return list_users()


@router.post("/", response_model=UserOut)
async def add_user(req: CreateUserRequest, admin: User = Depends(require_admin)):
    u = create_user(req)
    log_operation(
        username=admin.username,
        action="创建用户",
        target_type="user",
        target_id=u.username,
    )
    return UserOut(
        username=u.username, role=u.role,
        display_name=u.display_name, assigned_accounts=u.assigned_accounts,
    )


@router.put("/{username}", response_model=UserOut)
async def edit_user(username: str, req: UpdateUserRequest, admin: User = Depends(require_admin)):
    u = update_user(username, req)
    log_operation(
        username=admin.username,
        action="更新用户",
        target_type="user",
        target_id=username,
    )
    return UserOut(
        username=u.username, role=u.role,
        display_name=u.display_name, assigned_accounts=u.assigned_accounts,
    )


@router.delete("/{username}")
async def remove_user(username: str, admin: User = Depends(require_admin)):
    delete_user(username)
    log_operation(
        username=admin.username,
        action="删除用户",
        target_type="user",
        target_id=username,
    )
    return {"ok": True}
