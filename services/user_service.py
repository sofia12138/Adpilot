"""用户业务逻辑层"""
from __future__ import annotations

from typing import Optional

from fastapi import HTTPException
from passlib.context import CryptContext

from repositories import user_repository

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_password(plain: str) -> str:
    return pwd_context.hash(plain)


def verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)


def get_user(username: str) -> Optional[dict]:
    return user_repository.get_by_username(username)


def list_users() -> list[dict]:
    return user_repository.list_all()


def create_user(*, username: str, password: str, role: str = "optimizer",
                display_name: str = "", assigned_accounts: list[str] | None = None) -> dict:
    existing = user_repository.get_by_username(username)
    if existing:
        raise HTTPException(status_code=400, detail="用户名已存在")
    return user_repository.create(
        username=username,
        password_hash=hash_password(password),
        role=role,
        display_name=display_name or username,
        assigned_accounts=assigned_accounts or [],
    )


def update_user(username: str, *, password: str | None = None,
                role: str | None = None, display_name: str | None = None,
                assigned_accounts: list[str] | None = None) -> dict:
    existing = user_repository.get_by_username(username)
    if not existing:
        raise HTTPException(status_code=404, detail="用户不存在")
    fields = {}
    if password is not None:
        fields["password_hash"] = hash_password(password)
    if role is not None:
        fields["role"] = role
    if display_name is not None:
        fields["display_name"] = display_name
    if assigned_accounts is not None:
        fields["assigned_accounts"] = assigned_accounts
    return user_repository.update(username, **fields)


def delete_user(username: str):
    existing = user_repository.get_by_username(username)
    if not existing:
        raise HTTPException(status_code=404, detail="用户不存在")
    if existing["role"] == "admin":
        admin_count = user_repository.count_by_role("admin")
        if admin_count <= 1:
            raise HTTPException(status_code=400, detail="不能删除最后一个管理员")
    user_repository.delete(username)


def ensure_default_admin():
    """启动时确保至少有一个管理员和一个超级管理员账户存在"""
    try:
        admin_count = user_repository.count_by_role("admin")
        if admin_count == 0:
            users = user_repository.list_all()
            if not users:
                user_repository.create(
                    username="admin",
                    password_hash=hash_password("admin123"),
                    role="admin",
                    display_name="管理员",
                    assigned_accounts=[],
                )
    except Exception:
        pass
    try:
        sa_count = user_repository.count_by_role("super_admin")
        if sa_count == 0:
            existing = user_repository.get_by_username("superadmin")
            if not existing:
                user_repository.create(
                    username="superadmin",
                    password_hash=hash_password("super123"),
                    role="super_admin",
                    display_name="超级管理员",
                    assigned_accounts=[],
                )
    except Exception:
        pass
