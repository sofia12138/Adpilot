"""用户认证模块：JWT + 基于 MySQL 的用户存储（通过 user_service）"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import Depends, HTTPException, Request
from jose import JWTError, jwt
from pydantic import BaseModel

from config import get_settings
from services import user_service


# ---------- 数据模型（保持与旧版完全兼容）----------
class User(BaseModel):
    username: str
    hashed_password: str
    role: str  # "admin" | "optimizer" | "designer"
    display_name: str = ""
    assigned_accounts: list[str] = []


class UserOut(BaseModel):
    username: str
    role: str
    display_name: str
    assigned_accounts: list[str]


class LoginRequest(BaseModel):
    username: str
    password: str


class CreateUserRequest(BaseModel):
    username: str
    password: str
    role: str = "optimizer"
    display_name: str = ""
    assigned_accounts: list[str] = []


class UpdateUserRequest(BaseModel):
    password: Optional[str] = None
    role: Optional[str] = None
    display_name: Optional[str] = None
    assigned_accounts: Optional[list[str]] = None


# ---------- 用户操作（委托给 user_service）----------
def get_user(username: str) -> Optional[User]:
    data = user_service.get_user(username)
    if not data:
        return None
    return User(
        username=data["username"],
        hashed_password=data.get("password_hash", ""),
        role=data.get("role", "optimizer"),
        display_name=data.get("display_name", data["username"]),
        assigned_accounts=data.get("assigned_accounts") or [],
    )


def verify_password(plain: str, hashed: str) -> bool:
    return user_service.verify_password(plain, hashed)


def create_user(req: CreateUserRequest) -> User:
    data = user_service.create_user(
        username=req.username,
        password=req.password,
        role=req.role,
        display_name=req.display_name,
        assigned_accounts=req.assigned_accounts,
    )
    return User(
        username=data["username"],
        hashed_password=data.get("password_hash", ""),
        role=data["role"],
        display_name=data.get("display_name", data["username"]),
        assigned_accounts=data.get("assigned_accounts") or [],
    )


def update_user(username: str, req: UpdateUserRequest) -> User:
    data = user_service.update_user(
        username,
        password=req.password,
        role=req.role,
        display_name=req.display_name,
        assigned_accounts=req.assigned_accounts,
    )
    return User(
        username=data["username"],
        hashed_password=data.get("password_hash", ""),
        role=data["role"],
        display_name=data.get("display_name", data["username"]),
        assigned_accounts=data.get("assigned_accounts") or [],
    )


def delete_user(username: str):
    user_service.delete_user(username)


def list_users() -> list[UserOut]:
    rows = user_service.list_users()
    return [UserOut(
        username=u["username"],
        role=u.get("role", "optimizer"),
        display_name=u.get("display_name", u["username"]),
        assigned_accounts=u.get("assigned_accounts") or [],
    ) for u in rows]


# ---------- JWT ----------
def create_token(username: str, role: str) -> str:
    settings = get_settings()
    expire = datetime.now(timezone.utc) + timedelta(hours=settings.jwt_expire_hours)
    payload = {"sub": username, "role": role, "exp": expire}
    return jwt.encode(payload, settings.jwt_secret_key, algorithm=settings.jwt_algorithm)


def decode_token(token: str) -> dict:
    settings = get_settings()
    try:
        return jwt.decode(token, settings.jwt_secret_key, algorithms=[settings.jwt_algorithm])
    except JWTError:
        raise HTTPException(status_code=401, detail="Token 无效或已过期")


# ---------- FastAPI 依赖 ----------
def get_current_user(request: Request) -> User:
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="未登录")
    token = auth_header[7:]
    payload = decode_token(token)
    user = get_user(payload["sub"])
    if not user:
        raise HTTPException(status_code=401, detail="用户不存在")
    return user


def require_admin(user: User = Depends(get_current_user)) -> User:
    if user.role not in ("admin", "super_admin"):
        raise HTTPException(status_code=403, detail="需要管理员权限")
    return user
