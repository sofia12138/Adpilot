"""同步任务状态管理（进程内内存存储）

提供全局的同步任务状态读写，供定时调度和手动触发共用。
"""
from datetime import datetime
from typing import Optional

_state: dict = {
    "is_running":    False,
    "last_synced_at": None,   # datetime | None
    "last_error":    None,    # str | None
    "last_range":    None,    # str，如 "2026-04-06 ~ 2026-04-13"
}


def get_state() -> dict:
    """返回可序列化的状态快照"""
    return {
        "is_running":    _state["is_running"],
        "last_synced_at": (
            _state["last_synced_at"].isoformat() if _state["last_synced_at"] else None
        ),
        "last_error":    _state["last_error"],
        "last_range":    _state["last_range"],
    }


def set_running(running: bool, date_range: Optional[str] = None):
    _state["is_running"] = running
    if date_range:
        _state["last_range"] = date_range


def set_done(dt: Optional[datetime] = None):
    _state["is_running"]    = False
    _state["last_synced_at"] = dt or datetime.now()
    _state["last_error"]    = None


def set_error(err: str):
    _state["is_running"] = False
    _state["last_error"] = err
