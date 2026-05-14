"""用户付费面板 · 白名单审批流 service 层

业务规则：
- 任何 super_admin / admin 都可以"申请"
- 只有 super_admin 才能"审批"
- 申请人 ≠ 审批人（强制约束）
- 一个 user_id 同时只能有一个 pending 工单
- 审批通过：自动写入 biz_user_anomaly_whitelist（action='add'）或删除（action='remove'）
- 审批拒绝 / 撤回：不影响白名单表

抛出 ApplicationError 表示业务校验失败，routes 层统一捕获转 4xx。
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from repositories import user_anomaly_application_repository as app_repo
from repositories import user_anomaly_whitelist_repository as whitelist_repo

logger = logging.getLogger(__name__)


class ApplicationError(Exception):
    """业务校验错误（如重复 pending、申请人=审批人等），routes 层转 400/403。"""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


def submit_application(
    *,
    target_user_id: int,
    requested_tag: str,
    action: str,
    reason: str,
    applicant_user: str,
) -> dict:
    if not applicant_user:
        raise ApplicationError("applicant_required", "申请人未识别")
    if not reason or not reason.strip():
        raise ApplicationError("reason_required", "请填写申请理由")
    if action not in ("add", "remove"):
        raise ApplicationError("invalid_action", f"action 必须是 add 或 remove，收到 {action!r}")

    if app_repo.has_pending_for_user(target_user_id):
        raise ApplicationError(
            "duplicate_pending",
            f"用户 {target_user_id} 已有 pending 工单，请等待审批完成后再申请",
        )

    if action == "add" and whitelist_repo.get_by_user(target_user_id):
        raise ApplicationError(
            "already_whitelisted",
            f"用户 {target_user_id} 已在白名单中。如需修改 tag，请先申请 remove 再申请 add",
        )
    if action == "remove" and not whitelist_repo.get_by_user(target_user_id):
        raise ApplicationError(
            "not_in_whitelist",
            f"用户 {target_user_id} 不在白名单中，无需移除",
        )

    application_id = app_repo.create(
        target_user_id=target_user_id,
        requested_tag=requested_tag,
        action=action,
        reason=reason.strip(),
        applicant_user=applicant_user,
    )
    logger.info(
        "[approval] submit application id=%s target_user=%s action=%s by %s",
        application_id, target_user_id, action, applicant_user,
    )
    return app_repo.get(application_id)


def approve(*, application_id: int, reviewer_user: str, review_note: str = "") -> dict:
    """通过工单：申请人 ≠ 审批人；通过后联动 whitelist 表。"""
    app = app_repo.get(application_id)
    if not app:
        raise ApplicationError("not_found", f"工单 {application_id} 不存在")
    if app["status"] != "pending":
        raise ApplicationError(
            "not_pending",
            f"工单 {application_id} 当前状态是 {app['status']}，无法审批",
        )
    if not reviewer_user:
        raise ApplicationError("reviewer_required", "审批人未识别")
    if reviewer_user == app["applicant_user"]:
        raise ApplicationError(
            "self_review_forbidden",
            "申请人不能审批自己的工单",
        )

    rows = app_repo.update_status(
        application_id=application_id,
        new_status="approved",
        reviewer_user=reviewer_user,
        review_note=review_note,
    )
    if rows == 0:
        # 并发场景：另一个审批人刚刚改了状态
        raise ApplicationError(
            "race_condition",
            "工单状态已被其他审批人改变，请刷新后重试",
        )

    # 联动白名单表
    if app["action"] == "add":
        whitelist_repo.upsert(
            user_id=int(app["target_user_id"]),
            tag=app["requested_tag"],
            reason=app["reason"],
            marked_by=reviewer_user,
            application_id=application_id,
        )
    elif app["action"] == "remove":
        whitelist_repo.delete(int(app["target_user_id"]))

    logger.info(
        "[approval] APPROVE application id=%s target_user=%s action=%s by %s",
        application_id, app["target_user_id"], app["action"], reviewer_user,
    )
    return app_repo.get(application_id)


def reject(*, application_id: int, reviewer_user: str, review_note: str = "") -> dict:
    app = app_repo.get(application_id)
    if not app:
        raise ApplicationError("not_found", f"工单 {application_id} 不存在")
    if app["status"] != "pending":
        raise ApplicationError(
            "not_pending",
            f"工单 {application_id} 当前状态是 {app['status']}，无法拒绝",
        )
    if not reviewer_user:
        raise ApplicationError("reviewer_required", "审批人未识别")
    if reviewer_user == app["applicant_user"]:
        raise ApplicationError(
            "self_review_forbidden",
            "申请人不能审批（拒绝）自己的工单",
        )

    rows = app_repo.update_status(
        application_id=application_id,
        new_status="rejected",
        reviewer_user=reviewer_user,
        review_note=review_note,
    )
    if rows == 0:
        raise ApplicationError(
            "race_condition",
            "工单状态已被其他审批人改变，请刷新后重试",
        )
    logger.info(
        "[approval] REJECT application id=%s by %s",
        application_id, reviewer_user,
    )
    return app_repo.get(application_id)


def withdraw(*, application_id: int, applicant_user: str) -> dict:
    """申请人主动撤回 pending 工单。"""
    app = app_repo.get(application_id)
    if not app:
        raise ApplicationError("not_found", f"工单 {application_id} 不存在")
    if app["status"] != "pending":
        raise ApplicationError(
            "not_pending",
            f"工单 {application_id} 当前状态是 {app['status']}，无法撤回",
        )
    if not applicant_user:
        raise ApplicationError("applicant_required", "申请人未识别")
    if app["applicant_user"] != applicant_user:
        raise ApplicationError(
            "not_applicant",
            "只有申请人本人可以撤回工单",
        )
    rows = app_repo.update_status(
        application_id=application_id,
        new_status="withdrawn",
        reviewer_user=None,
        review_note="",
    )
    if rows == 0:
        raise ApplicationError(
            "race_condition",
            "工单状态已被改变，请刷新后重试",
        )
    logger.info("[approval] WITHDRAW application id=%s by %s", application_id, applicant_user)
    return app_repo.get(application_id)


def list_applications(
    *,
    status: Optional[str] = None,
    target_user_id: Optional[int] = None,
    applicant_user: Optional[str] = None,
    page: int = 1,
    page_size: int = 50,
) -> dict:
    page = max(1, int(page))
    page_size = max(1, min(int(page_size), 500))
    offset = (page - 1) * page_size
    items = app_repo.list_by(
        status=status,
        target_user_id=target_user_id,
        applicant_user=applicant_user,
        limit=page_size,
        offset=offset,
    )
    return {"items": items, "page": page, "page_size": page_size}


def count_pending() -> int:
    return app_repo.count_pending()
