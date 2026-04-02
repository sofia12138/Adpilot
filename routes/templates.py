"""广告投放模板管理 + 基于模板创建广告"""
from __future__ import annotations

from fastapi import APIRouter, Body, Depends, Query
from loguru import logger

from tiktok_ads.api.client import TikTokClient
from services import template_service
from services.oplog_service import log_operation
from auth import get_current_user, User

router = APIRouter(prefix="/templates", tags=["投放模板"])

# 内置默认模板定义 — 仅供 db.py migrate_json_data() 初始化时引用
# 运行时所有读取只走 MySQL，不再使用此常量
BUILTIN_TEMPLATES: list[dict] = [
    {
        "id": "tpl_tiktok_android_purchase",
        "name": "TikTok 安卓-付费版",
        "platform": "tiktok",
        "objective_type": "APP_PROMOTION",
        "app_promotion_type": "APP_INSTALL",
        "optimization_goal": "IN_APP_EVENT",
        "operating_system": "ANDROID",
        "age_min": 18,
        "deep_bid_type": "AEO",
        "billing_event": "OCPM",
        "placement_type": "PLACEMENT_TYPE_AUTOMATIC",
        "budget_mode": "BUDGET_MODE_DYNAMIC_DAILY_BUDGET",
        "schedule_type": "SCHEDULE_FROM_NOW",
        "created_at": "2026-03-12T00:00:00",
    },
    {
        "id": "tpl_web_to_app",
        "name": "Web to App - 网页转化",
        "platform": "tiktok",
        "objective_type": "WEB_CONVERSIONS",
        "promotion_type": "WEBSITE",
        "optimization_goal": "CONVERT",
        "optimization_event": "SHOPPING",
        "billing_event": "OCPM",
        "bid_type": "BID_TYPE_NO_BID",
        "age_min": 18,
        "placement_type": "PLACEMENT_TYPE_AUTOMATIC",
        "budget_mode": "BUDGET_MODE_DYNAMIC_DAILY_BUDGET",
        "schedule_type": "SCHEDULE_FROM_NOW",
        "pixel_id": "7591744906821959696",
        "created_at": "2026-03-13T12:00:00",
    },
    {
        "id": "tpl_miniapp_troas",
        "name": "小程序投放—TROAS",
        "platform": "tiktok",
        "objective_type": "APP_PROMOTION",
        "promotion_type": "MINI_APP",
        "optimization_goal": "VALUE",
        "optimization_event": "ACTIVE_PAY",
        "deep_bid_type": "VO_MIN_ROAS",
        "roas_bid": 0.9,
        "secondary_optimization_event": "PURCHASE_ROI",
        "billing_event": "OCPM",
        "bid_type": "BID_TYPE_NO_BID",
        "placement_type": "PLACEMENT_TYPE_NORMAL",
        "placements": ["PLACEMENT_TIKTOK"],
        "age_min": 18,
        "budget_mode": "BUDGET_MODE_DYNAMIC_DAILY_BUDGET",
        "schedule_type": "SCHEDULE_FROM_NOW",
        "vbo_window": "ZERO_DAY",
        "minis_id": "mnu8f8spjpxjy7oa",
        "app_id": "7613116626166104080",
        "created_at": "2026-03-13T20:30:00",
    },
]


# ── CRUD ──────────────────────────────────────────────────

@router.get("/")
async def list_templates():
    return {"data": template_service.list_templates()}


@router.get("/{tpl_id}")
async def get_template(tpl_id: str):
    t = template_service.get_template(tpl_id)
    if not t:
        return {"error": "模板不存在"}
    return {"data": t}


@router.post("/")
async def create_template(tpl: dict = Body(...), user: User = Depends(get_current_user)):
    result = template_service.create_template(tpl)
    log_operation(
        username=user.username,
        action="创建模板",
        target_type="template",
        target_id=result.get("id", ""),
    )
    return {"data": result}


@router.put("/{tpl_id}")
async def update_template(tpl_id: str, tpl: dict = Body(...), user: User = Depends(get_current_user)):
    result = template_service.update_template(tpl_id, tpl)
    if not result:
        return {"error": "模板不存在"}
    log_operation(
        username=user.username,
        action="更新模板",
        target_type="template",
        target_id=tpl_id,
    )
    return {"data": result}


@router.delete("/{tpl_id}")
async def delete_template(tpl_id: str, user: User = Depends(get_current_user)):
    template_service.delete_template(tpl_id)
    log_operation(username=user.username, action="删除模板", target_type="template", target_id=tpl_id)
    return {"message": "ok"}


# ── 基于模板创建广告 ──────────────────────────────────────

@router.post("/launch")
async def launch_from_template(payload: dict = Body(...), _user: User = Depends(get_current_user)):
    """
    payload:
      template_id       - 模板 ID
      advertiser_id     - 广告账户 ID
      campaign_name     - 广告系列名称
      budget_mode       - "CBO" | "ADGROUP"
      budget            - 预算金额
      location_ids      - [地区代码列表]
      languages         - [语言代码列表]
      placement_type    - "PLACEMENT_TYPE_AUTOMATIC" | "PLACEMENT_TYPE_NORMAL"
      placements        - [版位列表]
      ad_text           - 广告文案
      deeplink          - Deeplink URL
      app_id            - TikTok 应用 ID (可选)
      identity_id       - 身份 ID (可选)
      identity_type     - 身份类型 (可选)
      creatives         - [ { video_id, image_id, name } ... ]
    """
    tpl_id = payload.get("template_id")
    tpl = template_service.get_template(tpl_id)
    if not tpl:
        return {"error": "模板不存在"}

    advertiser_id = payload["advertiser_id"]
    campaign_name = payload["campaign_name"]
    budget_mode_choice = payload.get("budget_mode", "ADGROUP")
    budget = float(payload.get("budget", 50))
    location_ids = payload.get("location_ids", [])
    languages = payload.get("languages", [])
    placement_type = payload.get("placement_type", tpl.get("placement_type", "PLACEMENT_TYPE_AUTOMATIC"))
    placements = payload.get("placements")
    schedule_type = payload.get("schedule_type", tpl.get("schedule_type", "SCHEDULE_FROM_NOW"))
    schedule_start_time = payload.get("schedule_start_time")
    schedule_end_time = payload.get("schedule_end_time")
    ad_text = payload.get("ad_text", "")
    deeplink = payload.get("deeplink", "")
    app_id = payload.get("app_id")
    identity_id = payload.get("identity_id")
    identity_type = payload.get("identity_type")
    creatives = payload.get("creatives", [])

    client = TikTokClient()
    results = {"campaign": None, "adgroup": None, "ads": [], "summary": {"total": 0, "success": 0, "fail": 0}}

    # ── 1. 创建 Campaign ──
    try:
        campaign_payload = {
            "advertiser_id": advertiser_id,
            "campaign_name": campaign_name,
            "objective_type": tpl.get("objective_type", "APP_PROMOTION"),
            "campaign_type": "REGULAR_CAMPAIGN",
            "operation_status": "ENABLE",
        }
        if tpl.get("app_promotion_type"):
            campaign_payload["app_promotion_type"] = tpl["app_promotion_type"]

        if budget_mode_choice == "CBO":
            campaign_payload["budget_mode"] = "BUDGET_MODE_DAY"
            campaign_payload["budget"] = budget
        else:
            campaign_payload["budget_mode"] = "BUDGET_MODE_INFINITE"

        campaign_data = await client.post("campaign/create/", campaign_payload)
        campaign_id = campaign_data.get("campaign_id") or campaign_data.get("campaign_ids", [""])[0]
        results["campaign"] = {"success": True, "campaign_id": campaign_id}
        logger.info(f"Campaign 创建成功: {campaign_id}")
    except Exception as e:
        logger.error(f"Campaign 创建失败: {e}")
        results["campaign"] = {"success": False, "error": str(e)}
        return {"data": results}

    # ── 2. 创建 Ad Group ──
    try:
        adgroup_payload: dict = {
            "advertiser_id": advertiser_id,
            "campaign_id": campaign_id,
            "adgroup_name": campaign_name,
            "optimization_goal": tpl.get("optimization_goal", "IN_APP_EVENT"),
            "billing_event": tpl.get("billing_event", "OCPM"),
            "bid_type": tpl.get("bid_type", "BID_TYPE_NO_BID"),
            "placement_type": placement_type,
            "schedule_type": schedule_type,
            "operating_systems": [tpl.get("operating_system", "ANDROID")],
            "location_ids": location_ids,
            "operation_status": "ENABLE",
        }

        if tpl.get("secondary_optimization_event"):
            adgroup_payload["secondary_optimization_event"] = tpl["secondary_optimization_event"]

        if tpl.get("age_min"):
            adgroup_payload["age_groups"] = _age_groups_from_min(tpl["age_min"])

        if languages:
            adgroup_payload["languages"] = languages

        if placement_type == "PLACEMENT_TYPE_NORMAL" and placements:
            adgroup_payload["placements"] = placements

        if budget_mode_choice != "CBO":
            adgroup_payload["budget_mode"] = "BUDGET_MODE_DAY"
            adgroup_payload["budget"] = budget

        if schedule_start_time:
            adgroup_payload["schedule_start_time"] = schedule_start_time
        if schedule_end_time:
            adgroup_payload["schedule_end_time"] = schedule_end_time

        if app_id:
            adgroup_payload["app_id"] = app_id
            adgroup_payload["promotion_type"] = "APP_ANDROID"

        adgroup_data = await client.post("adgroup/create/", adgroup_payload)
        adgroup_id = adgroup_data.get("adgroup_id") or adgroup_data.get("adgroup_ids", [""])[0]
        results["adgroup"] = {"success": True, "adgroup_id": adgroup_id}
        logger.info(f"AdGroup 创建成功: {adgroup_id}")
    except Exception as e:
        logger.error(f"AdGroup 创建失败: {e}")
        results["adgroup"] = {"success": False, "error": str(e)}
        return {"data": results}

    # ── 3. 逐素材创建 Ad ──
    results["summary"]["total"] = len(creatives)
    for cr in creatives:
        ad_name = cr.get("name", "Ad")
        try:
            ad_payload: dict = {
                "advertiser_id": advertiser_id,
                "adgroup_id": adgroup_id,
                "ad_name": ad_name,
                "ad_text": ad_text,
                "ad_format": "SINGLE_VIDEO" if cr.get("video_id") else "SINGLE_IMAGE",
            }
            if cr.get("video_id"):
                ad_payload["video_id"] = cr["video_id"]
            if cr.get("image_id"):
                ad_payload["image_ids"] = [cr["image_id"]]

            if deeplink:
                ad_payload["deeplink"] = deeplink

            if identity_id:
                ad_payload["identity_id"] = identity_id
                ad_payload["identity_type"] = identity_type or "CUSTOMIZED_USER"

            ad_data = await client.post("ad/create/", ad_payload)
            ad_id = ad_data.get("ad_id") or ad_data.get("ad_ids", [""])[0]
            results["ads"].append({"ad_name": ad_name, "success": True, "ad_id": ad_id})
            results["summary"]["success"] += 1
            logger.info(f"Ad 创建成功: {ad_name} -> {ad_id}")
        except Exception as e:
            logger.error(f"Ad 创建失败 [{ad_name}]: {e}")
            results["ads"].append({"ad_name": ad_name, "success": False, "error": str(e)})
            results["summary"]["fail"] += 1

    return {"data": results}


def _age_groups_from_min(age_min: int) -> list[str]:
    """根据最低年龄生成 TikTok age_groups 列表"""
    all_groups = [
        ("AGE_13_17", 13), ("AGE_18_24", 18), ("AGE_25_34", 25),
        ("AGE_35_44", 35), ("AGE_45_54", 45), ("AGE_55_100", 55),
    ]
    return [g for g, a in all_groups if a >= age_min]
