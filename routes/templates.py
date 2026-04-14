"""广告投放模板管理 + 基于模板创建广告（TikTok / Meta 双平台）"""
from __future__ import annotations

import json
from fastapi import APIRouter, Body, Depends, Query
from loguru import logger

from tiktok_ads.api.client import TikTokClient
from meta_ads.api.client import MetaClient
from meta_ads.api.campaigns import MetaCampaignService
from meta_ads.api.adsets import MetaAdSetService
from meta_ads.api.ads import MetaAdService
from services import template_service
from services.oplog_service import log_operation
from auth import get_current_user, User
from repositories import biz_account_repository

router = APIRouter(prefix="/templates", tags=["投放模板"])

# ── 内置默认模板（仅供 db.py migrate_json_data 初始化时引用）───

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
    {
        "id": "tpl_meta_us_aeo",
        "name": "Meta US AEO Basic",
        "platform": "meta",
        "campaign": {
            "objective": "OUTCOME_APP_PROMOTION",
            "status": "PAUSED",
            "special_ad_categories": [],
            "is_adset_budget_sharing_enabled": False,
        },
        "adset": {
            "billing_event": "IMPRESSIONS",
            "optimization_goal": "APP_INSTALLS",
            "daily_budget": 5000,
            "targeting": {
                "geo_locations": {"countries": ["US"]},
                "age_min": 18,
                "age_max": 65,
            },
            "promoted_object": {},
            "status": "PAUSED",
        },
        "creative": {
            "page_id": "",
            "primary_text": "",
            "headline": "",
            "description": "",
            "call_to_action": "INSTALL_NOW",
            "link": "",
            "image_hash": "",
            "video_id": "",
        },
        "ad": {
            "status": "PAUSED",
        },
        "created_at": "2026-04-09T00:00:00",
    },
    {
        "id": "tpl_meta_web_to_app_basic_abo",
        "name": "Meta Web to App Basic (ABO)",
        "platform": "meta",
        "template_type": "web_to_app",
        "campaign": {
            "objective": "OUTCOME_TRAFFIC",
            "status": "PAUSED",
            "special_ad_categories": [],
            "is_adset_budget_sharing_enabled": False,
        },
        "adset": {
            "billing_event": "IMPRESSIONS",
            "optimization_goal": "LANDING_PAGE_VIEWS",
            "daily_budget": 5000,
            "status": "PAUSED",
            "targeting": {
                "geo_locations": {"countries": ["US"]},
                "age_min": 18,
                "age_max": 65,
            },
            "promoted_object": {
                "pixel_id": "",
                "custom_event_type": "",
            },
        },
        "creative": {
            "page_id": "",
            "primary_text": "Watch now on our site, then continue in the app.",
            "headline": "Start Watching Now",
            "description": "",
            "call_to_action": "LEARN_MORE",
            "link": "",
            "image_hash": "",
            "video_id": "",
        },
        "tracking": {
            "landing_page_url": "",
            "deep_link_url": "",
            "app_store_url": "",
            "fallback_url": "",
            "utm_source": "meta",
            "utm_campaign": "",
            "utm_content": "",
        },
        "ad": {
            "status": "PAUSED",
        },
        "created_at": "2026-04-14T00:00:00",
    },
    {
        "id": "tpl_meta_web_to_app_conv_abo",
        "name": "Meta Web to App Conversion (ABO)",
        "platform": "meta",
        "template_type": "web_to_app",
        "template_subtype": "conversion",
        "campaign": {
            "objective": "OUTCOME_SALES",
            "status": "PAUSED",
            "special_ad_categories": [],
            "is_adset_budget_sharing_enabled": False,
        },
        "adset": {
            "billing_event": "IMPRESSIONS",
            "optimization_goal": "OFFSITE_CONVERSIONS",
            "daily_budget": 5000,
            "status": "PAUSED",
            "targeting": {
                "geo_locations": {"countries": ["US"]},
                "age_min": 18,
                "age_max": 65,
            },
            "promoted_object": {
                "pixel_id": "",
                "custom_event_type": "PURCHASE",
            },
        },
        "creative": {
            "page_id": "",
            "primary_text": "Watch now on our site, then continue in the app.",
            "headline": "Start Watching Now",
            "description": "",
            "call_to_action": "LEARN_MORE",
            "link": "",
            "image_hash": "",
            "video_id": "",
        },
        "tracking": {
            "landing_page_url": "",
            "deep_link_url": "",
            "app_store_url": "",
            "fallback_url": "",
            "utm_source": "meta",
            "utm_campaign": "",
            "utm_content": "",
        },
        "ad": {
            "status": "PAUSED",
        },
        "created_at": "2026-04-14T12:00:00",
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
        platform=result.get("platform", ""),
        after_data=_tpl_summary(result),
        error_message=f"创建模板: {result.get('name', '')} ({result.get('platform', '')})",
    )
    return {"data": result}


@router.put("/{tpl_id}")
async def update_template(tpl_id: str, tpl: dict = Body(...), user: User = Depends(get_current_user)):
    old = template_service.get_template(tpl_id)
    if not old:
        return {"error": "模板不存在"}
    result = template_service.update_template(tpl_id, tpl)
    if not result:
        return {"error": "模板不存在"}

    changed = _diff_fields(old, result)
    detail = f"changed_fields: {', '.join(changed)}" if changed else "无字段变更"
    log_operation(
        username=user.username,
        action="更新模板",
        target_type="template",
        target_id=tpl_id,
        platform=result.get("platform", ""),
        before_data=_tpl_summary(old),
        after_data=_tpl_summary(result),
        error_message=detail,
    )
    return {"data": result}


@router.delete("/{tpl_id}")
async def delete_template(tpl_id: str, user: User = Depends(get_current_user)):
    _builtin_ids = {t["id"] for t in BUILTIN_TEMPLATES}
    if tpl_id in _builtin_ids:
        return {"error": "系统内置模板不可删除"}
    old = template_service.get_template(tpl_id)
    if not old:
        return {"error": "模板不存在"}
    template_service.delete_template(tpl_id)
    log_operation(
        username=user.username,
        action="删除模板",
        target_type="template",
        target_id=tpl_id,
        platform=old.get("platform", "") if old else "",
        before_data=_tpl_summary(old) if old else None,
        error_message=f"删除模板: {old.get('name', tpl_id) if old else tpl_id}",
    )
    return {"message": "ok"}


@router.post("/{tpl_id}/clone")
async def clone_template(tpl_id: str, body: dict = Body(...), user: User = Depends(get_current_user)):
    """从现有模板另存为一个新业务模板副本"""
    source = template_service.get_template(tpl_id)
    if not source:
        return {"error": "源模板不存在"}

    new_name = body.get("name", "").strip()
    if not new_name:
        return {"error": "新模板名称不能为空"}

    clone_data = {k: v for k, v in source.items()
                  if k not in ("id", "name", "created_at", "updated_at", "is_builtin")}
    clone_data["name"] = new_name
    if body.get("notes"):
        clone_data["notes"] = body["notes"]

    result = template_service.create_template(clone_data)

    log_operation(
        username=user.username,
        action="另存为模板",
        target_type="template",
        target_id=result.get("id", ""),
        platform=result.get("platform", ""),
        after_data={
            "source_template_id": tpl_id,
            "new_template_id": result.get("id", ""),
            "new_name": new_name,
            "template_type": result.get("template_type", ""),
            "platform": result.get("platform", ""),
        },
        error_message=f"另存为模板: {new_name} (来源: {source.get('name', tpl_id)})",
    )
    return {"data": result}


# ── 基于模板创建广告（平台分流）────────────────────────────

@router.post("/launch")
async def launch_from_template(payload: dict = Body(...), _user: User = Depends(get_current_user)):
    """
    通用 payload:
      template_id       - 模板 tpl_id
      campaign_name     - 广告系列名称
      budget            - 预算金额 (USD)

    TikTok 额外字段:
      advertiser_id, budget_mode, location_ids, languages,
      placement_type, placements, ad_text, deeplink,
      app_id, identity_id, identity_type, creatives

    Meta 额外字段:
      ad_account_id     - Meta 广告账户 ID (必填, 如 act_123456)
      overrides         - 可选覆盖字段, 支持 campaign/adset/creative/ad 子节点
    """
    tpl_id = payload.get("template_id")
    tpl = template_service.get_template(tpl_id)
    if not tpl:
        return {"error": "模板不存在"}

    platform = tpl.get("platform", "tiktok")
    logger.info(f"[launch] template_id={tpl_id}  platform={platform}")

    if platform == "meta":
        resp = await _launch_meta(tpl, payload)
    else:
        resp = await _launch_tiktok(tpl, payload)

    _log_launch(_user, tpl_id, platform, payload, resp)
    return resp


# ══════════════════════════════════════════════════════════
#  Meta 模板投放
# ══════════════════════════════════════════════════════════

async def _launch_meta(tpl: dict, payload: dict) -> dict:
    tpl_id = tpl.get("id", "?")
    template_type = tpl.get("template_type", "")
    ad_account_id = payload.get("ad_account_id", "").strip()
    if not ad_account_id:
        return {"error": "Meta 投放必须提供 ad_account_id（如 act_123456）"}

    campaign_name = payload.get("campaign_name", "")
    if not campaign_name:
        return {"error": "campaign_name 不能为空"}

    budget = float(payload.get("budget", 0))
    overrides = payload.get("overrides", {})

    results: dict = {
        "platform": "meta",
        "template_type": template_type,
        "ad_account_id": ad_account_id,
        "campaign": None,
        "adset": None,
        "ad": None,
    }

    tpl_campaign = _deep_merge(tpl.get("campaign", {}), overrides.get("campaign", {}))
    tpl_adset = _deep_merge(tpl.get("adset", {}), overrides.get("adset", {}))
    tpl_creative = _deep_merge(tpl.get("creative", {}), overrides.get("creative", {}))
    tpl_tracking = _deep_merge(tpl.get("tracking", {}), overrides.get("tracking", {}))
    tpl_ad = _deep_merge(tpl.get("ad", {}), overrides.get("ad", {}))

    # tracking.landing_page_url → creative.link 自动回填
    landing_url = tpl_tracking.get("landing_page_url", "")
    if landing_url and not tpl_creative.get("link"):
        tpl_creative["link"] = landing_url

    # ── web_to_app 必填校验 ──
    if template_type == "web_to_app":
        missing = []
        if not tpl_creative.get("page_id"):
            missing.append("creative.page_id")
        if not tpl_creative.get("link"):
            missing.append("creative.link (或 tracking.landing_page_url)")
        if not tpl_creative.get("primary_text"):
            missing.append("creative.primary_text")
        if not tpl_creative.get("headline"):
            missing.append("creative.headline")
        if not tpl_creative.get("image_hash") and not tpl_creative.get("video_id"):
            missing.append("creative.image_hash 或 creative.video_id (至少一个)")
        if missing:
            return {"error": f"Web-to-App 模板缺少必填字段: {', '.join(missing)}"}

    # promoted_object pixel/event 校验
    po = tpl_adset.get("promoted_object", {})
    po_pixel = po.get("pixel_id", "")
    po_event = po.get("custom_event_type", "")
    opt_goal = tpl_adset.get("optimization_goal", "")

    is_conversion = opt_goal in ("OFFSITE_CONVERSIONS", "APP_EVENTS")
    if is_conversion and (not po_pixel or not po_event):
        return {
            "error": f"optimization_goal={opt_goal} 要求 pixel_id 和 custom_event_type 必填，"
                     f"当前 pixel_id={'有' if po_pixel else '空'}, custom_event_type={'有' if po_event else '空'}"
        }
    if po_pixel and not po_event:
        return {"error": "pixel_id 已填写但 custom_event_type 为空，两者必须成对提供"}
    if po_event and not po_pixel:
        return {"error": "custom_event_type 已填写但 pixel_id 为空，两者必须成对提供"}

    # 获取该账户的专属 token（与 meta_assets 保持一致）
    _acct_row = biz_account_repository.get_by_platform_account("meta", ad_account_id)
    _acct_token = (_acct_row or {}).get("access_token", "")
    if _acct_token:
        _client = MetaClient(access_token=_acct_token)
        logger.info(f"[launch-meta] 使用账户专属 token (account={ad_account_id})")
    else:
        _client = MetaClient()
        logger.info(f"[launch-meta] 使用全局默认 token (account={ad_account_id}, 无专属 token)")

    svc_campaign = MetaCampaignService(ad_account_id, client=_client)
    svc_adset = MetaAdSetService(ad_account_id, client=_client)
    svc_ad = MetaAdService(ad_account_id, client=_client)

    # ── Step 1: Campaign (ABO — 不在 campaign 层设预算) ──
    step = "campaign"
    campaign_payload: dict = {}
    try:
        campaign_payload = {
            "name": campaign_name,
            "objective": tpl_campaign.get("objective", "OUTCOME_TRAFFIC"),
            "status": tpl_campaign.get("status", "PAUSED"),
            "special_ad_categories": json.dumps(
                tpl_campaign.get("special_ad_categories", [])
            ),
            "is_adset_budget_sharing_enabled": str(
                tpl_campaign.get("is_adset_budget_sharing_enabled", False)
            ).lower(),
        }

        safe_cp = {k: v for k, v in campaign_payload.items() if k != "access_token"}
        logger.info(
            f"[launch-meta] step={step}  type={template_type}  ad_account={ad_account_id}  "
            f"payload={safe_cp}"
        )
        campaign_data = await svc_campaign.create(campaign_payload)
        campaign_id = campaign_data.get("id", "")
        results["campaign"] = {"success": True, "campaign_id": campaign_id}
        logger.info(f"[launch-meta] Campaign 创建成功: {campaign_id}")
    except Exception as e:
        logger.error(
            f"[launch-meta] Campaign 创建失败: {e}\n"
            f"  payload_sent={safe_cp}"
        )
        results["campaign"] = {"success": False, "error": str(e), "step": step, "payload_sent": safe_cp}
        return {"data": results}

    # ── Step 2: AdSet (ABO — 预算在 adset 层) ──
    step = "adset"
    adset_payload: dict = {}
    try:
        adset_name = overrides.get("adset", {}).get("name") or f"{campaign_name}_adset"
        targeting = tpl_adset.get("targeting", {"geo_locations": {"countries": ["US"]}})
        opt_goal = tpl_adset.get("optimization_goal", "LANDING_PAGE_VIEWS")

        daily_budget = int(tpl_adset.get("daily_budget", 5000))
        if budget:
            daily_budget = int(budget * 100)

        adset_payload = {
            "campaign_id": campaign_id,
            "name": adset_name,
            "status": tpl_adset.get("status", "PAUSED"),
            "billing_event": tpl_adset.get("billing_event", "IMPRESSIONS"),
            "optimization_goal": opt_goal,
            "daily_budget": daily_budget,
            "bid_strategy": tpl_adset.get("bid_strategy", "LOWEST_COST_WITHOUT_CAP"),
            "targeting": json.dumps(targeting),
        }

        po_valid = {k: v for k, v in po.items() if v}
        if po_valid:
            adset_payload["promoted_object"] = json.dumps(po_valid)
        elif opt_goal in ("APP_INSTALLS", "APP_EVENTS", "OFFSITE_CONVERSIONS"):
            results["adset"] = {
                "success": False,
                "error": f"optimization_goal={opt_goal} 需要 promoted_object（至少包含 application_id），"
                         f"请通过 overrides.adset.promoted_object 传入",
                "step": step,
            }
            logger.error(f"[launch-meta] AdSet 校验失败: promoted_object 为空, opt_goal={opt_goal}")
            return {"data": results}

        if tpl_adset.get("start_time"):
            adset_payload["start_time"] = tpl_adset["start_time"]
        if tpl_adset.get("end_time"):
            adset_payload["end_time"] = tpl_adset["end_time"]

        safe_adset = {k: v for k, v in adset_payload.items() if k != "access_token"}
        logger.info(
            f"[launch-meta] step={step}  type={template_type}  campaign_id={campaign_id}  "
            f"payload={safe_adset}"
        )
        adset_data = await svc_adset.create(adset_payload)
        adset_id = adset_data.get("id", "")
        results["adset"] = {"success": True, "adset_id": adset_id}
        logger.info(f"[launch-meta] AdSet 创建成功: {adset_id}")
    except Exception as e:
        logger.error(
            f"[launch-meta] AdSet 创建失败: {e}\n"
            f"  payload_sent={safe_adset}"
        )
        results["adset"] = {"success": False, "error": str(e), "step": step, "payload_sent": safe_adset}
        return {"data": results}

    # ── Step 3: Ad (with inline creative) ──
    step = "ad"
    ad_payload: dict = {}
    try:
        ad_name = overrides.get("ad", {}).get("name") or f"{campaign_name}_ad"
        page_id = tpl_creative.get("page_id", "")
        video_id = tpl_creative.get("video_id", "")
        image_hash = tpl_creative.get("image_hash", "")
        link = tpl_creative.get("link", "")

        creative_spec: dict = {"object_story_spec": {}}
        if page_id:
            story_spec: dict = {"page_id": page_id}

            cta_block = {
                "type": tpl_creative.get("call_to_action", "LEARN_MORE"),
                "value": {"link": link},
            }

            if video_id:
                story_spec["video_data"] = {
                    "video_id": video_id,
                    "message": tpl_creative.get("primary_text", ""),
                    "title": tpl_creative.get("headline", ""),
                    "link_description": tpl_creative.get("description", ""),
                    "call_to_action": cta_block,
                }
            elif image_hash:
                story_spec["link_data"] = {
                    "image_hash": image_hash,
                    "message": tpl_creative.get("primary_text", ""),
                    "name": tpl_creative.get("headline", ""),
                    "description": tpl_creative.get("description", ""),
                    "link": link,
                    "call_to_action": cta_block,
                }

            creative_spec["object_story_spec"] = story_spec

        ad_payload = {
            "name": ad_name,
            "adset_id": adset_id,
            "status": tpl_ad.get("status", "PAUSED"),
            "creative": json.dumps(creative_spec),
        }

        # tracking 字段透传到 ad 层
        tracking_url = tpl_tracking.get("landing_page_url", "")
        if tracking_url:
            ad_payload["tracking_specs"] = json.dumps([{"action.type": ["offsite_conversion"], "fb_pixel": [po_pixel]}]) if po_pixel else ""

        logger.info(
            f"[launch-meta] step={step}  type={template_type}  adset_id={adset_id}  ad_name={ad_name}  "
            f"has_page_id={bool(page_id)}  has_video={bool(video_id)}  has_image={bool(image_hash)}  link={link[:80] if link else ''}"
        )
        ad_data = await svc_ad.create(ad_payload)
        ext_ad_id = ad_data.get("id", "")
        results["ad"] = {"success": True, "ad_id": ext_ad_id}
        logger.info(f"[launch-meta] Ad 创建成功: {ext_ad_id}")
    except Exception as e:
        logger.error(f"[launch-meta] Ad 创建失败: {e}")
        safe_ap = {k: v for k, v in ad_payload.items() if k != "access_token"}
        results["ad"] = {"success": False, "error": str(e), "step": step, "payload_sent": safe_ap}
        return {"data": results}

    return {"data": results}


def _deep_merge(base: dict, overrides: dict) -> dict:
    """浅层合并 overrides 到 base 的副本，overrides 中非 None 的字段优先。"""
    merged = dict(base)
    for k, v in overrides.items():
        if isinstance(v, dict) and isinstance(merged.get(k), dict):
            merged[k] = _deep_merge(merged[k], v)
        elif v is not None:
            merged[k] = v
    return merged


# ══════════════════════════════════════════════════════════
#  TikTok 模板投放（原逻辑保持不变）
# ══════════════════════════════════════════════════════════

async def _launch_tiktok(tpl: dict, payload: dict) -> dict:
    advertiser_id = payload.get("advertiser_id", "")
    campaign_name = payload.get("campaign_name", "")
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
    results = {"platform": "tiktok", "campaign": None, "adgroup": None, "ads": [], "summary": {"total": 0, "success": 0, "fail": 0}}

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


# ── 日志辅助函数 ──────────────────────────────────────────

_SENSITIVE_KEYS = {"access_token", "app_secret", "password", "cookie", "secret"}


def _tpl_summary(tpl: dict | None) -> dict | None:
    """提取模板摘要用于日志存储，排除敏感字段和超大内容。"""
    if not tpl:
        return None
    summary: dict = {}
    for k, v in tpl.items():
        if k in _SENSITIVE_KEYS:
            continue
        if isinstance(v, dict):
            summary[k] = {ik: iv for ik, iv in v.items() if ik not in _SENSITIVE_KEYS}
        elif isinstance(v, str) and len(v) > 500:
            summary[k] = v[:500] + "..."
        else:
            summary[k] = v
    return summary


def _diff_fields(old: dict, new: dict, prefix: str = "") -> list[str]:
    """对比两个字典，返回变更字段列表（支持一层嵌套）。"""
    changed: list[str] = []
    all_keys = set(old.keys()) | set(new.keys())
    skip = {"updated_at", "created_at"}
    for k in sorted(all_keys):
        if k in skip:
            continue
        full_key = f"{prefix}{k}" if not prefix else f"{prefix}.{k}"
        ov, nv = old.get(k), new.get(k)
        if isinstance(ov, dict) and isinstance(nv, dict):
            changed.extend(_diff_fields(ov, nv, full_key))
        elif ov != nv:
            changed.append(full_key)
    return changed


def _log_launch(user, tpl_id: str, platform: str, payload: dict, resp: dict):
    """记录模板投放操作日志。"""
    data = resp.get("data", {})
    error = resp.get("error")
    template_type = data.get("template_type", "") if isinstance(data, dict) else ""

    campaign_name = payload.get("campaign_name", "")
    ad_account_id = payload.get("ad_account_id", "")
    budget = payload.get("budget", "")

    if error:
        status = "fail"
        detail = f"template={tpl_id} | platform={platform} | error: {error}"
    else:
        failed_step = ""
        for step_name in ("campaign", "adset", "adgroup", "ad"):
            step_data = data.get(step_name)
            if isinstance(step_data, dict) and not step_data.get("success"):
                failed_step = step_name
                break

        if failed_step:
            step_err = data.get(failed_step, {}).get("error", "unknown")
            status = "fail"
            detail = f"template={tpl_id} | platform={platform} | type={template_type} | failed_step={failed_step} | error: {step_err}"
        else:
            status = "success"
            ids = []
            for k in ("campaign", "adset", "adgroup", "ad"):
                v = data.get(k, {})
                if isinstance(v, dict):
                    for id_key in ("campaign_id", "adset_id", "adgroup_id", "ad_id"):
                        if v.get(id_key):
                            ids.append(f"{k}={v[id_key]}")
            detail = f"template={tpl_id} | platform={platform} | type={template_type} | {', '.join(ids)}"

    if ad_account_id:
        detail += f" | account={ad_account_id}"

    after_summary: dict = {
        "template_id": tpl_id,
        "template_type": template_type,
        "platform": platform,
        "campaign_name": campaign_name,
        "budget": budget,
    }
    if ad_account_id:
        after_summary["ad_account_id"] = ad_account_id

    overrides = payload.get("overrides", {})
    ov_campaign = overrides.get("campaign", {})
    ov_creative = overrides.get("creative", {})
    ov_adset = overrides.get("adset", {})
    ov_po = ov_adset.get("promoted_object", {})
    after_summary["campaign_objective"] = ov_campaign.get("objective", "")
    after_summary["optimization_goal"] = ov_adset.get("optimization_goal", "")
    after_summary["creative_link"] = ov_creative.get("link", "")[:100]
    after_summary["has_image_hash"] = bool(ov_creative.get("image_hash"))
    after_summary["has_video_id"] = bool(ov_creative.get("video_id"))
    after_summary["has_pixel_id"] = bool(ov_po.get("pixel_id"))
    after_summary["pixel_id"] = ov_po.get("pixel_id", "")
    after_summary["custom_event_type"] = ov_po.get("custom_event_type", "")

    if status == "fail":
        for step_key in ("campaign", "adset", "adgroup", "ad"):
            step_data = data.get(step_key, {})
            if isinstance(step_data, dict) and step_data.get("payload_sent"):
                after_summary["failed_payload"] = step_data["payload_sent"]
                break

    log_operation(
        username=user.username,
        action="使用模板创建广告",
        target_type="template_launch",
        target_id=tpl_id,
        platform=platform,
        status=status,
        after_data=after_summary,
        error_message=detail,
    )
