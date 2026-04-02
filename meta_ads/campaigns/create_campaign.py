"""
一键创建广告：读取 YAML 配置 → 依次创建 Campaign → Ad Set → Ad

用法:
    python -m meta_ads.campaigns.create_campaign campaign_20260310.yaml
    python -m meta_ads.campaigns.create_campaign campaign_20260310.yaml --dry-run
"""

import asyncio
import argparse
import json
import sys
import os

import yaml

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from meta_ads.api.client import MetaClient, MetaApiError


def load_config(filepath: str) -> dict:
    with open(filepath, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def validate_config(cfg: dict) -> list[str]:
    """校验必填字段，返回缺失项列表"""
    errors = []

    if not cfg.get("ad_account_id"):
        errors.append("ad_account_id")

    campaign = cfg.get("campaign", {})
    if not campaign.get("name"):
        errors.append("campaign.name")

    adset = cfg.get("adset", {})
    if not adset.get("name"):
        errors.append("adset.name")
    if not adset.get("pixel_id"):
        errors.append("adset.pixel_id")
    if not adset.get("app_id"):
        errors.append("adset.app_id")

    ad = cfg.get("ad", {})
    if not ad.get("name"):
        errors.append("ad.name")
    if not ad.get("media_url"):
        errors.append("ad.media_url")
    if not ad.get("primary_text"):
        errors.append("ad.primary_text")
    if not ad.get("headline"):
        errors.append("ad.headline")
    if not ad.get("website_url"):
        errors.append("ad.website_url")
    if not ad.get("page_id"):
        errors.append("ad.page_id")

    return errors


def build_campaign_payload(cfg: dict) -> dict:
    c = cfg["campaign"]
    payload = {
        "name": c["name"],
        "objective": c.get("objective", "OUTCOME_SALES"),
        "status": "PAUSED",
        "special_ad_categories": "[]",
    }
    bid = c.get("bid_strategy", "LOWEST_COST_WITHOUT_CAP")
    if bid != "LOWEST_COST_WITHOUT_CAP":
        payload["bid_strategy"] = bid
    return payload


def build_adset_payload(cfg: dict, campaign_id: str) -> dict:
    a = cfg["adset"]
    c = cfg["campaign"]

    targeting = {"geo_locations": {}}
    t = a.get("targeting", {})
    countries = t.get("countries", ["US"])
    if countries:
        targeting["geo_locations"]["countries"] = countries
    if t.get("age_min"):
        targeting["age_min"] = t["age_min"]
    if t.get("age_max"):
        targeting["age_max"] = t["age_max"]
    gender = t.get("gender", "all")
    if gender == "male":
        targeting["genders"] = [1]
    elif gender == "female":
        targeting["genders"] = [2]

    interests = t.get("interests", [])
    if interests:
        targeting["flexible_spec"] = [{"interests": [{"id": str(i)} for i in interests]}]

    custom_audiences = t.get("custom_audiences", [])
    if custom_audiences:
        targeting["custom_audiences"] = [{"id": str(ca)} for ca in custom_audiences]

    excluded = t.get("excluded_audiences", [])
    if excluded:
        targeting["excluded_custom_audiences"] = [{"id": str(e)} for e in excluded]

    payload = {
        "campaign_id": campaign_id,
        "name": a["name"],
        "status": "PAUSED",
        "billing_event": "IMPRESSIONS",
        "optimization_goal": "OFFSITE_CONVERSIONS",
        "targeting": json.dumps(targeting),
        "promoted_object": json.dumps({
            "pixel_id": str(a["pixel_id"]),
            "custom_event_type": a.get("conversion_event", "PURCHASE"),
            "application_id": str(a["app_id"]),
            "object_store_url": a.get("google_play_url", ""),
        }),
    }

    budget_type = c.get("budget_type", "daily")
    amount = int(float(c.get("budget_amount", 100)) * 100)
    if budget_type == "daily":
        payload["daily_budget"] = amount
    else:
        payload["lifetime_budget"] = amount

    if a.get("start_time"):
        payload["start_time"] = a["start_time"]
    if a.get("end_time"):
        payload["end_time"] = a["end_time"]

    return payload


def build_ad_payload(cfg: dict, adset_id: str) -> dict:
    ad = cfg["ad"]

    creative = {
        "object_story_spec": {
            "page_id": str(ad["page_id"]),
            "video_data": {
                "video_id": "",
                "image_url": ad.get("thumbnail_url", ""),
                "title": ad.get("headline", ""),
                "message": ad.get("primary_text", ""),
                "link_description": ad.get("description", ""),
                "call_to_action": {
                    "type": ad.get("cta", "INSTALL_NOW"),
                    "value": {
                        "link": ad.get("website_url", ""),
                    },
                },
            },
        },
    }

    if ad.get("deep_link_ios"):
        creative["object_story_spec"]["video_data"]["call_to_action"]["value"]["app_link"] = ad["deep_link_ios"]
    if ad.get("deep_link_android"):
        creative["object_story_spec"]["video_data"]["call_to_action"]["value"]["app_link"] = ad["deep_link_android"]

    if ad.get("instagram_id"):
        creative["object_story_spec"]["instagram_actor_id"] = str(ad["instagram_id"])

    payload = {
        "name": ad["name"],
        "adset_id": adset_id,
        "status": "PAUSED",
        "creative": json.dumps(creative),
    }

    return payload


async def upload_video(client: MetaClient, ad_account_id: str, media_url: str) -> str:
    """上传视频素材，返回 video_id"""
    if media_url.startswith("http"):
        data = await client.post(f"{ad_account_id}/advideos", {"file_url": media_url})
    else:
        raise ValueError(f"暂不支持本地文件上传，请提供视频 URL: {media_url}")
    return data.get("id", "")


async def create_all(cfg: dict, dry_run: bool = False):
    ad_account_id = cfg["ad_account_id"]
    client = MetaClient()

    print("=" * 50)
    print("  Meta 广告创建")
    print("=" * 50)

    # 1. Campaign
    campaign_payload = build_campaign_payload(cfg)
    print(f"\n[1/3] Campaign: {campaign_payload['name']}")
    if dry_run:
        print(f"  (dry-run) payload: {json.dumps(campaign_payload, ensure_ascii=False, indent=2)}")
        campaign_id = "FAKE_CAMPAIGN_ID"
    else:
        result = await client.post(f"{ad_account_id}/campaigns", campaign_payload)
        campaign_id = result["id"]
        print(f"  -> 创建成功, ID: {campaign_id}")

    # 2. Ad Set
    adset_payload = build_adset_payload(cfg, campaign_id)
    print(f"\n[2/3] Ad Set: {adset_payload['name']}")
    if dry_run:
        print(f"  (dry-run) payload: {json.dumps(adset_payload, ensure_ascii=False, indent=2)}")
        adset_id = "FAKE_ADSET_ID"
    else:
        result = await client.post(f"{ad_account_id}/adsets", adset_payload)
        adset_id = result["id"]
        print(f"  -> 创建成功, ID: {adset_id}")

    # 3. Upload video & create Ad
    ad_cfg = cfg["ad"]
    media_url = ad_cfg.get("media_url", "")

    if media_url and ad_cfg.get("creative_type") == "video":
        print(f"\n[*] 上传视频素材...")
        if dry_run:
            print(f"  (dry-run) 视频 URL: {media_url}")
            video_id = "FAKE_VIDEO_ID"
        else:
            video_id = await upload_video(client, ad_account_id, media_url)
            print(f"  -> 上传成功, video_id: {video_id}")
    else:
        video_id = ""

    ad_payload = build_ad_payload(cfg, adset_id)
    if video_id:
        creative = json.loads(ad_payload["creative"])
        creative["object_story_spec"]["video_data"]["video_id"] = video_id
        ad_payload["creative"] = json.dumps(creative)

    print(f"\n[3/3] Ad: {cfg['ad']['name']}")
    if dry_run:
        print(f"  (dry-run) payload: {json.dumps(ad_payload, ensure_ascii=False, indent=2)}")
    else:
        result = await client.post(f"{ad_account_id}/ads", ad_payload)
        ad_id = result["id"]
        print(f"  -> 创建成功, ID: {ad_id}")

    await client.close()

    print("\n" + "=" * 50)
    if dry_run:
        print("  DRY-RUN 完成 (未实际创建)")
    else:
        print("  全部创建完成! 广告状态为 PAUSED，确认无误后手动开启。")
    print("=" * 50)


async def main():
    parser = argparse.ArgumentParser(description="从 YAML 配置创建 Meta 广告")
    parser.add_argument("config_file", help="YAML 配置文件路径")
    parser.add_argument("--dry-run", action="store_true", help="仅预览 payload，不实际创建")
    args = parser.parse_args()

    campaigns_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = args.config_file
    if not os.path.isabs(config_path):
        config_path = os.path.join(campaigns_dir, config_path)

    if not os.path.exists(config_path):
        print(f"文件不存在: {config_path}")
        sys.exit(1)

    cfg = load_config(config_path)

    errors = validate_config(cfg)
    if errors:
        print("配置校验失败，以下必填字段为空:")
        for e in errors:
            print(f"  - {e}")
        sys.exit(1)

    await create_all(cfg, dry_run=args.dry_run)


if __name__ == "__main__":
    asyncio.run(main())
