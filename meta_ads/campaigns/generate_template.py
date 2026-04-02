"""
模板生成器：自动从 Meta API 拉取已授权资产，生成预填好的 YAML 配置文件。

用法:
    python -m meta_ads.campaigns.generate_template
    python -m meta_ads.campaigns.generate_template --output my_campaign.yaml
"""

import asyncio
import argparse
import sys
import os
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from meta_ads.api.client import MetaClient
from config import get_settings


async def fetch_assets() -> dict:
    """从 API 拉取所有可用资产"""
    client = MetaClient()
    settings = get_settings()
    ad_account_id = settings.meta_ad_account_id
    assets = {}

    try:
        data = await client.get("me/adaccounts", {
            "fields": "account_id,name,account_status,currency,timezone_name",
            "limit": 50,
        })
        assets["ad_accounts"] = [
            a for a in data.get("data", []) if a.get("account_status") == 1
        ]
    except Exception:
        assets["ad_accounts"] = []

    try:
        data = await client.get(f"{ad_account_id}/adspixels", {
            "fields": "id,name,last_fired_time",
        })
        assets["pixels"] = data.get("data", [])
    except Exception:
        assets["pixels"] = []

    try:
        data = await client.get(f"{ad_account_id}/advertisable_applications", {
            "fields": "id,name,supported_platforms,object_store_urls",
        })
        assets["apps"] = data.get("data", [])
    except Exception:
        assets["apps"] = []

    try:
        data = await client.get("me/accounts", {
            "fields": "id,name,category",
        })
        assets["pages"] = data.get("data", [])
    except Exception:
        assets["pages"] = []

    try:
        data = await client.get(f"{ad_account_id}/instagram_accounts", {
            "fields": "id,username",
        })
        assets["instagram"] = data.get("data", [])
    except Exception:
        assets["instagram"] = []

    await client.close()
    return assets


def build_asset_comments(assets: dict) -> dict:
    """把资产列表转成 YAML 注释文本"""
    comments = {}

    lines = []
    for a in assets["ad_accounts"]:
        lines.append(f"#   - {a['id']}  ({a.get('name', '')} | {a.get('currency', '')} | {a.get('timezone_name', '')})")
    comments["ad_accounts"] = "\n".join(lines) if lines else "#   (无可用账户)"

    lines = []
    for p in assets["pixels"]:
        lines.append(f"#   - {p['id']}  ({p.get('name', '')})")
    comments["pixels"] = "\n".join(lines) if lines else "#   (无可用 Pixel)"

    lines = []
    for app in assets["apps"]:
        platforms = ", ".join(app.get("supported_platforms", []))
        store = app.get("object_store_urls", {})
        gp = store.get("google_play", "")
        ios_url = store.get("itunes", "")
        detail = f"{app.get('name', '')} | {platforms}"
        if gp:
            detail += f" | GP: {gp}"
        if ios_url:
            detail += f" | iOS: {ios_url}"
        lines.append(f"#   - {app['id']}  ({detail})")
    comments["apps"] = "\n".join(lines) if lines else "#   (无可用应用)"

    lines = []
    for pg in assets["pages"]:
        lines.append(f"#   - {pg['id']}  ({pg.get('name', '')} | {pg.get('category', '')})")
    comments["pages"] = "\n".join(lines) if lines else "#   (无可用主页)"

    lines = []
    for ig in assets["instagram"]:
        lines.append(f"#   - {ig['id']}  (@{ig.get('username', '')})")
    comments["instagram"] = "\n".join(lines) if lines else "#   (无关联 IG 账户)"

    return comments


def pick_defaults(assets: dict) -> dict:
    """自动选取默认资产 ID（取第一个可用的）"""
    defaults = {}
    if assets["ad_accounts"]:
        defaults["ad_account_id"] = assets["ad_accounts"][0]["id"]
    if assets["pixels"]:
        defaults["pixel_id"] = assets["pixels"][0]["id"]
    if assets["apps"]:
        app = assets["apps"][0]
        defaults["app_id"] = app["id"]
        store = app.get("object_store_urls", {})
        defaults["google_play_url"] = store.get("google_play", "")
        defaults["ios_url"] = store.get("itunes", "")
    if assets["pages"]:
        defaults["page_id"] = assets["pages"][0]["id"]
    if assets["instagram"]:
        defaults["instagram_id"] = assets["instagram"][0]["id"]
    return defaults


def generate_yaml(assets: dict) -> str:
    comments = build_asset_comments(assets)
    defaults = pick_defaults(assets)
    tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00")

    return f"""# ============================================
# Meta Web-to-App 广告配置
# 由 generate_template.py 自动生成
# 生成时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
# ============================================

# -------------------- 账户 --------------------
# 可用广告账户:
{comments["ad_accounts"]}
ad_account_id: "{defaults.get("ad_account_id", "")}"

# -------------------- Campaign --------------------
campaign:
  name: ""                          # 必填: 广告系列名称
  objective: "OUTCOME_SALES"        # OUTCOME_SALES / OUTCOME_APP_PROMOTION / OUTCOME_LEADS
  budget_type: "daily"              # daily / lifetime
  budget_amount: 100                # 预算金额 (USD)
  bid_strategy: "LOWEST_COST_WITHOUT_CAP"
  # cost_cap_amount: 10             # 仅 bid_strategy=COST_CAP 时填写

# -------------------- Ad Set --------------------
adset:
  name: ""                          # 必填: 广告组名称
  start_time: "{tomorrow}"
  end_time: ""                      # 留空 = 持续投放

  # -- Pixel (转化追踪) --
  # 可用 Pixel:
{comments["pixels"]}
  pixel_id: "{defaults.get("pixel_id", "")}"
  conversion_event: "PURCHASE"      # PURCHASE / ADD_TO_CART / INITIATE_CHECKOUT / COMPLETE_REGISTRATION

  # -- 应用 --
  # 可用应用:
{comments["apps"]}
  app_id: "{defaults.get("app_id", "")}"
  google_play_url: "{defaults.get("google_play_url", "")}"
  ios_url: "{defaults.get("ios_url", "")}"

  # -- 受众定向 --
  targeting:
    countries: ["US"]               # 国家代码列表
    age_min: 18
    age_max: 65
    gender: "all"                   # all / male / female
    interests: []                   # 兴趣 ID 列表
    custom_audiences: []            # 自定义受众 ID 列表
    excluded_audiences: []          # 排除受众 ID 列表

  placement: "auto"                 # auto / manual
  # manual_placements: ["facebook_feed", "instagram_feed", "instagram_stories", "instagram_reels"]

# -------------------- Ad --------------------
ad:
  name: ""                          # 必填: 广告名称

  # -- 发布身份 --
  # 可用主页:
{comments["pages"]}
  page_id: "{defaults.get("page_id", "")}"
  # 可用 Instagram:
{comments["instagram"]}
  instagram_id: "{defaults.get("instagram_id", "")}"

  # -- 素材 --
  creative_type: "video"            # image / video / carousel
  media_url: ""                     # 必填: 视频/图片 URL 或本地文件路径
  thumbnail_url: ""                 # 视频封面图 (可选)

  # -- 文案 --
  primary_text: ""                  # 必填: 广告主文案
  headline: ""                      # 必填: 标题
  description: ""                   # 描述 (可选)

  # -- 链接 --
  website_url: ""                   # 必填: 落地页 URL
  deep_link_ios: ""                 # iOS 深度链接 (可选)
  deep_link_android: ""             # Android 深度链接 (可选)

  cta: "INSTALL_NOW"                # SHOP_NOW / INSTALL_NOW / LEARN_MORE / SIGN_UP / ORDER_NOW
"""


async def main():
    parser = argparse.ArgumentParser(description="生成 Meta 广告配置模板")
    parser.add_argument("--output", "-o", default=None,
                        help="输出文件名 (默认: campaign_<日期>.yaml)")
    args = parser.parse_args()

    print("正在从 Meta API 拉取已授权资产...")
    assets = await fetch_assets()

    accounts = len(assets["ad_accounts"])
    pixels = len(assets["pixels"])
    apps = len(assets["apps"])
    pages = len(assets["pages"])
    print(f"已获取: {accounts} 个账户, {pixels} 个 Pixel, {apps} 个应用, {pages} 个主页")

    yaml_content = generate_yaml(assets)

    if args.output:
        filename = args.output
    else:
        filename = f"campaign_{datetime.now().strftime('%Y%m%d_%H%M%S')}.yaml"

    campaigns_dir = os.path.dirname(os.path.abspath(__file__))
    filepath = os.path.join(campaigns_dir, filename)

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(yaml_content)

    print(f"\n模板已生成: {filepath}")
    print("请编辑文件填写: name, media_url, primary_text, headline, website_url")
    print("填写完成后运行: python -m meta_ads.campaigns.create_campaign <文件名>")


if __name__ == "__main__":
    asyncio.run(main())
