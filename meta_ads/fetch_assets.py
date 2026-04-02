"""拉取 Meta 已授权资产（广告账户、Pixel、App、主页）"""

import asyncio
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from meta_ads.api.client import MetaClient


async def fetch_all_assets():
    client = MetaClient()

    print("=" * 60)
    print("  Meta 已授权资产列表")
    print("=" * 60)

    # 1. 广告账户
    print("\n[1] 广告账户 (Ad Accounts)")
    print("-" * 40)
    try:
        data = await client.get("me/adaccounts", {
            "fields": "account_id,name,account_status,currency,timezone_name",
            "limit": 50,
        })
        for acc in data.get("data", []):
            status_map = {1: "ACTIVE", 2: "DISABLED", 3: "UNSETTLED", 7: "PENDING_RISK_REVIEW"}
            status = status_map.get(acc.get("account_status"), str(acc.get("account_status")))
            print(f"  ID: {acc['id']}")
            print(f"  名称: {acc.get('name', 'N/A')}")
            print(f"  状态: {status} | 币种: {acc.get('currency', 'N/A')} | 时区: {acc.get('timezone_name', 'N/A')}")
            print()
    except Exception as e:
        print(f"  拉取失败: {e}\n")

    # 2. Pixel
    from config import get_settings
    ad_account_id = get_settings().meta_ad_account_id

    print("[2] Pixel (数据源)")
    print("-" * 40)
    try:
        data = await client.get(f"{ad_account_id}/adspixels", {
            "fields": "id,name,last_fired_time,is_created_by_business",
        })
        pixels = data.get("data", [])
        if not pixels:
            print("  (无可用 Pixel)")
        for px in pixels:
            print(f"  ID: {px['id']}")
            print(f"  名称: {px.get('name', 'N/A')}")
            print(f"  最后触发: {px.get('last_fired_time', 'N/A')}")
            print()
    except Exception as e:
        print(f"  拉取失败: {e}\n")

    # 3. 应用 (Apps)
    print("[3] 应用 (Apps)")
    print("-" * 40)
    try:
        data = await client.get(f"{ad_account_id}/advertisable_applications", {
            "fields": "id,name,logo_url,supported_platforms,object_store_urls",
        })
        apps = data.get("data", [])
        if not apps:
            print("  (无可用应用)")
        for app in apps:
            print(f"  App ID: {app['id']}")
            print(f"  名称: {app.get('name', 'N/A')}")
            platforms = app.get("supported_platforms", [])
            if platforms:
                print(f"  平台: {', '.join(platforms)}")
            store_urls = app.get("object_store_urls", {})
            for platform, url in store_urls.items():
                print(f"  {platform}: {url}")
            print()
    except Exception as e:
        print(f"  拉取失败: {e}\n")

    # 4. 主页 (Pages)
    print("[4] 主页 (Pages)")
    print("-" * 40)
    try:
        data = await client.get("me/accounts", {
            "fields": "id,name,category,access_token",
        })
        pages = data.get("data", [])
        if not pages:
            print("  (无可用主页)")
        for page in pages:
            print(f"  Page ID: {page['id']}")
            print(f"  名称: {page.get('name', 'N/A')}")
            print(f"  分类: {page.get('category', 'N/A')}")
            print()
    except Exception as e:
        print(f"  拉取失败: {e}\n")

    # 5. Instagram 账户
    print("[5] Instagram 账户")
    print("-" * 40)
    try:
        data = await client.get(f"{ad_account_id}/instagram_accounts", {
            "fields": "id,username,profile_pic",
        })
        ig_accounts = data.get("data", [])
        if not ig_accounts:
            print("  (无关联 Instagram 账户)")
        for ig in ig_accounts:
            print(f"  IG ID: {ig['id']}")
            print(f"  用户名: {ig.get('username', 'N/A')}")
            print()
    except Exception as e:
        print(f"  拉取失败: {e}\n")

    await client.close()
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(fetch_all_assets())
