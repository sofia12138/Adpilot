"""补充创建剩余2条广告"""

import httpx
import asyncio
import json
import time


async def create_remaining():
    access_token = "214402722754f2bd1c2e90872ac17ab7f0f7cf2d"
    advertiser_id = "7602627489392394248"
    adgroup_id = "1859167991452849"
    base_api = "https://business-api.tiktok.com/open_api/v1.3"
    headers = {"Access-Token": access_token, "Content-Type": "application/json"}

    identity_id = "b6014b97-022e-5fd3-af17-9b47ab071d4f"
    identity_type = "BC_AUTH_TT"
    identity_authorized_bc_id = "7589933029276155920"

    remaining = [
        ("v10033g50000d621e97og65inbp50s0g", "Reborn-Ep02"),
        ("v10033g50000d621e77og65umlps7rcg", "Reborn-Ep09"),
    ]

    async with httpx.AsyncClient(timeout=60.0) as c:
        for video_id, name in remaining:
            print(f"=== 处理 {name} ===")

            # 获取封面
            r = await c.get(
                f"{base_api}/file/video/ad/info/",
                headers=headers,
                params={
                    "advertiser_id": advertiser_id,
                    "video_ids": json.dumps([video_id]),
                },
            )
            cover_url = r.json()["data"]["list"][0].get("video_cover_url", "")
            if cover_url.startswith("http://"):
                cover_url = "https://" + cover_url[7:]

            # 上传封面（用唯一文件名）
            ts = int(time.time())
            r = await c.post(
                f"{base_api}/file/image/ad/upload/",
                headers=headers,
                json={
                    "advertiser_id": advertiser_id,
                    "upload_type": "UPLOAD_BY_URL",
                    "image_url": cover_url,
                    "file_name": f"cover-{name}-{ts}.jpg",
                },
            )
            img_data = r.json()
            if img_data.get("code") != 0:
                print(f"  封面上传失败: {img_data.get('message')}")
                continue
            image_id = img_data["data"].get("image_id", "")
            print(f"  封面: {image_id}")

            if not image_id:
                print(f"  image_id 为空，跳过")
                continue

            # 创建广告
            payload = {
                "advertiser_id": advertiser_id,
                "adgroup_id": adgroup_id,
                "creatives": [{
                    "ad_name": f"Ad-{name}-Install-0309",
                    "ad_format": "SINGLE_VIDEO",
                    "video_id": video_id,
                    "image_ids": [image_id],
                    "ad_text": "Download now and start watching!",
                    "identity_id": identity_id,
                    "identity_type": identity_type,
                    "identity_authorized_bc_id": identity_authorized_bc_id,
                    "call_to_action": "INSTALL_NOW",
                }],
            }
            r = await c.post(f"{base_api}/ad/create/", headers=headers, json=payload)
            data = r.json()
            if data.get("code") == 0:
                print(f"  广告创建成功! ID: {data['data'].get('ad_ids')}")
            else:
                print(f"  广告创建失败: {data.get('message')}")
            print()

        # 最终确认
        print("=== 最终广告列表 ===")
        r = await c.get(
            f"{base_api}/ad/get/",
            headers=headers,
            params={
                "advertiser_id": advertiser_id,
                "filtering": json.dumps({"adgroup_ids": [adgroup_id]}),
            },
        )
        data = r.json()
        if data.get("code") == 0:
            ads = data["data"].get("list", [])
            total = data["data"].get("page_info", {}).get("total_number", 0)
            print(f"  共 {total} 条广告:")
            for ad in ads:
                print(f"  [{ad.get('operation_status')}] {ad.get('ad_name')} (ID: {ad.get('ad_id')})")


asyncio.run(create_remaining())
