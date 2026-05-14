"""按剧筛选 SQL 片段助手 — 供 素材分析 / 设计师人效 等接口复用

数据桥梁：
  ad_drama_mapping（platform / account_id / campaign_id 唯一键，per campaign 一行）
    → 每条 campaign 解析出 content_key / localized_drama_name / language_code / drama_id

主表对接：
  - normalized 系（biz_ad_daily_normalized 等）：platform=tiktok/meta、account_id 与 mapping 完全一致
  - attribution 系（biz_attribution_ad_daily 等）：
      platform=facebook → mapping.platform=meta（CASE 映射）
      account_id Meta 无 'act_' 前缀 → 需要 CONCAT('act_', account_id)

约束：
  - JOIN 使用 LEFT JOIN，不命中时各剧字段返回 NULL，保持原查询不漏行
  - aggregate 查询里访问 m.* 必须用 MAX() 包裹（campaign_id → mapping 一对一保证 MAX 无歧义）
"""
from __future__ import annotations

from typing import Optional


def drama_join_for_normalized(main_alias: str = "n", mapping_alias: str = "m") -> str:
    """对接 normalized 主表（platform、account_id 与 ad_drama_mapping 一致）。"""
    return (
        f" LEFT JOIN ad_drama_mapping {mapping_alias} "
        f"ON {mapping_alias}.platform    = {main_alias}.platform "
        f"AND {mapping_alias}.account_id = {main_alias}.account_id "
        f"AND {mapping_alias}.campaign_id = {main_alias}.campaign_id"
    )


def drama_join_for_attribution(main_alias: str = "a", mapping_alias: str = "m") -> str:
    """对接 attribution 主表（platform=facebook/tiktok，account_id Meta 无 'act_' 前缀）。"""
    plat_expr = (
        f"CASE WHEN {main_alias}.platform = 'facebook' THEN 'meta' "
        f"ELSE {main_alias}.platform END"
    )
    acc_expr = (
        f"CASE WHEN {main_alias}.platform = 'facebook' "
        f"AND {main_alias}.account_id NOT LIKE 'act\\_%%' "
        f"THEN CONCAT('act_', {main_alias}.account_id) "
        f"ELSE {main_alias}.account_id END"
    )
    return (
        f" LEFT JOIN ad_drama_mapping {mapping_alias} "
        f"ON {mapping_alias}.platform    = {plat_expr} "
        f"AND {mapping_alias}.account_id = {acc_expr} "
        f"AND {mapping_alias}.campaign_id = {main_alias}.campaign_id"
    )


def drama_filter_where(
    *,
    content_key: Optional[str] = None,
    drama_keyword: Optional[str] = None,
    language_code: Optional[str] = None,
    mapping_alias: str = "m",
) -> tuple[str, list]:
    """生成剧筛选 WHERE 片段（含前导 ' AND '，可直接拼接到已有 WHERE 后面）。

    返回 (sql_fragment, args)。所有条件都为空时返回 ('', [])。
    """
    parts: list[str] = []
    args: list = []
    if content_key:
        parts.append(f"{mapping_alias}.content_key = %s")
        args.append(content_key)
    if drama_keyword:
        parts.append(f"{mapping_alias}.localized_drama_name LIKE %s")
        args.append(f"%{drama_keyword}%")
    if language_code:
        parts.append(f"{mapping_alias}.language_code = %s")
        args.append(language_code)
    if not parts:
        return "", []
    return " AND " + " AND ".join(parts), args


def drama_select_fields(mapping_alias: str = "m", *, aggregate: bool = True) -> str:
    """在 SELECT 子句中追加剧维度字段。

    - aggregate=True：用 MAX() 包裹，适用于 GROUP BY 查询
    - aggregate=False：裸字段，适用于单行查询
    """
    if aggregate:
        return (
            f"MAX({mapping_alias}.content_key)          AS content_key, "
            f"MAX({mapping_alias}.localized_drama_name) AS localized_drama_name, "
            f"MAX({mapping_alias}.language_code)        AS language_code, "
            f"MAX({mapping_alias}.drama_id)             AS drama_id"
        )
    return (
        f"{mapping_alias}.content_key          AS content_key, "
        f"{mapping_alias}.localized_drama_name AS localized_drama_name, "
        f"{mapping_alias}.language_code        AS language_code, "
        f"{mapping_alias}.drama_id             AS drama_id"
    )
