# -*- coding: utf-8 -*-
"""三库隔离数据库层 — PRD(产研/只读) / APP(应用/读写) / BIZ(业务/读写)"""
from __future__ import annotations

import json
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Optional

import pymysql
import pymysql.cursors
from loguru import logger

from config import get_settings


class DatabaseUnavailableError(Exception):
    """MySQL 连接不可用时抛出，由全局异常处理器捕获返回 503"""
    pass


# ═══════════════════════════════════════════════════════════
#  三库连接池（完全隔离）
# ═══════════════════════════════════════════════════════════

_POOL_SIZE = 5

_prd_pool: list[pymysql.Connection] = []
_app_pool: list[pymysql.Connection] = []
_biz_pool: list[pymysql.Connection] = []

_prd_available: bool | None = None


# ── 安全校验 ──────────────────────────────────────────────

def validate_db_config():
    """启动时调用，校验三库配置不冲突。"""
    settings = get_settings()
    prd_db = _resolve_prd_database(settings)

    app_db = settings.app_mysql_database
    if not app_db:
        raise RuntimeError("APP_MYSQL_DATABASE 未配置，请设置应用库数据库名")
    if app_db == prd_db:
        raise RuntimeError(
            f"APP_MYSQL_DATABASE('{app_db}') 不能指向产研库 '{prd_db}'，请配置独立数据库"
        )

    biz_db = settings.biz_mysql_database
    if biz_db:
        if biz_db == prd_db:
            raise RuntimeError(
                f"BIZ_MYSQL_DATABASE('{biz_db}') 不能指向产研库 '{prd_db}'，请配置独立数据库"
            )
        if biz_db == app_db:
            raise RuntimeError(
                f"BIZ_MYSQL_DATABASE('{biz_db}') 不能与 APP_MYSQL_DATABASE('{app_db}') 相同"
            )

    logger.info(f"数据库配置校验通过: PRD={prd_db}, APP={app_db}, BIZ={biz_db or '(未配置)'}")


def _resolve_prd_database(settings) -> str:
    return settings.prd_mysql_database or settings.mysql_database


# ── 连接创建 ──────────────────────────────────────────────

def _create_prd_conn() -> pymysql.Connection:
    """创建产研库连接（只读）"""
    settings = get_settings()
    return pymysql.connect(
        host=settings.prd_mysql_host or settings.mysql_host,
        port=settings.prd_mysql_port or settings.mysql_port,
        user=settings.prd_mysql_user or settings.mysql_user,
        password=settings.prd_mysql_password or settings.mysql_password,
        database=settings.prd_mysql_database or settings.mysql_database,
        charset="utf8mb4",
        connect_timeout=5,
        read_timeout=10,
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )


def _create_app_conn() -> pymysql.Connection:
    """创建应用库连接（读写）"""
    settings = get_settings()
    return pymysql.connect(
        host=settings.app_mysql_host or settings.mysql_host,
        port=settings.app_mysql_port or settings.mysql_port,
        user=settings.app_mysql_user or settings.mysql_user,
        password=settings.app_mysql_password or settings.mysql_password,
        database=settings.app_mysql_database,
        charset="utf8mb4",
        connect_timeout=5,
        read_timeout=10,
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )


def _create_biz_conn() -> pymysql.Connection:
    """创建业务库连接（读写，第二阶段）"""
    settings = get_settings()
    if not settings.biz_mysql_database:
        raise DatabaseUnavailableError("BIZ_MYSQL_DATABASE 未配置")
    return pymysql.connect(
        host=settings.biz_mysql_host or settings.mysql_host,
        port=settings.biz_mysql_port or settings.mysql_port,
        user=settings.biz_mysql_user or settings.mysql_user,
        password=settings.biz_mysql_password or settings.mysql_password,
        database=settings.biz_mysql_database,
        charset="utf8mb4",
        connect_timeout=5,
        read_timeout=10,
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )


# ── 连接池管理（通用） ────────────────────────────────────

def _acquire(pool: list[pymysql.Connection],
             create_fn, label: str) -> pymysql.Connection | None:
    """从池中获取可用连接或新建。返回连接或 None。"""
    while pool:
        c = pool.pop()
        try:
            c.ping(reconnect=True)
            return c
        except Exception:
            try:
                c.close()
            except Exception:
                pass
    return create_fn()


def _release(pool: list[pymysql.Connection], conn: pymysql.Connection):
    """归还连接到池中"""
    if len(pool) < _POOL_SIZE:
        try:
            pool.append(conn)
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
    else:
        try:
            conn.close()
        except Exception:
            pass


# ── 上下文管理器 ──────────────────────────────────────────

@contextmanager
def get_prd_conn():
    """产研库连接（只读）。连接失败 → yield None（兼容旧 bizdata 降级）。"""
    global _prd_available
    try:
        conn = _acquire(_prd_pool, _create_prd_conn, "PRD")
    except Exception as e:
        if _prd_available is not False:
            logger.warning(f"PRD 产研库连接失败（已静默跳过）: {e}")
            _prd_available = False
        yield None
        return
    _prd_available = True
    try:
        yield conn
    finally:
        _release(_prd_pool, conn)


@contextmanager
def get_app_conn():
    """应用库连接（读写）。连接失败 → 抛出 DatabaseUnavailableError。"""
    try:
        conn = _acquire(_app_pool, _create_app_conn, "APP")
    except Exception as e:
        raise DatabaseUnavailableError(f"APP 应用库连接失败: {e}") from e
    try:
        yield conn
    finally:
        _release(_app_pool, conn)


@contextmanager
def get_biz_conn():
    """业务库连接（读写，第二阶段）。连接失败 → 抛出 DatabaseUnavailableError。"""
    try:
        conn = _acquire(_biz_pool, _create_biz_conn, "BIZ")
    except Exception as e:
        raise DatabaseUnavailableError(f"BIZ 业务库连接失败: {e}") from e
    try:
        yield conn
    finally:
        _release(_biz_pool, conn)


# 兼容旧代码：get_conn = get_prd_conn
get_conn = get_prd_conn


# ═══════════════════════════════════════════════════════════
#  产研库查询函数（全部使用 get_prd_conn）
# ═══════════════════════════════════════════════════════════

_ca_map_cache: dict[int, str] = {}
_ca_map_ts: float = 0
_CA_MAP_TTL = 300


def get_channel_advertiser_map() -> dict[int, str]:
    global _ca_map_cache, _ca_map_ts
    now = time.time()
    if _ca_map_cache and (now - _ca_map_ts) < _CA_MAP_TTL:
        return _ca_map_cache

    sql = """
        SELECT DISTINCT c.channel_id, t.advertiser_id
        FROM channel_day_report c
        JOIN tiktok_media_campaign_day t ON c.campaign_id = t.campaign_id
        WHERE t.advertiser_id IS NOT NULL AND t.advertiser_id != ''
    """
    with get_prd_conn() as conn:
        if conn is None:
            return _ca_map_cache or {}
        cur = conn.cursor()
        cur.execute(sql)
        _ca_map_cache = {r["channel_id"]: r["advertiser_id"] for r in cur.fetchall()}
        _ca_map_ts = now
    return _ca_map_cache


def query_channel_report(
    start_date: str,
    end_date: str,
    campaign_id: Optional[str] = None,
    advertiser_id: Optional[str] = None,
    ad_platform: Optional[int] = None,
) -> list[dict]:
    sql = """
        SELECT
            c.campaign_id, c.report_date, c.channel_id,
            c.register_count, c.first_subscribe_count, c.first_subscribe_amount,
            c.repeat_subscribe_count, c.repeat_subscribe_amount,
            c.first_inapp_count, c.first_inapp_amount,
            c.repeat_inapp_count, c.repeat_inapp_amount,
            c.inapp_total_amount, c.subscribe_total_amount,
            c.recharge_total_amount, c.ad_cost_amount,
            c.day1_roi, c.day3_roi, c.day7_roi, c.day14_roi,
            c.day30_roi, c.day90_roi, c.day120_roi, c.ad_platform
        FROM channel_day_report c
        WHERE c.report_date BETWEEN %s AND %s
    """
    params: list = [start_date, end_date]

    if campaign_id:
        sql += " AND c.campaign_id = %s"
        params.append(campaign_id)
    if ad_platform is not None:
        sql += " AND c.ad_platform = %s"
        params.append(ad_platform)

    sql += " ORDER BY c.report_date DESC"

    with get_prd_conn() as conn:
        if conn is None:
            return []
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()

    if advertiser_id:
        mapping = get_channel_advertiser_map()
        rows = [r for r in rows if mapping.get(r["channel_id"]) == advertiser_id]

    return rows


def query_channel_report_summary(
    start_date: str,
    end_date: str,
    campaign_id: Optional[str] = None,
    advertiser_id: Optional[str] = None,
    ad_platform: Optional[int] = None,
) -> dict:
    rows = query_channel_report(start_date, end_date, campaign_id, advertiser_id, ad_platform)
    summary = {
        "register_count": 0, "first_subscribe_count": 0, "first_subscribe_amount": 0,
        "repeat_subscribe_count": 0, "repeat_subscribe_amount": 0,
        "first_inapp_count": 0, "first_inapp_amount": 0,
        "repeat_inapp_count": 0, "repeat_inapp_amount": 0,
        "inapp_total_amount": 0, "subscribe_total_amount": 0,
        "recharge_total_amount": 0, "ad_cost_amount": 0,
        "day1_roi": 0, "day3_roi": 0, "day7_roi": 0,
        "day14_roi": 0, "day30_roi": 0, "day90_roi": 0, "day120_roi": 0,
    }
    for r in rows:
        for k in summary:
            summary[k] += int(r.get(k, 0) or 0)

    if rows:
        roi_count = len([r for r in rows if r.get("day1_roi", 0)])
        if roi_count:
            for k in ["day1_roi", "day3_roi", "day7_roi", "day14_roi",
                       "day30_roi", "day90_roi", "day120_roi"]:
                summary[k] = round(summary[k] / roi_count)

    return summary


def query_campaign_business_map(
    start_date: str,
    end_date: str,
    ad_platform: Optional[int] = None,
) -> dict[str, dict]:
    rows = query_channel_report(start_date, end_date, ad_platform=ad_platform)
    result: dict[str, dict] = {}
    for r in rows:
        cid = str(r["campaign_id"])
        if not cid:
            continue
        if cid not in result:
            result[cid] = {
                "register_count": 0, "first_subscribe_count": 0,
                "subscribe_total_amount": 0, "inapp_total_amount": 0,
                "recharge_total_amount": 0, "purchase_count": 0,
                "day1_roi": 0, "day3_roi": 0, "day7_roi": 0,
                "day14_roi": 0, "day30_roi": 0, "day90_roi": 0, "day120_roi": 0,
                "_count": 0,
            }
        d = result[cid]
        d["register_count"] += int(r.get("register_count", 0) or 0)
        d["first_subscribe_count"] += int(r.get("first_subscribe_count", 0) or 0)
        d["subscribe_total_amount"] += int(r.get("subscribe_total_amount", 0) or 0)
        d["inapp_total_amount"] += int(r.get("inapp_total_amount", 0) or 0)
        d["recharge_total_amount"] += int(r.get("recharge_total_amount", 0) or 0)
        d["purchase_count"] += int(r.get("first_inapp_count", 0) or 0) + int(r.get("repeat_inapp_count", 0) or 0)
        for k in ["day1_roi", "day3_roi", "day7_roi", "day14_roi",
                   "day30_roi", "day90_roi", "day120_roi"]:
            d[k] += int(r.get(k, 0) or 0)
        d["_count"] += 1

    for cid, d in result.items():
        if d["_count"] > 0:
            for k in ["day1_roi", "day3_roi", "day7_roi", "day14_roi",
                       "day30_roi", "day90_roi", "day120_roi"]:
                d[k] = round(d[k] / d["_count"])
        del d["_count"]

    return result


def query_media_campaign_day(
    start_date: str,
    end_date: str,
    platform: str = "tiktok",
    advertiser_id: Optional[str] = None,
) -> list[dict]:
    if platform == "tiktok":
        sql = "SELECT * FROM tiktok_media_campaign_day WHERE stat_time_day BETWEEN %s AND %s"
        params: list = [start_date, end_date]
        if advertiser_id:
            sql += " AND advertiser_id = %s"
            params.append(advertiser_id)
    else:
        sql = "SELECT * FROM facebook_media_campaign_day WHERE stat_time_day BETWEEN %s AND %s"
        params = [start_date, end_date]
        if advertiser_id:
            sql += " AND account_id = %s"
            params.append(advertiser_id)

    sql += " ORDER BY stat_time_day DESC"

    with get_prd_conn() as conn:
        if conn is None:
            return []
        cur = conn.cursor()
        cur.execute(sql, params)
        return cur.fetchall()


# ═══════════════════════════════════════════════════════════
#  应用库建表 + 迁移（全部使用 _create_app_conn）
# ═══════════════════════════════════════════════════════════

_APP_TABLES_SQL = [
    """
    CREATE TABLE IF NOT EXISTS panel_definitions (
        id          BIGINT AUTO_INCREMENT PRIMARY KEY,
        panel_key   VARCHAR(100) NOT NULL UNIQUE,
        panel_name  VARCHAR(200) NOT NULL DEFAULT '',
        panel_group VARCHAR(100) NOT NULL DEFAULT '',
        route_path  VARCHAR(200) NOT NULL DEFAULT '',
        sort_order  INT          NOT NULL DEFAULT 0,
        is_enabled  TINYINT      NOT NULL DEFAULT 1,
        created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_group (panel_group),
        INDEX idx_sort (sort_order)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS role_panel_permissions (
        id          BIGINT AUTO_INCREMENT PRIMARY KEY,
        role_key    VARCHAR(50)  NOT NULL,
        panel_key   VARCHAR(100) NOT NULL,
        can_view    TINYINT      NOT NULL DEFAULT 1,
        created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_role_panel (role_key, panel_key)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS user_panel_permissions (
        id          BIGINT AUTO_INCREMENT PRIMARY KEY,
        username    VARCHAR(100) NOT NULL,
        panel_key   VARCHAR(100) NOT NULL,
        can_view    TINYINT      NOT NULL DEFAULT 1,
        source_type VARCHAR(20)  NOT NULL DEFAULT 'user',
        created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_user_panel (username, panel_key)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS app_insight_config (
        id           BIGINT AUTO_INCREMENT PRIMARY KEY,
        config_key   VARCHAR(100) NOT NULL UNIQUE,
        config_value JSON         NOT NULL,
        created_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS app_users (
        id              BIGINT AUTO_INCREMENT PRIMARY KEY,
        username        VARCHAR(100) NOT NULL UNIQUE,
        password_hash   VARCHAR(255) NOT NULL,
        role            VARCHAR(50)  NOT NULL DEFAULT 'optimizer',
        display_name    VARCHAR(100) NOT NULL DEFAULT '',
        assigned_accounts JSON       DEFAULT NULL,
        status          VARCHAR(20)  NOT NULL DEFAULT 'active',
        created_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_role (role)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS app_templates (
        id          BIGINT AUTO_INCREMENT PRIMARY KEY,
        tpl_id      VARCHAR(64)  NOT NULL UNIQUE,
        name        VARCHAR(255) NOT NULL DEFAULT '',
        platform    VARCHAR(50)  NOT NULL DEFAULT 'tiktok',
        is_builtin  TINYINT      NOT NULL DEFAULT 0,
        content     JSON         NOT NULL,
        created_by  VARCHAR(100) NOT NULL DEFAULT '',
        created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_platform (platform)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS app_oplog (
        id            BIGINT AUTO_INCREMENT PRIMARY KEY,
        user_id       BIGINT       DEFAULT NULL,
        username      VARCHAR(100) NOT NULL DEFAULT '',
        action        VARCHAR(100) NOT NULL DEFAULT '',
        target_type   VARCHAR(50)  NOT NULL DEFAULT '',
        target_id     VARCHAR(100) NOT NULL DEFAULT '',
        platform      VARCHAR(50)  NOT NULL DEFAULT '',
        before_data   JSON         DEFAULT NULL,
        after_data    JSON         DEFAULT NULL,
        status        VARCHAR(20)  NOT NULL DEFAULT 'success',
        error_message TEXT         DEFAULT NULL,
        created_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_username (username),
        INDEX idx_target (target_type, target_id),
        INDEX idx_created (created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    # ── 广告资产库 ──
    """
    CREATE TABLE IF NOT EXISTS asset_landing_pages (
        id            BIGINT AUTO_INCREMENT PRIMARY KEY,
        org_id        VARCHAR(100) NOT NULL DEFAULT '',
        name          VARCHAR(255) NOT NULL DEFAULT '',
        landing_page_url TEXT NOT NULL,
        product_name  VARCHAR(255) NOT NULL DEFAULT '',
        channel       VARCHAR(100) NOT NULL DEFAULT '',
        language      VARCHAR(50)  NOT NULL DEFAULT '',
        region_tags   JSON         DEFAULT NULL,
        remark        TEXT         DEFAULT NULL,
        status        VARCHAR(20)  NOT NULL DEFAULT 'active',
        usage_count   INT          NOT NULL DEFAULT 0,
        last_used_at  DATETIME     DEFAULT NULL,
        created_by    VARCHAR(100) NOT NULL DEFAULT '',
        created_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_org (org_id),
        INDEX idx_status (status),
        INDEX idx_name (name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS asset_copy_packs (
        id            BIGINT AUTO_INCREMENT PRIMARY KEY,
        org_id        VARCHAR(100) NOT NULL DEFAULT '',
        name          VARCHAR(255) NOT NULL DEFAULT '',
        primary_text  TEXT         DEFAULT NULL,
        headline      VARCHAR(500) NOT NULL DEFAULT '',
        description   TEXT         DEFAULT NULL,
        language      VARCHAR(50)  NOT NULL DEFAULT '',
        product_name  VARCHAR(255) NOT NULL DEFAULT '',
        channel       VARCHAR(100) NOT NULL DEFAULT '',
        country_tags  JSON         DEFAULT NULL,
        theme_tags    JSON         DEFAULT NULL,
        remark        TEXT         DEFAULT NULL,
        status        VARCHAR(20)  NOT NULL DEFAULT 'active',
        usage_count   INT          NOT NULL DEFAULT 0,
        last_used_at  DATETIME     DEFAULT NULL,
        created_by    VARCHAR(100) NOT NULL DEFAULT '',
        created_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_org (org_id),
        INDEX idx_status (status),
        INDEX idx_name (name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS asset_region_groups (
        id            BIGINT AUTO_INCREMENT PRIMARY KEY,
        org_id        VARCHAR(100) NOT NULL DEFAULT '',
        name          VARCHAR(255) NOT NULL DEFAULT '',
        country_codes JSON         NOT NULL,
        country_count INT          NOT NULL DEFAULT 0,
        language_hint VARCHAR(100) NOT NULL DEFAULT '',
        remark        TEXT         DEFAULT NULL,
        status        VARCHAR(20)  NOT NULL DEFAULT 'active',
        usage_count   INT          NOT NULL DEFAULT 0,
        last_used_at  DATETIME     DEFAULT NULL,
        created_by    VARCHAR(100) NOT NULL DEFAULT '',
        created_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_org (org_id),
        INDEX idx_status (status),
        INDEX idx_name (name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
]


_PANEL_SEED = [
    ("dashboard",          "首页概览",      "首页概览",   "/dashboard",          10),
    ("ads_data",           "广告数据",      "投放管理",   "/ads",                20),
    ("tiktok_console",     "TikTok操作台",  "投放管理",   "/console/tiktok",     21),
    ("meta_console",       "Meta操作台",    "投放管理",   "/console/meta",       22),
    ("ad_create",          "新建广告",      "投放管理",   "/ads/create",         23),
    ("template_mgmt",      "模板管理",      "投放管理",   "/templates",          24),
    ("creatives",          "素材库",        "素材中心",   "/creatives",          30),
    ("creative_analysis",  "素材分析",      "素材中心",   "/creative-analysis",  31),
    ("overview",           "数据总览",      "数据分析",   "/overview",           40),
    ("channel_analysis",   "渠道分析",      "数据分析",   "/channel-analysis",   41),
    ("biz_analysis",       "业务分析",      "数据分析",   "/biz-analysis",       42),
    ("data_compare",           "数据对比",        "数据对比",   "/data-compare",            50),
    # 广告回传分析（过渡版，回传口径，非订单真值）
    ("returned_conversion",    "广告回传分析",    "数据分析",   "/returned-conversion",     43),
    # 剧级分析
    ("drama_analysis",         "剧级分析",        "数据分析",   "/drama-analysis",          44),
    # 设计师人效报表
    ("designer_performance",   "设计师人效",      "素材中心",   "/designer-performance",    32),
    # 优化师人效报表
    ("optimizer_performance",  "优化师人效",      "数据分析",   "/optimizer-performance",   45),
    ("data_source",            "数据源配置",      "系统管理",   "/data-source",             60),
    ("user_mgmt",              "用户权限",        "系统管理",   "/user-mgmt",               61),
    ("role_perm",              "角色权限管理",    "系统管理",   "/role-perm",               62),
    ("insight_config",         "ROI阈值配置",     "系统管理",   "/insight-config",          63),
    ("oplog",                  "操作日志",        "系统管理",   "/oplog",                   64),
    ("optimizer_directory",    "优化师名单配置",  "系统管理",   "/optimizer-directory",     65),
    # 广告资产库
    ("asset_landing_pages",    "落地页库",        "广告资产库", "/assets/landing-pages",    70),
    ("asset_copy_packs",       "文案库",          "广告资产库", "/assets/copy-packs",       71),
    ("asset_region_groups",    "地区组库",        "广告资产库", "/assets/region-groups",    72),
]

_ROLE_DEFAULT_PANELS: dict[str, list[str]] = {
    "super_admin": [p[0] for p in _PANEL_SEED],
    "admin":       [p[0] for p in _PANEL_SEED if p[0] != "role_perm"],
    "optimizer":   ["dashboard", "ads_data", "tiktok_console", "meta_console", "ad_create",
                    "template_mgmt", "creatives", "creative_analysis", "optimizer_performance",
                    "optimizer_directory", "designer_performance", "drama_analysis",
                    "returned_conversion", "oplog",
                    "asset_landing_pages", "asset_copy_packs", "asset_region_groups"],
    "designer":    ["dashboard", "creatives", "creative_analysis", "designer_performance"],
    "analyst":     ["dashboard", "overview", "channel_analysis", "biz_analysis", "data_compare",
                    "creative_analysis", "returned_conversion", "drama_analysis", "designer_performance",
                    "optimizer_performance"],
    "viewer":      ["dashboard"],
}


def init_app_tables():
    """在应用库中创建元数据表（幂等）。启动时调用。"""
    try:
        conn = _create_app_conn()
    except Exception as e:
        logger.warning(f"init_app_tables: APP 应用库连接失败，跳过建表: {e}")
        return
    try:
        cur = conn.cursor()
        for sql in _APP_TABLES_SQL:
            cur.execute(sql)
        conn.commit()
        _seed_panels(cur, conn)
        _seed_role_panels(cur, conn)
        _seed_insight_config(cur, conn)
        logger.info("APP 应用库元数据表初始化完成")
    except Exception as e:
        logger.error(f"init_app_tables 失败: {e}")
    finally:
        conn.close()


def _seed_panels(cur, conn):
    """幂等：将 _PANEL_SEED 中缺失的面板补充写入 panel_definitions（INSERT IGNORE）"""
    count = 0
    for panel_key, panel_name, panel_group, route_path, sort_order in _PANEL_SEED:
        cur.execute(
            """INSERT IGNORE INTO panel_definitions
               (panel_key, panel_name, panel_group, route_path, sort_order)
               VALUES (%s, %s, %s, %s, %s)""",
            (panel_key, panel_name, panel_group, route_path, sort_order),
        )
        count += cur.rowcount
    conn.commit()
    if count:
        logger.info(f"已补充写入 {count} 条面板定义种子数据")


def _seed_role_panels(cur, conn):
    """幂等：将 _ROLE_DEFAULT_PANELS 中缺失的角色面板权限补充写入（INSERT IGNORE）"""
    count = 0
    for role_key, panels in _ROLE_DEFAULT_PANELS.items():
        for panel_key in panels:
            cur.execute(
                """INSERT IGNORE INTO role_panel_permissions (role_key, panel_key, can_view)
                   VALUES (%s, %s, 1)""",
                (role_key, panel_key),
            )
            count += cur.rowcount
    conn.commit()
    if count:
        logger.info(f"已补充写入 {count} 条角色默认面板权限")


def _seed_insight_config(cur, conn):
    """幂等：仅当 app_insight_config 为空时写入默认 ROI 阈值"""
    cur.execute("SELECT COUNT(*) AS cnt FROM app_insight_config")
    if cur.fetchone()["cnt"] > 0:
        return
    import json as _json
    default_roi = _json.dumps({"min": 0.1, "low": 0.8, "target": 1.2, "high": 2.0})
    cur.execute(
        "INSERT INTO app_insight_config (config_key, config_value) VALUES (%s, %s)",
        ("roi_thresholds", default_roi),
    )
    conn.commit()
    logger.info("已写入 Insight 默认 ROI 阈值配置")


# ═══════════════════════════════════════════════════════════
#  业务库建表（全部使用 _create_biz_conn）
# ═══════════════════════════════════════════════════════════

_BIZ_TABLES_SQL = [
    """
    CREATE TABLE IF NOT EXISTS biz_ad_accounts (
        id              BIGINT AUTO_INCREMENT PRIMARY KEY,
        platform        VARCHAR(20)   NOT NULL COMMENT 'tiktok / meta',
        account_id      VARCHAR(100)  NOT NULL COMMENT '平台侧账户ID',
        account_name    VARCHAR(255)  NOT NULL DEFAULT '',
        currency        VARCHAR(10)   NOT NULL DEFAULT 'USD',
        timezone        VARCHAR(50)   NOT NULL DEFAULT 'UTC',
        status          VARCHAR(30)   NOT NULL DEFAULT 'ACTIVE',
        access_token    TEXT          DEFAULT NULL COMMENT '平台 Access Token',
        app_id          VARCHAR(200)  NOT NULL DEFAULT '' COMMENT 'TikTok App ID',
        app_secret      VARCHAR(200)  NOT NULL DEFAULT '' COMMENT 'TikTok App Secret',
        is_default      TINYINT       NOT NULL DEFAULT 0 COMMENT '是否为默认账户',
        last_synced_at  DATETIME      DEFAULT NULL COMMENT '上次数据同步时间',
        raw_json        JSON          DEFAULT NULL,
        created_at      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_platform_account (platform, account_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS biz_campaigns (
        id              BIGINT AUTO_INCREMENT PRIMARY KEY,
        platform        VARCHAR(20)   NOT NULL,
        account_id      VARCHAR(100)  NOT NULL,
        campaign_id     VARCHAR(100)  NOT NULL,
        campaign_name   VARCHAR(500)  NOT NULL DEFAULT '',
        objective       VARCHAR(100)  NOT NULL DEFAULT '',
        buying_type     VARCHAR(50)   NOT NULL DEFAULT '',
        status          VARCHAR(50)   NOT NULL DEFAULT '',
        is_active       TINYINT       NOT NULL DEFAULT 1,
        raw_json        JSON          DEFAULT NULL,
        created_at      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_platform_campaign (platform, campaign_id),
        INDEX idx_platform_account (platform, account_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS biz_campaign_daily_normalized (
        id              BIGINT AUTO_INCREMENT PRIMARY KEY,
        platform        VARCHAR(20)   NOT NULL,
        account_id      VARCHAR(100)  NOT NULL,
        campaign_id     VARCHAR(100)  NOT NULL,
        campaign_name   VARCHAR(500)  NOT NULL DEFAULT '',
        stat_date       DATE          NOT NULL,
        spend           DECIMAL(14,4) NOT NULL DEFAULT 0,
        impressions     BIGINT        NOT NULL DEFAULT 0,
        clicks          BIGINT        NOT NULL DEFAULT 0,
        installs        BIGINT        NOT NULL DEFAULT 0,
        conversions     BIGINT        NOT NULL DEFAULT 0,
        revenue         DECIMAL(14,4) NOT NULL DEFAULT 0,
        ctr             DECIMAL(10,6) DEFAULT NULL,
        cpc             DECIMAL(10,4) DEFAULT NULL,
        cpm             DECIMAL(10,4) DEFAULT NULL,
        cpi             DECIMAL(10,4) DEFAULT NULL,
        cpa             DECIMAL(10,4) DEFAULT NULL,
        roas            DECIMAL(10,4) DEFAULT NULL,
        raw_json        JSON          DEFAULT NULL,
        created_at      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_platform_campaign_date (platform, campaign_id, stat_date),
        INDEX idx_stat_date (stat_date),
        INDEX idx_platform_account_date (platform, account_id, stat_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS biz_adgroups (
        id              BIGINT AUTO_INCREMENT PRIMARY KEY,
        platform        VARCHAR(20)   NOT NULL,
        account_id      VARCHAR(100)  NOT NULL,
        campaign_id     VARCHAR(100)  NOT NULL DEFAULT '',
        adgroup_id      VARCHAR(100)  NOT NULL,
        adgroup_name    VARCHAR(500)  NOT NULL DEFAULT '',
        status          VARCHAR(50)   NOT NULL DEFAULT '',
        is_active       TINYINT       NOT NULL DEFAULT 1,
        raw_json        JSON          DEFAULT NULL,
        created_at      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_platform_adgroup (platform, adgroup_id),
        INDEX idx_platform_campaign (platform, campaign_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS biz_adgroup_daily_normalized (
        id              BIGINT AUTO_INCREMENT PRIMARY KEY,
        platform        VARCHAR(20)   NOT NULL,
        account_id      VARCHAR(100)  NOT NULL,
        campaign_id     VARCHAR(100)  NOT NULL DEFAULT '',
        campaign_name   VARCHAR(500)  NOT NULL DEFAULT '',
        adgroup_id      VARCHAR(100)  NOT NULL,
        adgroup_name    VARCHAR(500)  NOT NULL DEFAULT '',
        stat_date       DATE          NOT NULL,
        spend           DECIMAL(14,4) NOT NULL DEFAULT 0,
        impressions     BIGINT        NOT NULL DEFAULT 0,
        clicks          BIGINT        NOT NULL DEFAULT 0,
        installs        BIGINT        NOT NULL DEFAULT 0,
        conversions     BIGINT        NOT NULL DEFAULT 0,
        revenue         DECIMAL(14,4) NOT NULL DEFAULT 0,
        ctr             DECIMAL(10,6) DEFAULT NULL,
        cpc             DECIMAL(10,4) DEFAULT NULL,
        cpm             DECIMAL(10,4) DEFAULT NULL,
        cpi             DECIMAL(10,4) DEFAULT NULL,
        cpa             DECIMAL(10,4) DEFAULT NULL,
        roas            DECIMAL(10,4) DEFAULT NULL,
        raw_json        JSON          DEFAULT NULL,
        created_at      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_platform_adgroup_date (platform, adgroup_id, stat_date),
        INDEX idx_stat_date (stat_date),
        INDEX idx_platform_account_date (platform, account_id, stat_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS biz_ad_daily_normalized (
        id              BIGINT AUTO_INCREMENT PRIMARY KEY,
        platform        VARCHAR(20)   NOT NULL,
        account_id      VARCHAR(100)  NOT NULL,
        campaign_id     VARCHAR(100)  NOT NULL DEFAULT '',
        campaign_name   VARCHAR(500)  NOT NULL DEFAULT '',
        adgroup_id      VARCHAR(100)  NOT NULL DEFAULT '',
        adgroup_name    VARCHAR(500)  NOT NULL DEFAULT '',
        ad_id           VARCHAR(100)  NOT NULL,
        ad_name         VARCHAR(500)  NOT NULL DEFAULT '',
        stat_date       DATE          NOT NULL,
        spend           DECIMAL(14,4) NOT NULL DEFAULT 0,
        impressions     BIGINT        NOT NULL DEFAULT 0,
        clicks          BIGINT        NOT NULL DEFAULT 0,
        installs        BIGINT        NOT NULL DEFAULT 0,
        conversions     BIGINT        NOT NULL DEFAULT 0,
        revenue         DECIMAL(14,4) NOT NULL DEFAULT 0,
        ctr             DECIMAL(10,6) DEFAULT NULL,
        cpc             DECIMAL(10,4) DEFAULT NULL,
        cpm             DECIMAL(10,4) DEFAULT NULL,
        cpi             DECIMAL(10,4) DEFAULT NULL,
        cpa             DECIMAL(10,4) DEFAULT NULL,
        roas            DECIMAL(10,4) DEFAULT NULL,
        raw_json        JSON          DEFAULT NULL,
        created_at      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_platform_ad_date (platform, ad_id, stat_date),
        INDEX idx_stat_date (stat_date),
        INDEX idx_platform_account_date (platform, account_id, stat_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS biz_ads (
        id              BIGINT AUTO_INCREMENT PRIMARY KEY,
        platform        VARCHAR(20)   NOT NULL,
        account_id      VARCHAR(100)  NOT NULL,
        campaign_id     VARCHAR(100)  NOT NULL DEFAULT '',
        adgroup_id      VARCHAR(100)  NOT NULL DEFAULT '',
        ad_id           VARCHAR(100)  NOT NULL,
        ad_name         VARCHAR(500)  NOT NULL DEFAULT '',
        status          VARCHAR(50)   NOT NULL DEFAULT '',
        is_active       TINYINT       NOT NULL DEFAULT 1,
        raw_json        JSON          DEFAULT NULL,
        created_at      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_platform_ad (platform, ad_id),
        INDEX idx_platform_adgroup (platform, adgroup_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    -- 广告回传转化日报表（回传口径，非订单真值，存储于 BIZ 业务库）
    -- data_label = 'returned' 标识本表全部数据为广告平台归因回传口径
    CREATE TABLE IF NOT EXISTS ad_returned_conversion_daily (
        id                       BIGINT         AUTO_INCREMENT PRIMARY KEY,
        stat_date                DATE           NOT NULL COMMENT '统计日期',
        media_source             VARCHAR(50)    NOT NULL COMMENT '媒体来源: meta / tiktok / google',
        account_id               VARCHAR(100)   DEFAULT NULL COMMENT '广告账户ID',
        campaign_id              VARCHAR(100)   DEFAULT NULL COMMENT '广告系列ID',
        campaign_name            VARCHAR(255)   DEFAULT NULL COMMENT '广告系列名称',
        adset_id                 VARCHAR(100)   DEFAULT NULL COMMENT '广告组ID',
        adset_name               VARCHAR(255)   DEFAULT NULL COMMENT '广告组名称',
        ad_id                    VARCHAR(100)   DEFAULT NULL COMMENT '广告ID',
        ad_name                  VARCHAR(255)   DEFAULT NULL COMMENT '广告名称',
        country                  VARCHAR(50)    DEFAULT NULL COMMENT '国家/地区',
        platform                 VARCHAR(50)    DEFAULT NULL COMMENT '操作系统平台: ios / android / mixed',
        impressions              BIGINT         NOT NULL DEFAULT 0 COMMENT '展示量',
        clicks                   BIGINT         NOT NULL DEFAULT 0 COMMENT '点击量',
        installs                 BIGINT         NOT NULL DEFAULT 0 COMMENT '安装量',
        spend                    DECIMAL(18,4)  NOT NULL DEFAULT 0 COMMENT '广告花费',
        -- 以下字段均为广告平台归因回传口径，非后端订单真值 --
        registrations_returned   BIGINT         NOT NULL DEFAULT 0 COMMENT '回传注册数（平台归因）',
        purchase_value_returned  DECIMAL(18,4)  NOT NULL DEFAULT 0 COMMENT '回传充值价值（平台归因）',
        subscribe_value_returned DECIMAL(18,4)  NOT NULL DEFAULT 0 COMMENT '回传订阅价值（平台归因，不支持时为0）',
        total_value_returned     DECIMAL(18,4)  NOT NULL DEFAULT 0 COMMENT '回传总价值 = purchase + subscribe',
        d0_roi_returned          DECIMAL(18,6)  NOT NULL DEFAULT 0 COMMENT 'D0 ROI 回传口径 = total_value / spend',
        d1_value_returned        DECIMAL(18,4)  NOT NULL DEFAULT 0 COMMENT 'D1 回传价值（仅平台支持时有值）',
        d1_roi_returned          DECIMAL(18,6)  NOT NULL DEFAULT 0 COMMENT 'D1 ROI 回传口径 = d1_value / spend',
        -- 首日（D0 Cohort）拆分字段，需平台支持 D0 cohort 上报时才有值，默认 0 --
        d0_registrations_returned    BIGINT        NOT NULL DEFAULT 0 COMMENT '首日注册数（D0 cohort，平台支持时有值）',
        d0_purchase_value_returned   DECIMAL(18,4) NOT NULL DEFAULT 0 COMMENT '首日充值金额（D0 cohort，平台支持时有值）',
        d0_subscribe_value_returned  DECIMAL(18,4) NOT NULL DEFAULT 0 COMMENT '首日订阅金额（D0 cohort，平台支持时有值）',
        data_label               VARCHAR(50)    NOT NULL DEFAULT 'returned' COMMENT '数据口径标识，固定为 returned',
        raw_payload              JSON           DEFAULT NULL COMMENT '原始平台数据，用于追溯字段来源',
        created_at               DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at               DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_returned_daily (stat_date, media_source, account_id, campaign_id, adset_id, ad_id, country, platform),
        INDEX idx_stat_date (stat_date),
        INDEX idx_media_source (media_source),
        INDEX idx_account_date (account_id, stat_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    COMMENT='广告回传转化日报 — 回传口径，非订单真值，存储在 adpilot_biz 业务库'
    """,
    """
    CREATE TABLE IF NOT EXISTS biz_sync_logs (
        id              BIGINT AUTO_INCREMENT PRIMARY KEY,
        task_name       VARCHAR(100)  NOT NULL,
        platform        VARCHAR(20)   NOT NULL DEFAULT '',
        account_id      VARCHAR(100)  NOT NULL DEFAULT '',
        sync_date       DATE          DEFAULT NULL,
        status          VARCHAR(20)   NOT NULL DEFAULT 'running',
        message         TEXT          DEFAULT NULL,
        rows_affected   INT           NOT NULL DEFAULT 0,
        started_at      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
        finished_at     DATETIME      DEFAULT NULL,
        created_at      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_task_date (task_name, sync_date),
        INDEX idx_status (status)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    # ─── 剧级映射表：存储每条广告活动的剧名解析结果 ───────────────────
    """
    CREATE TABLE IF NOT EXISTS ad_drama_mapping (
        id                   BIGINT AUTO_INCREMENT PRIMARY KEY,
        source_type          VARCHAR(50)   NOT NULL DEFAULT '' COMMENT '来源类型: 小程序 / APP',
        platform             VARCHAR(20)   NOT NULL DEFAULT '' COMMENT '媒体平台: tiktok / meta',
        channel              VARCHAR(50)   NOT NULL DEFAULT '' COMMENT '渠道标识',
        account_id           VARCHAR(100)  NOT NULL DEFAULT '' COMMENT '广告账户ID',
        campaign_id          VARCHAR(100)  NOT NULL DEFAULT '' COMMENT '活动ID',
        campaign_name        TEXT          NOT NULL COMMENT '原始活动名称',
        adset_id             VARCHAR(100)  NOT NULL DEFAULT '',
        adset_name           VARCHAR(500)  NOT NULL DEFAULT '',
        ad_id                VARCHAR(100)  NOT NULL DEFAULT '',
        ad_name              VARCHAR(500)  NOT NULL DEFAULT '',

        drama_id             VARCHAR(100)  NOT NULL DEFAULT '' COMMENT '剧集ID（小程序解析得到；APP暂留）',
        drama_type           VARCHAR(50)   NOT NULL DEFAULT '' COMMENT '剧集类型，如 AIGC',
        country              VARCHAR(20)   NOT NULL DEFAULT '' COMMENT '国家/地区代码',

        drama_name_raw       VARCHAR(500)  NOT NULL DEFAULT '' COMMENT '第10字段原始剧名（唯一合法来源）',
        localized_drama_name VARCHAR(500)  NOT NULL DEFAULT '' COMMENT '去掉语言尾缀的剧名',
        language_code        VARCHAR(10)   NOT NULL DEFAULT 'unknown' COMMENT '语言代码，未识别为 unknown',
        language_tag_raw     VARCHAR(20)   DEFAULT NULL COMMENT '原始语言标记，如 (EN)',

        buyer_name           VARCHAR(100)  NOT NULL DEFAULT '' COMMENT '投手姓名（小程序字段6）',
        buyer_short_name     VARCHAR(100)  NOT NULL DEFAULT '' COMMENT '投手简称（APP字段11）',
        optimization_type    VARCHAR(100)  NOT NULL DEFAULT '' COMMENT '优化目标',
        bid_type             VARCHAR(50)   NOT NULL DEFAULT '' COMMENT '出价类型',
        publish_date         VARCHAR(20)   NOT NULL DEFAULT '' COMMENT '发布日期，格式 YYYYMMDD',

        remark_raw           TEXT          DEFAULT NULL COMMENT '备注原文（第11+字段），不参与任何解析',

        content_key          VARCHAR(500)  NOT NULL DEFAULT '' COMMENT '剧内容唯一键：小程序=drama_id，APP=normalized(localized_drama_name)',
        match_source         VARCHAR(50)   NOT NULL DEFAULT 'parser' COMMENT '映射来源: parser / manual',
        is_confirmed         TINYINT       NOT NULL DEFAULT 0 COMMENT '是否人工确认',
        parse_status         VARCHAR(20)   NOT NULL DEFAULT 'ok' COMMENT 'ok / partial / failed',
        parse_error          TEXT          DEFAULT NULL COMMENT '解析失败原因',

        created_at           DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at           DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

        UNIQUE KEY uk_campaign (platform, account_id, campaign_id),
        INDEX idx_content_key (content_key(191)),
        INDEX idx_drama_id (drama_id),
        INDEX idx_language (language_code),
        INDEX idx_source_type (source_type)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    COMMENT='广告活动剧名解析映射表'
    """,
    # ─── 剧级日报事实表：聚合后的每日投放数据 ────────────────────────
    """
    CREATE TABLE IF NOT EXISTS fact_drama_daily (
        id                   BIGINT AUTO_INCREMENT PRIMARY KEY,
        stat_date            DATE          NOT NULL COMMENT '统计日期',
        source_type          VARCHAR(50)   NOT NULL DEFAULT '' COMMENT '来源类型: 小程序 / APP',
        platform             VARCHAR(20)   NOT NULL DEFAULT '' COMMENT '媒体平台: tiktok / meta',
        channel              VARCHAR(50)   NOT NULL DEFAULT '' COMMENT '渠道标识',
        account_id           VARCHAR(100)  NOT NULL DEFAULT '' COMMENT '广告账户ID',
        country              VARCHAR(20)   NOT NULL DEFAULT '' COMMENT '国家/地区代码',

        drama_id             VARCHAR(100)  NOT NULL DEFAULT '' COMMENT '剧集ID',
        drama_type           VARCHAR(50)   NOT NULL DEFAULT '' COMMENT '剧集类型',
        localized_drama_name VARCHAR(500)  NOT NULL DEFAULT '' COMMENT '本地化剧名（不含语言尾缀）',
        language_code        VARCHAR(10)   NOT NULL DEFAULT 'unknown' COMMENT '语言代码',
        content_key          VARCHAR(500)  NOT NULL DEFAULT '' COMMENT '剧内容唯一键',

        spend                DECIMAL(18,4) NOT NULL DEFAULT 0 COMMENT '花费（USD）',
        impressions          BIGINT        NOT NULL DEFAULT 0 COMMENT '展示次数',
        clicks               BIGINT        NOT NULL DEFAULT 0 COMMENT '点击次数',
        installs             BIGINT        NOT NULL DEFAULT 0 COMMENT '安装数',
        registrations        BIGINT        NOT NULL DEFAULT 0 COMMENT '注册数',
        purchase_value       DECIMAL(18,4) NOT NULL DEFAULT 0 COMMENT '购买/充值价值',

        created_at           DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at           DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

        UNIQUE KEY uk_drama_daily (stat_date, source_type, platform, channel, account_id, country, content_key(191), language_code),
        INDEX idx_content_key (content_key(191)),
        INDEX idx_stat_date (stat_date),
        INDEX idx_drama_id (drama_id),
        INDEX idx_language (language_code)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    COMMENT='剧级日报事实表（按 content_key + language_code 聚合）'
    """,
    # ─── 优化师-Campaign 映射表 ────────────────────────
    """
    CREATE TABLE IF NOT EXISTS campaign_optimizer_mapping (
        id                        BIGINT AUTO_INCREMENT PRIMARY KEY,
        source_type               VARCHAR(50)   NOT NULL DEFAULT '' COMMENT '来源类型: 小程序 / APP',
        platform                  VARCHAR(20)   NOT NULL DEFAULT '' COMMENT '媒体平台: tiktok / meta',
        channel                   VARCHAR(50)   NOT NULL DEFAULT '' COMMENT '渠道标识',
        account_id                VARCHAR(100)  NOT NULL DEFAULT '' COMMENT '广告账户ID',
        campaign_id               VARCHAR(100)  NOT NULL DEFAULT '' COMMENT '活动ID',
        campaign_name             TEXT          NOT NULL COMMENT '原始活动名称',

        optimizer_name_raw        VARCHAR(200)  NOT NULL DEFAULT '' COMMENT '解析出的原始优化师名称',
        optimizer_name_normalized VARCHAR(200)  NOT NULL DEFAULT '未识别' COMMENT '标准化优化师名称（大写去空格）',
        optimizer_source          VARCHAR(50)   NOT NULL DEFAULT 'campaign_name' COMMENT '优化师来源: campaign_name / structured',

        parse_status              VARCHAR(20)   NOT NULL DEFAULT 'ok' COMMENT 'ok / failed',
        parse_error               TEXT          DEFAULT NULL COMMENT '解析失败原因',

        created_at                DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at                DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

        UNIQUE KEY uk_optimizer_campaign (source_type, platform, account_id, campaign_id),
        INDEX idx_optimizer_name (optimizer_name_normalized),
        INDEX idx_source_type (source_type)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    COMMENT='Campaign 到优化师的映射表'
    """,
    # ─── 优化师日报事实表 ────────────────────────
    """
    CREATE TABLE IF NOT EXISTS fact_optimizer_daily (
        id                        BIGINT AUTO_INCREMENT PRIMARY KEY,
        stat_date                 DATE          NOT NULL COMMENT '统计日期',
        source_type               VARCHAR(50)   NOT NULL DEFAULT '' COMMENT '来源类型: 小程序 / APP',
        platform                  VARCHAR(20)   NOT NULL DEFAULT '' COMMENT '媒体平台: tiktok / meta',
        channel                   VARCHAR(50)   NOT NULL DEFAULT '' COMMENT '渠道标识',
        account_id                VARCHAR(100)  NOT NULL DEFAULT '' COMMENT '广告账户ID',
        country                   VARCHAR(20)   NOT NULL DEFAULT '' COMMENT '国家/地区代码',
        optimizer_name            VARCHAR(200)  NOT NULL DEFAULT '未识别' COMMENT '优化师名称（标准化）',

        spend                     DECIMAL(18,4) NOT NULL DEFAULT 0 COMMENT '花费（USD）',
        impressions               BIGINT        NOT NULL DEFAULT 0 COMMENT '展示次数',
        clicks                    BIGINT        NOT NULL DEFAULT 0 COMMENT '点击次数',
        installs                  BIGINT        NOT NULL DEFAULT 0 COMMENT '安装数',
        registrations             BIGINT        NOT NULL DEFAULT 0 COMMENT '注册数',
        purchase_value            DECIMAL(18,4) NOT NULL DEFAULT 0 COMMENT '购买/充值价值',
        campaign_count            INT           NOT NULL DEFAULT 0 COMMENT '关联Campaign数',

        created_at                DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at                DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

        UNIQUE KEY uk_optimizer_daily (stat_date, source_type, platform, channel, account_id, country, optimizer_name),
        INDEX idx_optimizer_name (optimizer_name),
        INDEX idx_stat_date (stat_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    COMMENT='优化师日报事实表'
    """,
    # ─── 优化师默认规则表 ────────────────────────
    """
    CREATE TABLE IF NOT EXISTS optimizer_default_rules (
        id                        BIGINT AUTO_INCREMENT PRIMARY KEY,
        source_type               VARCHAR(50)   NOT NULL DEFAULT '' COMMENT '来源类型: 小程序 / APP / 空=全部',
        platform                  VARCHAR(20)   NOT NULL DEFAULT '' COMMENT '媒体平台: tiktok / meta / 空=全部',
        channel                   VARCHAR(50)   NOT NULL DEFAULT '' COMMENT '渠道标识 / 空=全部',
        account_id                VARCHAR(100)  NOT NULL DEFAULT '' COMMENT '广告账户ID / 空=全部',
        country                   VARCHAR(20)   NOT NULL DEFAULT '' COMMENT '国家代码 / 空=全部',
        optimizer_name            VARCHAR(200)  NOT NULL COMMENT '匹配到的优化师名称',
        priority                  INT           NOT NULL DEFAULT 0 COMMENT '优先级（越大越高）',
        is_enabled                TINYINT       NOT NULL DEFAULT 1 COMMENT '是否启用',
        created_at                DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at                DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_priority (priority DESC),
        INDEX idx_enabled (is_enabled)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    COMMENT='优化师默认兜底规则表'
    """,
    # ─── 优化师名单表 ────────────────────────
    """
    CREATE TABLE IF NOT EXISTS optimizer_directory (
        id                        BIGINT AUTO_INCREMENT PRIMARY KEY,
        optimizer_name            VARCHAR(200)  NOT NULL COMMENT '标准名称',
        optimizer_code            VARCHAR(100)  NOT NULL COMMENT '唯一编码',
        aliases                   VARCHAR(500)  NOT NULL DEFAULT '' COMMENT '别名，逗号分隔',
        is_active                 TINYINT       NOT NULL DEFAULT 1 COMMENT '是否启用',
        remark                    VARCHAR(500)  NOT NULL DEFAULT '' COMMENT '备注',
        created_at                DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at                DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_optimizer_code (optimizer_code),
        INDEX idx_optimizer_name (optimizer_name),
        INDEX idx_is_active (is_active)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    COMMENT='优化师名单配置表'
    """,
]


def init_biz_tables():
    """在业务库中创建数据沉淀表(幂等)。启动时调用。"""
    settings = get_settings()
    if not settings.biz_mysql_database:
        logger.info("init_biz_tables: BIZ_MYSQL_DATABASE 未配置，跳过")
        return
    try:
        conn = _create_biz_conn()
    except Exception as e:
        logger.warning(f"init_biz_tables: BIZ 业务库连接失败，跳过建表: {e}")
        return
    try:
        cur = conn.cursor()
        for sql in _BIZ_TABLES_SQL:
            cur.execute(sql)
        _migrate_biz_ad_accounts_columns(cur)
        _migrate_returned_conversion_d0_columns(cur)
        _migrate_returned_conversion_cleanup(cur)
        _migrate_optimizer_mapping_columns(cur)
        conn.commit()
        logger.info("BIZ 业务库数据沉淀表初始化完成")
    except Exception as e:
        logger.error(f"init_biz_tables 失败: {e}")
    finally:
        conn.close()


def _migrate_optimizer_mapping_columns(cur):
    """为 campaign_optimizer_mapping 补齐 match_source / match_confidence / match_position 列（幂等）"""
    cur.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'campaign_optimizer_mapping'")
    existing = {r["COLUMN_NAME"] for r in cur.fetchall()}
    migrations = [
        ("optimizer_match_source",
         "ADD COLUMN optimizer_match_source VARCHAR(50) NOT NULL DEFAULT 'campaign_name' "
         "COMMENT '匹配来源: structured_field/campaign_name/historical_mapping/default_rule/unassigned' "
         "AFTER parse_error"),
        ("optimizer_match_confidence",
         "ADD COLUMN optimizer_match_confidence DECIMAL(4,2) NOT NULL DEFAULT 0.90 "
         "COMMENT '匹配置信度 0~1' AFTER optimizer_match_source"),
        ("optimizer_match_position",
         "ADD COLUMN optimizer_match_position VARCHAR(20) NOT NULL DEFAULT '' "
         "COMMENT '匹配位置: field_6/field_11/field_12/unassigned' AFTER optimizer_match_confidence"),
    ]
    for col, ddl in migrations:
        if col not in existing:
            cur.execute(f"ALTER TABLE campaign_optimizer_mapping {ddl}")


def _migrate_returned_conversion_d0_columns(cur):
    """为 ad_returned_conversion_daily 补齐 D0 Cohort 三列（幂等）"""
    cur.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'ad_returned_conversion_daily'")
    existing = {r["COLUMN_NAME"] for r in cur.fetchall()}
    migrations = [
        ("d0_registrations_returned",   "ADD COLUMN d0_registrations_returned   BIGINT        NOT NULL DEFAULT 0 COMMENT '首日注册数（D0 cohort）' AFTER d1_roi_returned"),
        ("d0_purchase_value_returned",  "ADD COLUMN d0_purchase_value_returned  DECIMAL(18,4) NOT NULL DEFAULT 0 COMMENT '首日充值金额（D0 cohort）' AFTER d0_registrations_returned"),
        ("d0_subscribe_value_returned", "ADD COLUMN d0_subscribe_value_returned DECIMAL(18,4) NOT NULL DEFAULT 0 COMMENT '首日订阅金额（D0 cohort）' AFTER d0_purchase_value_returned"),
    ]
    for col, ddl in migrations:
        if col not in existing:
            cur.execute(f"ALTER TABLE ad_returned_conversion_daily {ddl}")


def _migrate_returned_conversion_cleanup(cur):
    """清理 ad_returned_conversion_daily 中因 NULL 唯一键失效产生的重复行 + campaign/adset 汇总行（幂等）。

    问题根因：
      1. upsert() 将空字符串转为 NULL → 唯一键无法去重 → 每次同步都 INSERT 新行
      2. campaign/adset/ad 三级同时写入 → 同一笔花费被 SUM 3 次
    修复策略：
      Step 1: 先删除"NULL 等价重复行"（把 NULL 视为 '' 后与其他行重复的行，只保留 id 最大的）
      Step 2: 将唯一键列中的 NULL 统一为空字符串
      Step 3: 删除转换后仍然存在的重复行
      Step 4: 删除 campaign/adset 汇总行
    """
    cur.execute("SELECT COUNT(*) AS cnt FROM ad_returned_conversion_daily")
    total = cur.fetchone()["cnt"]
    if total == 0:
        return

    # Step 1: 删除"NULL 等价重复行"：将 NULL 视为 '' 后分组，每组只保留 id 最大的
    cur.execute("""
        DELETE t1 FROM ad_returned_conversion_daily t1
        INNER JOIN (
            SELECT COALESCE(stat_date, '1970-01-01') AS g_date,
                   COALESCE(media_source, '') AS g_ms,
                   COALESCE(account_id, '') AS g_acc,
                   COALESCE(campaign_id, '') AS g_cid,
                   COALESCE(adset_id, '') AS g_asid,
                   COALESCE(ad_id, '') AS g_adid,
                   COALESCE(country, '') AS g_co,
                   COALESCE(platform, '') AS g_pf,
                   MAX(id) AS keep_id
            FROM ad_returned_conversion_daily
            GROUP BY g_date, g_ms, g_acc, g_cid, g_asid, g_adid, g_co, g_pf
            HAVING COUNT(*) > 1
        ) t2 ON COALESCE(t1.stat_date, '1970-01-01') = t2.g_date
            AND COALESCE(t1.media_source, '') = t2.g_ms
            AND COALESCE(t1.account_id, '') = t2.g_acc
            AND COALESCE(t1.campaign_id, '') = t2.g_cid
            AND COALESCE(t1.adset_id, '') = t2.g_asid
            AND COALESCE(t1.ad_id, '') = t2.g_adid
            AND COALESCE(t1.country, '') = t2.g_co
            AND COALESCE(t1.platform, '') = t2.g_pf
            AND t1.id != t2.keep_id
    """)
    dedup_deleted = cur.rowcount
    if dedup_deleted > 0:
        logger.info(f"returned_conversion 清理: 删除 {dedup_deleted} 条唯一键重复行")

    # Step 2: NULL → 空字符串（让唯一键可以正常工作）
    for col in ("account_id", "campaign_id", "adset_id", "ad_id", "country", "platform"):
        cur.execute(f"UPDATE ad_returned_conversion_daily SET {col} = '' WHERE {col} IS NULL")

    # Step 3: 删除 campaign 汇总行（adset_id='' AND ad_id=''），前提是同 campaign 下存在 ad 级别行
    cur.execute("""
        DELETE FROM ad_returned_conversion_daily
        WHERE adset_id = '' AND ad_id = ''
          AND campaign_id IN (
              SELECT DISTINCT campaign_id FROM (
                  SELECT campaign_id FROM ad_returned_conversion_daily WHERE ad_id != ''
              ) sub
          )
    """)
    campaign_deleted = cur.rowcount

    # Step 4: 删除 adset 汇总行（ad_id=''），前提是同 adset 下存在 ad 级别行
    cur.execute("""
        DELETE FROM ad_returned_conversion_daily
        WHERE ad_id = '' AND adset_id != ''
          AND adset_id IN (
              SELECT DISTINCT adset_id FROM (
                  SELECT adset_id FROM ad_returned_conversion_daily WHERE ad_id != ''
              ) sub
          )
    """)
    adset_deleted = cur.rowcount

    if campaign_deleted + adset_deleted > 0:
        logger.info(f"returned_conversion 清理: 删除 {campaign_deleted} 条 campaign 汇总行, "
                    f"{adset_deleted} 条 adset 汇总行")

    cur.execute("SELECT COUNT(*) AS cnt FROM ad_returned_conversion_daily")
    remaining = cur.fetchone()["cnt"]
    logger.info(f"returned_conversion 清理完成: 清理前 {total} 行 → 清理后 {remaining} 行")


def _migrate_biz_ad_accounts_columns(cur):
    """为 biz_ad_accounts 表补齐新增列（幂等）"""
    cur.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'biz_ad_accounts'")
    existing = {r["COLUMN_NAME"] for r in cur.fetchall()}
    migrations = [
        ("access_token",   "ADD COLUMN access_token TEXT DEFAULT NULL AFTER status"),
        ("app_id",         "ADD COLUMN app_id VARCHAR(200) NOT NULL DEFAULT '' AFTER access_token"),
        ("app_secret",     "ADD COLUMN app_secret VARCHAR(200) NOT NULL DEFAULT '' AFTER app_id"),
        ("is_default",     "ADD COLUMN is_default TINYINT NOT NULL DEFAULT 0 AFTER app_secret"),
        ("last_synced_at", "ADD COLUMN last_synced_at DATETIME DEFAULT NULL AFTER is_default"),
    ]
    for col, ddl in migrations:
        if col not in existing:
            cur.execute(f"ALTER TABLE biz_ad_accounts {ddl}")
            logger.info(f"biz_ad_accounts: 已添加列 {col}")


def migrate_json_data():
    """将 JSON 文件中的历史数据一次性导入应用库（仅当目标表为空时执行）。"""
    try:
        conn = _create_app_conn()
    except Exception as e:
        logger.warning(f"migrate_json_data: APP 应用库连接失败，跳过迁移: {e}")
        return
    try:
        cur = conn.cursor()
        _migrate_users(cur, conn)
        _migrate_templates(cur, conn)
        _migrate_oplog(cur, conn)
    except Exception as e:
        logger.error(f"migrate_json_data 失败: {e}")
    finally:
        conn.close()


def _table_is_empty(cur, table: str) -> bool:
    cur.execute(f"SELECT COUNT(*) AS cnt FROM {table}")
    return cur.fetchone()["cnt"] == 0


def _migrate_users(cur, conn):
    if not _table_is_empty(cur, "app_users"):
        return
    json_path = Path(__file__).parent / "users.json"
    if not json_path.exists():
        return
    try:
        users = json.loads(json_path.read_text(encoding="utf-8"))
    except Exception:
        return
    count = 0
    for username, data in users.items():
        cur.execute(
            """INSERT INTO app_users (username, password_hash, role, display_name, assigned_accounts)
               VALUES (%s, %s, %s, %s, %s)""",
            (
                data.get("username", username),
                data.get("hashed_password", ""),
                data.get("role", "optimizer"),
                data.get("display_name", username),
                json.dumps(data.get("assigned_accounts", []), ensure_ascii=False),
            ),
        )
        count += 1
    conn.commit()
    logger.info(f"已从 users.json 迁移 {count} 个用户到 APP.app_users")


def _migrate_templates(cur, conn):
    from routes.templates import BUILTIN_TEMPLATES

    is_empty = _table_is_empty(cur, "app_templates")

    all_templates: list[dict] = []
    builtin_ids = {t["id"] for t in BUILTIN_TEMPLATES}

    for t in BUILTIN_TEMPLATES:
        all_templates.append(t)

    if is_empty:
        json_path = Path(__file__).parent / "templates.json"
        if json_path.exists():
            try:
                file_data = json.loads(json_path.read_text(encoding="utf-8"))
                if isinstance(file_data, list):
                    for t in file_data:
                        if isinstance(t, dict) and t.get("id"):
                            if t["id"] in builtin_ids:
                                for i, bt in enumerate(all_templates):
                                    if bt["id"] == t["id"]:
                                        all_templates[i] = t
                                        break
                            else:
                                all_templates.append(t)
            except Exception:
                pass

    inserted = 0
    updated = 0
    for t in all_templates:
        tpl_id = t.get("id", "")
        name = t.get("name", "")
        platform = t.get("platform", "tiktok")
        is_builtin = 1 if tpl_id in builtin_ids else 0
        created_at = t.get("created_at")
        content = {k: v for k, v in t.items() if k not in ("id", "name", "platform", "created_at", "updated_at")}
        content_json = json.dumps(content, ensure_ascii=False)

        cur.execute("SELECT 1 FROM app_templates WHERE tpl_id = %s", (tpl_id,))
        if cur.fetchone():
            if tpl_id in builtin_ids:
                cur.execute(
                    """UPDATE app_templates
                          SET name = %s, platform = %s, content = %s, is_builtin = 1
                        WHERE tpl_id = %s""",
                    (name, platform, content_json, tpl_id),
                )
                updated += 1
            continue

        cur.execute(
            """INSERT INTO app_templates (tpl_id, name, platform, is_builtin, content, created_at)
               VALUES (%s, %s, %s, %s, %s, %s)""",
            (tpl_id, name, platform, is_builtin,
             content_json,
             created_at or time.strftime("%Y-%m-%d %H:%M:%S")),
        )
        inserted += 1
    if inserted or updated:
        conn.commit()
        logger.info(f"模板同步完成: 新增 {inserted}, 更新 {updated} 个内置模板到 APP.app_templates")


def _migrate_oplog(cur, conn):
    if not _table_is_empty(cur, "app_oplog"):
        return
    json_path = Path(__file__).parent / "oplog.json"
    if not json_path.exists():
        return
    try:
        logs = json.loads(json_path.read_text(encoding="utf-8"))
    except Exception:
        return
    if not isinstance(logs, list):
        return
    count = 0
    for entry in logs:
        target_raw = entry.get("target", "")
        target_type = ""
        target_id = ""
        platform = ""
        if "TikTok" in target_raw:
            platform = "tiktok"
        elif "Meta" in target_raw:
            platform = "meta"
        if "广告系列" in target_raw:
            target_type = "campaign"
            nums = [p for p in target_raw.split() if p.isdigit()]
            target_id = nums[0] if nums else ""
        elif "广告组" in target_raw:
            target_type = "adgroup"
            nums = [p for p in target_raw.split() if p.isdigit()]
            target_id = nums[0] if nums else ""
        else:
            target_type = target_raw

        cur.execute(
            """INSERT INTO app_oplog (username, action, target_type, target_id, platform, created_at)
               VALUES (%s, %s, %s, %s, %s, %s)""",
            (
                entry.get("user", ""),
                entry.get("action", ""),
                target_type,
                target_id,
                platform,
                entry.get("time", time.strftime("%Y-%m-%d %H:%M:%S")),
            ),
        )
        count += 1
    conn.commit()
    logger.info(f"已从 oplog.json 迁移 {count} 条日志到 APP.app_oplog")
