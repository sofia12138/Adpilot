from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    tiktok_app_id: str = ""
    tiktok_app_secret: str = ""
    tiktok_access_token: str = ""
    tiktok_advertiser_id: str = ""
    # Business Center ID（可选）：配置后 /api/advertisers/ 会额外拉 BC 下全部子账户并 union
    tiktok_bc_id: str = ""
    tiktok_api_base_url: str = "https://business-api.tiktok.com/open_api/v1.3"

    # 旧配置（保留兼容，作为 PRD 默认值）
    mysql_host: str = ""
    mysql_port: int = 3306
    mysql_user: str = ""
    mysql_password: str = ""
    mysql_database: str = ""

    # 产研库 PRD（只读）
    prd_mysql_host: str = ""
    prd_mysql_port: int = 3306
    prd_mysql_user: str = ""
    prd_mysql_password: str = ""
    prd_mysql_database: str = ""

    # 应用库 APP（读写）
    app_mysql_host: str = ""
    app_mysql_port: int = 3306
    app_mysql_user: str = ""
    app_mysql_password: str = ""
    app_mysql_database: str = ""

    # 业务库 BIZ（读写，第二阶段）
    biz_mysql_host: str = ""
    biz_mysql_port: int = 3306
    biz_mysql_user: str = ""
    biz_mysql_password: str = ""
    biz_mysql_database: str = ""

    # 订单库 ORDER（只读，业务订单原始表 = 运营面板付费侧真值源）
    # 用于把付费侧从 MaxCompute (T+1) 直连到业务库（实时 + 真值）
    order_mysql_host: str = ""
    order_mysql_port: int = 3306
    order_mysql_user: str = ""
    order_mysql_password: str = ""
    order_mysql_database: str = ""

    # 归因数仓 MaxCompute（用于拉取 metis_dw.ads_ad_delivery_di 等 ADS 层）
    odps_access_key_id: str = ""
    odps_access_key_secret: str = ""
    odps_endpoint: str = "https://service.us-west-1.maxcompute.aliyun.com/api"
    odps_project: str = "metis_dw"
    odps_tunnel_endpoint: str = ""  # 可选；不填使用默认

    # 阿里云 DMS Enterprise OpenAPI（用于 ClickHouse 实时同步 + MaxCompute 回退查询）
    # 鉴权复用 odps_access_key_id / odps_access_key_secret（同一 RAM 用户）
    # 也可独立指定 dms_access_key_id / dms_access_key_secret 覆盖
    dms_access_key_id: str = ""
    dms_access_key_secret: str = ""
    dms_endpoint: str = "dms-enterprise.us-west-1.aliyuncs.com"
    dms_ck_db_id: int = 79572320       # metis (ClickHouse)
    dms_mc_db_id: int = 80154230       # metis_dw (MaxCompute)
    dms_max_retries: int = 3
    dms_retry_backoff_sec: float = 1.5
    # 30min 自动同步任务总开关（默认 disabled，环境变量 ENABLE_CK_INTRADAY_SYNC=1 开启）
    enable_ck_intraday_sync: bool = False

    # 归因数据接入 11 视图：默认数据源（auto 模式下生效）
    # 取值：blend / attribution / legacy
    #   blend       — normalized 投放指标 + attribution 真值业务结果（推荐生产口径，默认）
    #   attribution — 纯归因表（biz_attribution_ad_daily/intraday，TikTok spend 当前不全）
    #   legacy      — 纯 normalized 表（旧口径，spend 完整但 revenue 失真）
    # 前端 ?source=blend|attribution|legacy 可强制指定，不传则按 auto 取这里的默认值
    data_source_default: str = "blend"

    # 兼容旧 env：仅当 data_source_default 未显式设置时才考虑 attribution_primary
    # （True → attribution，False → legacy；不影响 blend）
    attribution_primary: bool = False

    meta_app_id: str = ""
    meta_app_secret: str = ""
    meta_access_token: str = ""
    meta_ad_account_id: str = ""
    meta_api_base_url: str = "https://graph.facebook.com/v21.0"

    jwt_secret_key: str = "change-me-in-production"
    jwt_algorithm: str = "HS256"
    jwt_expire_hours: int = 24

    cors_origins: str = "*"

    server_host: str = "0.0.0.0"
    server_port: int = 8000

    model_config = {"env_file": "../.env", "env_file_encoding": "utf-8"}


@lru_cache
def get_settings() -> Settings:
    return Settings()
