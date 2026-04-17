from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    tiktok_app_id: str = ""
    tiktok_app_secret: str = ""
    tiktok_access_token: str = ""
    tiktok_advertiser_id: str = ""
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
