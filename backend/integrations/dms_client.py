"""dms_client.py — 阿里云 DMS Enterprise OpenAPI 封装

为什么需要：
    metis 数仓在 DMS 安全规则下，CK (dbId=79572320) 和 MaxCompute (dbId=80154230)
    都通过 DMS ExecuteScript 接口对外提供 SQL 查询。直接走 pyodps 只能到 MC，
    且依赖项目级 ACL；走 DMS 一条 API 同时兼容 CK + MC，更通用。

提供：
    DmsClient.execute(sql, db_id)             同步执行单条 SQL，返回 ExecuteResult
    DmsClient.execute_scalar(sql, db_id)      返回首行首列的值（COUNT/MAX 等便利接口）
    DmsClient.execute_rows(sql, db_id)        生成器 yield 每行 dict（业务层友好）
    DmsClient.execute_paged_iter(sql, db_id, page_size)
                                              用 LIMIT/OFFSET 分页流式拉取
    error class:
        DmsError              所有 DMS 异常基类
        DmsTransportError     网络/SDK 异常（可重试）
        DmsApiError           DMS API 顶层 Success=false（可能是参数错）
        DmsAuthError          权限不足（无库/无表 SELECT）
        DmsSqlError           SQL 执行失败但有 ErrorMessage（如语法错、表不存在）

约定：
- 仅对 DmsTransportError 做指数退避重试，权限/SQL 错误立即抛出
- ColumnNames 字段顺序 = ResultSet 列顺序；Rows 列表里每个 dict 的键就是列名
- 数值类型在 DMS 返回里都是 string（如 "741"），调用方按需转换
"""
from __future__ import annotations

import logging
import re
import time
from threading import Lock
from typing import Any, Iterable, Iterator, Optional

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
#  异常类
# ─────────────────────────────────────────────────────────────
class DmsError(Exception):
    """DMS 调用相关异常的基类"""


class DmsTransportError(DmsError):
    """网络/SDK 异常（可重试）"""


class DmsApiError(DmsError):
    """DMS API 顶层 Success=false（参数错、签名错等）"""


class DmsAuthError(DmsError):
    """SQL 执行被 DMS 安全规则拒绝（无库/无表 SELECT）"""


class DmsSqlError(DmsError):
    """SQL 执行失败但通过了权限校验（语法错、引用不存在的列等）"""


# 触发 DmsAuthError 的关键字（错误信息里出现这些就归类为权限错）
_PERMISSION_HINTS = (
    "无库[",
    "无表[",
    "查询权限",
    "no permission",
    "noPermission",
    "permission denied",
)


# ─────────────────────────────────────────────────────────────
#  ExecuteResult: 包装单条 SQL 的结果集
# ─────────────────────────────────────────────────────────────
class ExecuteResult:
    """单条 SQL 的结果集封装"""

    __slots__ = ("columns", "rows", "row_count", "request_id", "raw_message")

    def __init__(
        self,
        columns: list[str],
        rows: list[dict[str, Any]],
        row_count: int,
        request_id: str = "",
        raw_message: str = "",
    ):
        self.columns = columns
        self.rows = rows
        self.row_count = row_count
        self.request_id = request_id
        self.raw_message = raw_message

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"<ExecuteResult cols={self.columns} rows={self.row_count} "
            f"req={self.request_id}>"
        )

    def first_value(self) -> Any:
        if not self.rows or not self.columns:
            return None
        return self.rows[0].get(self.columns[0])


# ─────────────────────────────────────────────────────────────
#  DmsClient
# ─────────────────────────────────────────────────────────────
class DmsClient:
    """阿里云 DMS Enterprise ExecuteScript 客户端 (Tea SDK 封装)。

    Lazy 初始化，全进程单例：第一次 execute 才建 SDK Client；后续复用。
    在 APScheduler 多线程环境下安全（SDK Client 内部线程安全 + lock 守护初始化）。
    """

    def __init__(
        self,
        access_key_id: str,
        access_key_secret: str,
        endpoint: str,
        max_retries: int = 3,
        retry_backoff_sec: float = 1.5,
    ):
        if not access_key_id or not access_key_secret:
            raise DmsError(
                "DmsClient 缺少 access_key_id/access_key_secret，请检查 .env"
            )
        self._ak_id = access_key_id
        self._ak_secret = access_key_secret
        self._endpoint = endpoint
        self._max_retries = max(1, max_retries)
        self._backoff = retry_backoff_sec
        self._sdk_client: Any = None
        self._sdk_models: Any = None
        self._lock = Lock()

    # ------------------------------------------------------------
    # SDK 懒加载
    # ------------------------------------------------------------
    def _get_sdk(self):
        if self._sdk_client is not None:
            return self._sdk_client, self._sdk_models
        with self._lock:
            if self._sdk_client is not None:
                return self._sdk_client, self._sdk_models
            try:
                from alibabacloud_dms_enterprise20181101.client import (
                    Client as DmsApiClient,
                )
                from alibabacloud_dms_enterprise20181101 import (
                    models as dms_models,
                )
                from alibabacloud_tea_openapi import models as openapi_models
            except ImportError as exc:
                raise DmsError(
                    "未安装阿里云 DMS SDK，请执行：\n"
                    "  pip install alibabacloud-dms-enterprise20181101 "
                    "alibabacloud-tea-openapi alibabacloud-tea-util"
                ) from exc
            cfg = openapi_models.Config(
                access_key_id=self._ak_id,
                access_key_secret=self._ak_secret,
                endpoint=self._endpoint,
            )
            self._sdk_client = DmsApiClient(cfg)
            self._sdk_models = dms_models
            logger.debug(
                "DMS SDK client 初始化完毕：endpoint=%s ak_id=%s***",
                self._endpoint, self._ak_id[:6],
            )
            return self._sdk_client, self._sdk_models

    # ------------------------------------------------------------
    # 主接口
    # ------------------------------------------------------------
    def execute(
        self,
        sql: str,
        db_id: int,
        *,
        logic: bool = False,
    ) -> ExecuteResult:
        """同步执行单条 SQL，返回 ExecuteResult。

        会按 max_retries 对 DmsTransportError 做指数退避重试；
        DmsAuthError / DmsSqlError / DmsApiError 立即抛出不重试。
        """
        last_exc: Optional[Exception] = None
        for attempt in range(1, self._max_retries + 1):
            try:
                return self._execute_once(sql, db_id, logic=logic)
            except DmsTransportError as e:
                last_exc = e
                if attempt < self._max_retries:
                    sleep = self._backoff * (2 ** (attempt - 1))
                    logger.warning(
                        "DMS execute 网络异常 attempt=%d/%d sleep=%.1fs: %s",
                        attempt, self._max_retries, sleep, e,
                    )
                    time.sleep(sleep)
                else:
                    raise
        # 理论不会到这里，但 type checker 满意
        raise last_exc if last_exc else DmsTransportError("execute retry exhausted")

    def execute_scalar(
        self,
        sql: str,
        db_id: int,
        *,
        logic: bool = False,
    ) -> Any:
        """返回结果集的首行首列。空结果返回 None。"""
        res = self.execute(sql, db_id, logic=logic)
        return res.first_value()

    def execute_rows(
        self,
        sql: str,
        db_id: int,
        *,
        logic: bool = False,
    ) -> Iterator[dict[str, Any]]:
        """流式 yield 每行 dict（实际为内存中一次性加载，仅是接口语义友好）。

        DMS ExecuteScript 是 sync 模式，本身没有真正的流式分块；
        如果数据量大，请改用 execute_paged_iter() 分页拉取。
        """
        res = self.execute(sql, db_id, logic=logic)
        for row in res.rows:
            yield row

    def execute_paged_iter(
        self,
        sql_template: str,
        db_id: int,
        *,
        page_size: int = 5000,
        max_pages: int = 10000,
        logic: bool = False,
    ) -> Iterator[list[dict[str, Any]]]:
        """用 LIMIT/OFFSET 分页拉取，每次 yield 一批 rows。

        sql_template 必须**不包含** LIMIT/OFFSET，本方法会自动追加：
            <sql_template> LIMIT <page_size> OFFSET <offset>

        当某一页返回行数 < page_size 时停止迭代。
        max_pages 是安全阈值，防止 SQL 写错时无限拉。
        """
        if re.search(r"\bLIMIT\b", sql_template, flags=re.IGNORECASE):
            raise DmsSqlError(
                "execute_paged_iter() 的 sql_template 不能自带 LIMIT，"
                "请在外层去掉 LIMIT 子句"
            )
        offset = 0
        for page in range(max_pages):
            paged_sql = f"{sql_template.rstrip().rstrip(';')} LIMIT {page_size} OFFSET {offset}"
            res = self.execute(paged_sql, db_id, logic=logic)
            if not res.rows:
                break
            yield res.rows
            if len(res.rows) < page_size:
                break
            offset += page_size
        else:  # pragma: no cover
            raise DmsSqlError(
                f"execute_paged_iter 触达 max_pages={max_pages} 安全上限"
            )

    # ------------------------------------------------------------
    # 内部
    # ------------------------------------------------------------
    def _execute_once(
        self,
        sql: str,
        db_id: int,
        *,
        logic: bool = False,
    ) -> ExecuteResult:
        client, models = self._get_sdk()

        try:
            req = models.ExecuteScriptRequest(
                db_id=db_id,
                script=sql,
                logic=logic,
            )
            resp = client.execute_script(req)
        except Exception as e:
            raise DmsTransportError(
                f"DMS ExecuteScript SDK 异常: {e!r}"
            ) from e

        body = resp.body
        body_dict: dict[str, Any] = (
            body.to_map() if hasattr(body, "to_map") else {"raw": str(body)}
        )

        request_id = body_dict.get("RequestId") or ""
        api_success = bool(body_dict.get("Success"))
        api_error_msg = body_dict.get("ErrorMessage") or ""
        results = body_dict.get("Results")

        if not api_success:
            raise DmsApiError(
                f"DMS API 顶层失败 RequestId={request_id} ErrorMessage={api_error_msg!r}"
            )

        # Results 实际上是数组，每条 SQL 一条 ExecuteResult
        if not isinstance(results, list) or not results:
            raise DmsApiError(
                f"DMS Results 为空或格式异常 RequestId={request_id} body={body_dict!r}"
            )

        # 仅取第一条（execute_script 默认单 SQL；多 SQL 场景请显式拆分调用）
        first = results[0] if isinstance(results[0], dict) else {}
        stmt_success = first.get("Success", api_success)
        stmt_message = (first.get("Message") or "").strip()
        columns: list[str] = list(first.get("ColumnNames") or [])
        rows: list[dict[str, Any]] = list(first.get("Rows") or [])
        row_count = int(first.get("RowCount") or 0)

        if not stmt_success:
            if any(hint in stmt_message for hint in _PERMISSION_HINTS):
                raise DmsAuthError(
                    f"DMS 权限拒绝 db_id={db_id} RequestId={request_id} msg={stmt_message!r}"
                )
            raise DmsSqlError(
                f"DMS SQL 执行失败 db_id={db_id} RequestId={request_id} msg={stmt_message!r}"
            )

        return ExecuteResult(
            columns=columns,
            rows=rows,
            row_count=row_count,
            request_id=request_id,
            raw_message=stmt_message,
        )


# ─────────────────────────────────────────────────────────────
#  全局单例
# ─────────────────────────────────────────────────────────────
_default_client: Optional[DmsClient] = None
_default_lock = Lock()


def get_default_client() -> DmsClient:
    """从 settings 取配置，懒构造全局 DmsClient。

    优先用 dms_access_key_id / dms_access_key_secret，未填则 fallback 到
    odps_access_key_id / odps_access_key_secret（同一 RAM 用户）。
    """
    global _default_client
    if _default_client is not None:
        return _default_client
    with _default_lock:
        if _default_client is not None:
            return _default_client
        from config import get_settings  # 懒 import 避开循环
        s = get_settings()
        ak_id = s.dms_access_key_id or s.odps_access_key_id
        ak_secret = s.dms_access_key_secret or s.odps_access_key_secret
        _default_client = DmsClient(
            access_key_id=ak_id,
            access_key_secret=ak_secret,
            endpoint=s.dms_endpoint,
            max_retries=s.dms_max_retries,
            retry_backoff_sec=s.dms_retry_backoff_sec,
        )
        return _default_client


def reset_default_client() -> None:
    """测试/重载用：清空单例，下次 get_default_client 重建"""
    global _default_client
    with _default_lock:
        _default_client = None
