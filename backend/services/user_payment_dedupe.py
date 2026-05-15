"""按 trade_no 维度的订单去重 — sync 任务与实时层共用

背景
----
苹果 IAP / 谷歌 Play 续订回调存在一个已知缺陷：同一笔真实交易（同一个
trade_no / origin_transaction_id）有时会被"按 subscribe_count 1..N 拆行"
方式写入 recharge_order，且 user_id 有时错位到当时活跃的另一个账号。

线上观测（2026-05 近 14 天）：发现 16 组 trade_no 重复，共 40 笔虚增订单
/ $639.60 虚增 GMV。典型案例：trade_no=180003454541748 一笔 $19.99 的
VIP 订阅，被复制成 9 条订单（8 条挂到 user 10904，1 条留在真实买家
user 10890 头上）。

去重规则
--------
- order_status == 1 (已支付) 且 trade_no 非空时，按 trade_no 分组，每组
  保留**一笔** canonical row：
    1. first_subscribe = 1 (首订) 优先
    2. 否则 order_id (PolarDB.recharge_order.id) 最小那条 = 最早写入
- 其他状态（待支付/取消/退款/支付失败）或 trade_no 为空：保持原样不动
  - 待支付订单 trade_no 多为空，不算重复
  - 退款/取消行属于状态变更，与原始已支付行 order_no 不同，不去重

输入字段约定（缺失时按 0/空处理）
--------------------------------
- order_status: int
- trade_no:     str | None
- first_subscribe: int (0/1)
- order_id 或 id: int (PolarDB 主键)
"""
from __future__ import annotations

import logging
from typing import Iterable

logger = logging.getLogger(__name__)


def _pick_better(curr: dict, prev: dict) -> dict:
    """同 trade_no 组内，返回应保留的 canonical row。

    优先级：first_subscribe=1 > order_id 最小
    """
    prev_fs = int(prev.get("first_subscribe") or 0)
    curr_fs = int(_g(curr, "first_subscribe") or 0)
    if curr_fs != prev_fs:
        return curr if curr_fs > prev_fs else prev

    prev_id = _id_of(prev)
    curr_id = _id_of(curr)
    if curr_id and (prev_id == 0 or curr_id < prev_id):
        return curr
    return prev


def _id_of(row: dict) -> int:
    """统一拿 PolarDB 主键 id。sync 任务里叫 order_id，realtime 是 id。"""
    v = row.get("order_id") if row.get("order_id") is not None else row.get("id")
    try:
        return int(v or 0)
    except (TypeError, ValueError):
        return 0


def _g(row: dict, key: str):
    """容错读取（None / 缺失键统一返回 None）"""
    return row.get(key)


def dedupe_orders_by_trade_no(
    orders: Iterable[dict],
) -> tuple[list[dict], int]:
    """按 trade_no 去重已支付订单。

    返回:
        (deduped_rows, dropped_count)
            deduped_rows: 去重后的订单行列表（顺序未保证）
            dropped_count: 被剔除的虚增订单笔数
    """
    keep_by_trade: dict[str, dict] = {}
    no_dedupe: list[dict] = []
    paid_with_trade = 0

    for o in orders:
        status = int(o.get("order_status") or 0)
        trade_no_raw = o.get("trade_no")
        trade_no = (str(trade_no_raw).strip() if trade_no_raw else "")
        if status != 1 or not trade_no:
            no_dedupe.append(o)
            continue
        paid_with_trade += 1
        prev = keep_by_trade.get(trade_no)
        if prev is None:
            keep_by_trade[trade_no] = o
        else:
            keep_by_trade[trade_no] = _pick_better(o, prev)

    out = no_dedupe + list(keep_by_trade.values())
    dropped = paid_with_trade - len(keep_by_trade)
    if dropped:
        logger.warning(
            "trade_no 去重剔除 %d 笔虚增订单（已支付有 trade_no 行 %d → 保留 %d）",
            dropped, paid_with_trade, len(keep_by_trade),
        )
    return out, dropped
