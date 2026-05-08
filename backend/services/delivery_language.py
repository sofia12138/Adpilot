"""投放语种 → 平台原生语言定向映射

模板母版统一以 ISO-639-1 / BCP-47 风格的代码（如 "en"、"zh-Hant"）声明 delivery_languages，
launch 时按本表映射成 Meta locales（整数 ID）/ TikTok languages（小写 2 位代码）。

设计原则：
1. **强制语言定向**：投放语种为本系统的核心控制项，缺失或映射不存在时必须立刻报错。
2. **向后兼容**：模板缺少 delivery_languages / default_delivery_language 时按 ["en"] / "en" 兜底。
3. **静态映射 + 注释**：Meta locale ID 来自 FB Locales API 常用映射，业务上线后可由运维直接增补本表。
"""
from __future__ import annotations

from typing import Iterable

# ── 默认值 ────────────────────────────────────────────────

DEFAULT_DELIVERY_LANGUAGES: list[str] = ["en"]
DEFAULT_DELIVERY_LANGUAGE: str = "en"

# 本系统支持的语种代码（与前端常量保持一致）
#
# 设计：每个 code 严格对应 *一个* Meta locale ID（保证后台只显示一条），
# 多地区语言提供「全部」聚合 code（如 en）+ 各国家变体 code（如 en-US / en-GB），
# 用户在模板内可任选粒度，例如同时勾选 en-US + en-GB 也合法。
SUPPORTED_LANGUAGE_CODES: list[str] = [
    "en", "en-US", "en-GB",
    "es", "es-ES",
    "pt", "pt-BR", "pt-PT",
    "fr", "fr-FR", "fr-CA",
    "de", "it", "nl", "ru", "ar", "ja", "ko", "id", "th", "vi",
    "zh-Hant", "zh-HK", "zh-Hans",
]

SUPPORTED_LANGUAGE_LABELS: dict[str, str] = {
    "en":      "英语（全部）",
    "en-US":   "英语（美国）",
    "en-GB":   "英语（英国）",
    "es":      "西班牙语",
    "es-ES":   "西班牙语（西班牙）",
    "pt":      "葡萄牙语（全部）",
    "pt-BR":   "葡萄牙语（巴西）",
    "pt-PT":   "葡萄牙语（葡萄牙）",
    "fr":      "法语（全部）",
    "fr-FR":   "法语（法国）",
    "fr-CA":   "法语（加拿大）",
    "de":      "德语",
    "it":      "意大利语",
    "nl":      "荷兰语",
    "ru":      "俄语",
    "ar":      "阿拉伯语",
    "ja":      "日语",
    "ko":      "韩语",
    "id":      "印尼语",
    "th":      "泰语",
    "vi":      "越南语",
    "zh-Hant": "繁体中文（台湾）",
    "zh-HK":   "繁体中文（香港）",
    "zh-Hans": "简体中文",
}


# ── Meta：locales 为整数 ID 列表 ──
# ID 由 Facebook Graph /search?type=adlocale 实时返回，下列值已用 META_ACCESS_TOKEN
# 在生产环境拉取并核对（v22.0，2026-05-08）。一旦 Graph 报 "Invalid locale id"，
# 请重新调用 /search?type=adlocale 校准。
#
# 1:1 映射：每个 code 对应 *一个* locale ID，确保 Ads Manager 后台只显示一条。
# 多地区语言提供「语言全部」聚合 ID（1001~1005）+ 各国家变体 ID 双轨可选。
#
# ⚠️ 切勿凭印象修改这些 ID：错位会导致广告投放到完全不同的语种（例如 nl=13 实际是
# 「挪威语 nb_NO」而非「荷兰语 nl_NL」）。增减语种时也务必先用 Graph API 验证。
META_LOCALES: dict[str, list[int]] = {
    "en":      [1001],          # 英语（全部，覆盖所有英语地区）
    "en-US":   [6],             # 英语（美国）
    "en-GB":   [24],            # 英语（英国）
    "es":      [23],            # 西班牙语（默认 / 拉美等地区）
    "es-ES":   [7],             # 西班牙语（西班牙）
    "pt":      [1005],          # 葡萄牙语（所有，覆盖巴西+葡萄牙）
    "pt-BR":   [16],            # 葡萄牙语（巴西）
    "pt-PT":   [31],            # 葡萄牙语（葡萄牙）
    "fr":      [1003],          # 法语（全部）
    "fr-FR":   [9],             # 法语（法国）
    "fr-CA":   [44],            # 法语（加拿大）
    "de":      [5],             # 德语
    "it":      [10],            # 意大利语
    "nl":      [14],            # 荷兰语
    "ru":      [17],            # 俄语
    "ar":      [28],            # 阿拉伯语
    "ja":      [11],            # 日语
    "ko":      [12],            # 韩语
    "id":      [25],            # 印度尼西亚语
    "th":      [35],            # 泰语
    "vi":      [27],            # 越南语
    # 中文不暴露 1004「中文（全部）」，因为我们刻意把繁/简拆成独立 code 精确投放；
    # 1004 会同时覆盖简繁，与 zh-Hant/zh-HK/zh-Hans 的语义冲突。
    "zh-Hant": [22],             # 繁体中文（台湾）
    "zh-HK":   [21],             # 繁体中文（香港）
    "zh-Hans": [20],             # 简体中文（中国）
}


# ── TikTok：languages 为小写 ISO-639-1 字符串列表（Marketing API targeting languages） ──
# TikTok 不细分国家变体（en-US 与 en-GB 都映射为 "en"），也不区分繁简（统一 "zh"）。
TIKTOK_LANGUAGES: dict[str, list[str]] = {
    "en":      ["en"],
    "en-US":   ["en"],
    "en-GB":   ["en"],
    "es":      ["es"],
    "es-ES":   ["es"],
    "pt":      ["pt"],
    "pt-BR":   ["pt"],
    "pt-PT":   ["pt"],
    "fr":      ["fr"],
    "fr-FR":   ["fr"],
    "fr-CA":   ["fr"],
    "de":      ["de"],
    "it":      ["it"],
    "nl":      ["nl"],
    "ru":      ["ru"],
    "ar":      ["ar"],
    "ja":      ["ja"],
    "ko":      ["ko"],
    "id":      ["id"],
    "th":      ["th"],
    "vi":      ["vi"],
    "zh-Hant": ["zh"],
    "zh-HK":   ["zh"],
    "zh-Hans": ["zh"],
}


# ── 异常 ──────────────────────────────────────────────────

class DeliveryLanguageError(ValueError):
    """投放语种相关错误：不在允许列表 / 平台映射缺失 / 缺字段。"""


# ── 工具函数 ──────────────────────────────────────────────

def _clean_codes(codes: Iterable[str] | None) -> list[str]:
    """去空白、去空串、保留输入顺序 + 去重。"""
    if not codes:
        return []
    seen: set[str] = set()
    cleaned: list[str] = []
    for c in codes:
        if not isinstance(c, str):
            continue
        c = c.strip()
        if not c or c in seen:
            continue
        seen.add(c)
        cleaned.append(c)
    return cleaned


def normalize_template_languages(content: dict) -> dict:
    """将模板 content 中的 delivery_languages / default_delivery_language 规范化为合法值。

    规则：
      - delivery_languages 为空 → 落回 ["en"]
      - default_delivery_language 为空或不在 delivery_languages 中 → 取 delivery_languages[0]
    本函数会原地修改 content，并返回它。
    """
    if not isinstance(content, dict):
        return content
    langs = _clean_codes(content.get("delivery_languages"))
    if not langs:
        langs = list(DEFAULT_DELIVERY_LANGUAGES)
    default = content.get("default_delivery_language")
    if not isinstance(default, str) or not default.strip() or default not in langs:
        default = langs[0]
    content["delivery_languages"] = langs
    content["default_delivery_language"] = default
    return content


def validate_selected(selected: str | None, allowed: list[str]) -> str:
    """校验用户本次选择的投放语种合法性，返回经 strip 的最终值。"""
    if not selected or not isinstance(selected, str) or not selected.strip():
        raise DeliveryLanguageError("缺少 selected_delivery_language，必须选择本次投放语种")
    selected = selected.strip()
    allowed = _clean_codes(allowed) or list(DEFAULT_DELIVERY_LANGUAGES)
    if selected not in allowed:
        raise DeliveryLanguageError(
            f"投放语种 '{selected}' 不在模板允许范围 {allowed} 内"
        )
    return selected


def resolve_meta_locales(code: str) -> list[int]:
    """返回 Meta targeting.locales 列表（整数 ID）。"""
    locales = META_LOCALES.get(code)
    if not locales:
        raise DeliveryLanguageError(
            f"Meta 不支持投放语种 '{code}'，请在模板的 delivery_languages 中移除该项，"
            f"或在 backend/services/delivery_language.py 的 META_LOCALES 中补充映射"
        )
    return list(locales)


def resolve_tiktok_languages(code: str) -> list[str]:
    """返回 TikTok adgroup.languages 列表（小写 ISO-639-1）。"""
    langs = TIKTOK_LANGUAGES.get(code)
    if not langs:
        raise DeliveryLanguageError(
            f"TikTok 不支持投放语种 '{code}'，请在模板的 delivery_languages 中移除该项，"
            f"或在 backend/services/delivery_language.py 的 TIKTOK_LANGUAGES 中补充映射"
        )
    return list(langs)
