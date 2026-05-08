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
SUPPORTED_LANGUAGE_CODES: list[str] = [
    "en", "es", "pt", "fr", "de", "it", "nl", "ru",
    "ar", "ja", "ko", "id", "th", "vi", "zh-Hant", "zh-Hans",
]

SUPPORTED_LANGUAGE_LABELS: dict[str, str] = {
    "en":      "英语 (English)",
    "es":      "西班牙语 (Español)",
    "pt":      "葡萄牙语 (Português)",
    "fr":      "法语 (Français)",
    "de":      "德语 (Deutsch)",
    "it":      "意大利语 (Italiano)",
    "nl":      "荷兰语 (Nederlands)",
    "ru":      "俄语 (Русский)",
    "ar":      "阿拉伯语 (العربية)",
    "ja":      "日语 (日本語)",
    "ko":      "韩语 (한국어)",
    "id":      "印尼语 (Bahasa Indonesia)",
    "th":      "泰语 (ไทย)",
    "vi":      "越南语 (Tiếng Việt)",
    "zh-Hant": "繁体中文 (繁體中文)",
    "zh-Hans": "简体中文 (简体中文)",
}


# ── Meta：locales 为整数 ID 列表（来自 Facebook /search?type=adlocale） ──
# 一种语种可由多组 locale 共同覆盖；下列 ID 为 Meta 公开文档与广告主长期沿用的常见值，
# 上线后如发现 Graph 报 "Invalid locale id"，请在 ops 侧用最新 /search?type=adlocale 替换。
META_LOCALES: dict[str, list[int]] = {
    "en":      [6, 24],         # 6=en_US, 24=en_GB
    "es":      [7, 23],         # 7=es_ES, 23=es_LA
    "pt":      [19, 20],        # 19=pt_BR, 20=pt_PT
    "fr":      [8, 9],          # 8=fr_FR, 9=fr_CA
    "de":      [5],             # 5=de_DE
    "it":      [11],            # 11=it_IT
    "nl":      [13],            # 13=nl_NL
    "ru":      [25],            # 25=ru_RU
    "ar":      [3],             # 3=ar_AR
    "ja":      [27],            # 27=ja_JP
    "ko":      [29],            # 29=ko_KR
    "id":      [12],            # 12=id_ID
    "th":      [4],             # 4=th_TH
    "vi":      [22],            # 22=vi_VN
    "zh-Hant": [32, 33],        # 32=zh_TW, 33=zh_HK
    "zh-Hans": [31],            # 31=zh_CN
}


# ── TikTok：languages 为小写 ISO-639-1 字符串列表（Marketing API targeting languages） ──
# TikTok 不区分繁简，统一映射为 "zh"；其余按标准两位代码。
TIKTOK_LANGUAGES: dict[str, list[str]] = {
    "en":      ["en"],
    "es":      ["es"],
    "pt":      ["pt"],
    "fr":      ["fr"],
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
