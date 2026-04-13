/**
 * 单元测试：drama-name-parser.ts
 *
 * 覆盖场景：
 *  1. parseDramaName — 正常解析 (EN)/(ES)/（PT）/[FR]
 *  2. parseDramaName — 无语言尾缀
 *  3. parseDramaName — 空字符串输入
 *  4. parseDramaName — 未知括号内容不误识别
 *  5. parseMiniProgramCampaignName — 正好 10 段
 *  6. parseMiniProgramCampaignName — 第 11 段及以后有备注（含 ES 字符不误识别）
 *  7. parseMiniProgramCampaignName — 剧名有 (EN)
 *  8. parseMiniProgramCampaignName — 剧名无语言尾缀
 *  9. parseMiniProgramCampaignName — 字段数不足（< 10）→ failed
 * 10. parseMiniProgramCampaignName — 备注中包含 (EN) 不影响剧名解析
 * 11. parseAppCampaignName — 正常 10 段
 * 12. parseAppCampaignName — 11 段（有 buyerShortName）
 * 13. parseAppCampaignName — 12+ 段（有 buyerShortName + remark）
 * 14. parseAppCampaignName — 字段数不足（< 10）→ failed
 * 15. parseAppCampaignName — 备注中含 ES 不影响 languageCode
 * 16. normalizeContentKey — 转小写、去空格
 */

import { describe, it, expect } from 'vitest'
import {
  parseDramaName,
  parseMiniProgramCampaignName,
  parseAppCampaignName,
  normalizeContentKey,
} from '../drama-name-parser'

// ─────────────────────────────────────────────────────────────
// parseDramaName
// ─────────────────────────────────────────────────────────────
describe('parseDramaName', () => {
  it('正常解析半角括号 (EN)', () => {
    const r = parseDramaName('末日来袭，我靠缩小系统逆袭人生(EN)')
    expect(r.localizedDramaName).toBe('末日来袭，我靠缩小系统逆袭人生')
    expect(r.languageCode).toBe('en')
    expect(r.languageTagRaw).toBe('(EN)')
    expect(r.dramaNameRaw).toBe('末日来袭，我靠缩小系统逆袭人生(EN)')
  })

  it('正常解析全角括号 （ES）', () => {
    const r = parseDramaName('末日来袭，我靠缩小系统逆袭人生（ES）')
    expect(r.localizedDramaName).toBe('末日来袭，我靠缩小系统逆袭人生')
    expect(r.languageCode).toBe('es')
    expect(r.languageTagRaw).toBe('（ES）')
  })

  it('正常解析方括号 [PT]', () => {
    const r = parseDramaName('末日来袭[PT]')
    expect(r.localizedDramaName).toBe('末日来袭')
    expect(r.languageCode).toBe('pt')
    expect(r.languageTagRaw).toBe('[PT]')
  })

  it('JP 映射为 ja', () => {
    const r = parseDramaName('某剧名(JP)')
    expect(r.languageCode).toBe('ja')
  })

  it('JA 映射为 ja', () => {
    const r = parseDramaName('某剧名(JA)')
    expect(r.languageCode).toBe('ja')
  })

  it('KR 映射为 ko', () => {
    const r = parseDramaName('某剧名(KR)')
    expect(r.languageCode).toBe('ko')
  })

  it('无语言尾缀 → languageCode=unknown，localizedDramaName=原始内容', () => {
    const r = parseDramaName('末日来袭，我靠缩小系统逆袭人生')
    expect(r.languageCode).toBe('unknown')
    expect(r.localizedDramaName).toBe('末日来袭，我靠缩小系统逆袭人生')
    expect(r.languageTagRaw).toBeUndefined()
  })

  it('空字符串 → languageCode=unknown，localizedDramaName=""', () => {
    const r = parseDramaName('')
    expect(r.languageCode).toBe('unknown')
    expect(r.localizedDramaName).toBe('')
  })

  it('括号内容是未知代码 (XX) → 不识别，languageCode=unknown', () => {
    const r = parseDramaName('某剧名(XX)')
    expect(r.languageCode).toBe('unknown')
    expect(r.localizedDramaName).toBe('某剧名(XX)')
  })

  it('括号不在结尾 → 不识别', () => {
    const r = parseDramaName('(EN)某剧名')
    expect(r.languageCode).toBe('unknown')
    expect(r.localizedDramaName).toBe('(EN)某剧名')
  })

  it('大小写不敏感 — (en) 能识别', () => {
    const r = parseDramaName('某剧名(en)')
    expect(r.languageCode).toBe('en')
  })
})

// ─────────────────────────────────────────────────────────────
// parseMiniProgramCampaignName
// ─────────────────────────────────────────────────────────────
describe('parseMiniProgramCampaignName', () => {
  // 标准10段示例
  const BASE_NAME = '51-AIGC-US-小程序-TT-YXW-TROAS-0.9-20260309-末日来袭，我靠缩小系统逆袭人生(EN)'

  it('正好 10 段，正常解析', () => {
    const r = parseMiniProgramCampaignName(BASE_NAME)
    expect(r.parseStatus).toBe('ok')
    expect(r.dramaId).toBe('51')
    expect(r.dramaType).toBe('AIGC')
    expect(r.country).toBe('US')
    expect(r.sourceType).toBe('小程序')
    expect(r.mediaChannel).toBe('TT')
    expect(r.buyerName).toBe('YXW')
    expect(r.optimizationType).toBe('TROAS')
    expect(r.bidType).toBe('0.9')
    expect(r.publishDate).toBe('20260309')
    expect(r.dramaNameRaw).toBe('末日来袭，我靠缩小系统逆袭人生(EN)')
    expect(r.localizedDramaName).toBe('末日来袭，我靠缩小系统逆袭人生')
    expect(r.languageCode).toBe('en')
    expect(r.remarkRaw).toBeUndefined()
  })

  it('11 段（第 11 字段是单个备注）', () => {
    const r = parseMiniProgramCampaignName(BASE_NAME + '-新素材测试')
    expect(r.parseStatus).toBe('ok')
    expect(r.remarkRaw).toBe('新素材测试')
    // 剧名解析结果不受备注影响
    expect(r.localizedDramaName).toBe('末日来袭，我靠缩小系统逆袭人生')
    expect(r.languageCode).toBe('en')
  })

  it('第 11 段及以后多段备注，用 - 重新拼接', () => {
    const r = parseMiniProgramCampaignName(BASE_NAME + '-新素材测试-4月放量')
    expect(r.parseStatus).toBe('ok')
    expect(r.remarkRaw).toBe('新素材测试-4月放量')
  })

  it('备注中包含 (ES) 字符，不影响剧名的 languageCode', () => {
    // 第 10 字段剧名是 (EN)，第 11 字段备注含 (ES)
    const r = parseMiniProgramCampaignName(BASE_NAME + '-(ES)补量')
    expect(r.languageCode).toBe('en') // 只取第 10 字段
    expect(r.remarkRaw).toBe('(ES)补量')
  })

  it('备注中出现 EN 文字，不影响剧名解析结果', () => {
    const r = parseMiniProgramCampaignName(
      '51-AIGC-US-小程序-TT-YXW-TROAS-0.9-20260309-某剧名-EN版本追投'
    )
    expect(r.parseStatus).toBe('ok')
    expect(r.localizedDramaName).toBe('某剧名') // 第10字段无语言尾缀
    expect(r.languageCode).toBe('unknown')
    expect(r.remarkRaw).toBe('EN版本追投') // 备注只保留
  })

  it('剧名无语言尾缀 → languageCode=unknown', () => {
    const r = parseMiniProgramCampaignName(
      '51-AIGC-US-小程序-TT-YXW-TROAS-0.9-20260309-末日来袭无尾缀剧'
    )
    expect(r.parseStatus).toBe('ok')
    expect(r.languageCode).toBe('unknown')
    expect(r.localizedDramaName).toBe('末日来袭无尾缀剧')
  })

  it('字段数不足（9 段）→ parseStatus=failed', () => {
    const r = parseMiniProgramCampaignName('51-AIGC-US-小程序-TT-YXW-TROAS-0.9-20260309')
    expect(r.parseStatus).toBe('failed')
    expect(r.parseError).toMatch(/字段数不足/)
  })

  it('字段数不足（1 段）→ parseStatus=failed', () => {
    const r = parseMiniProgramCampaignName('某活动名')
    expect(r.parseStatus).toBe('failed')
  })

  it('campaignNameRaw 始终保留原始字符串', () => {
    const raw = '短-名'
    const r = parseMiniProgramCampaignName(raw)
    expect(r.campaignNameRaw).toBe(raw)
  })
})

// ─────────────────────────────────────────────────────────────
// parseAppCampaignName
// ─────────────────────────────────────────────────────────────
describe('parseAppCampaignName', () => {
  const BASE_APP = 'w2a-2-31-51-4810-9-7-sofia0408-tt-末世来临，我凭诺亚方舟硬抗危机(EN)'

  it('正好 10 段，正常解析', () => {
    const r = parseAppCampaignName(BASE_APP)
    expect(r.parseStatus).toBe('ok')
    expect(r.field1).toBe('w2a')
    expect(r.dramaNameRaw).toBe('末世来临，我凭诺亚方舟硬抗危机(EN)')
    expect(r.localizedDramaName).toBe('末世来临，我凭诺亚方舟硬抗危机')
    expect(r.languageCode).toBe('en')
    expect(r.buyerShortName).toBeUndefined()
    expect(r.remarkRaw).toBeUndefined()
  })

  it('11 段 → buyerShortName 有值，无 remark', () => {
    const r = parseAppCampaignName(BASE_APP + '-YXW')
    expect(r.parseStatus).toBe('ok')
    expect(r.buyerShortName).toBe('YXW')
    expect(r.remarkRaw).toBeUndefined()
  })

  it('12+ 段 → buyerShortName + remark 多段拼接', () => {
    const r = parseAppCampaignName(BASE_APP + '-YXW-老素材复投')
    expect(r.parseStatus).toBe('ok')
    expect(r.buyerShortName).toBe('YXW')
    expect(r.remarkRaw).toBe('老素材复投')
  })

  it('remark 多段拼接', () => {
    const r = parseAppCampaignName(BASE_APP + '-YXW-老素材复投-4月增量')
    expect(r.remarkRaw).toBe('老素材复投-4月增量')
  })

  it('备注中含 ES 标记不影响 languageCode', () => {
    const r = parseAppCampaignName(BASE_APP + '-YXW-(ES)复投')
    expect(r.languageCode).toBe('en') // 只取第 10 字段
    expect(r.remarkRaw).toBe('(ES)复投')
  })

  it('字段数不足（< 10）→ parseStatus=failed', () => {
    const r = parseAppCampaignName('w2a-2-31-51-4810-9-7-sofia0408')
    expect(r.parseStatus).toBe('failed')
    expect(r.parseError).toMatch(/字段数不足/)
  })

  it('campaignNameRaw 始终保留原始字符串', () => {
    const raw = 'short'
    const r = parseAppCampaignName(raw)
    expect(r.campaignNameRaw).toBe(raw)
  })
})

// ─────────────────────────────────────────────────────────────
// normalizeContentKey
// ─────────────────────────────────────────────────────────────
describe('normalizeContentKey', () => {
  it('转小写并 trim', () => {
    expect(normalizeContentKey('  末日来袭  ')).toBe('末日来袭')
  })

  it('压缩多余空格', () => {
    expect(normalizeContentKey('某  剧  名')).toBe('某  剧  名'.trim().toLowerCase().replace(/\s+/g, ' '))
  })

  it('英文字母转小写', () => {
    expect(normalizeContentKey('DRAMA Name')).toBe('drama name')
  })
})
