/**
 * 运营数据面板 — 图表配色（与设计规范一致）
 *
 * 命名约定：
 *   - 维度色（独立维度）：ios / android / sub / onetime
 *   - 复合色（双维度交叉）：ios_sub / ios_onetime / android_sub / android_onetime
 *     深色（ios_sub / android_sub）= 同色族内的「订阅」一档；
 *     浅色（ios_onetime / android_onetime）= 同色族内的「普通」一档
 */
export const COLORS = {
  ios:             '#378ADD',
  android:         '#3BC99A',
  sub:             '#7F77DD',
  onetime:         '#EF9F27',
  ios_sub:         '#378ADD',
  ios_onetime:     '#85B7EB',
  android_sub:     '#3BC99A',
  android_onetime: '#9FE1CB',
} as const
