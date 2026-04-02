import { apiFetch } from './api'
import { DEFAULT_INSIGHT_THRESHOLDS, type InsightThresholds } from '@/config/insight-thresholds'

export async function getInsightConfig(): Promise<InsightThresholds> {
  try {
    return await apiFetch<InsightThresholds>('/api/insight/config')
  } catch {
    return DEFAULT_INSIGHT_THRESHOLDS
  }
}

export async function updateInsightConfig(data: InsightThresholds): Promise<void> {
  await apiFetch('/api/insight/config', {
    method: 'PUT',
    body: JSON.stringify(data),
  })
}
