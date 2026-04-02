export interface RoiThresholds {
  min: number
  low: number
  target: number
  high: number
}

export interface InsightThresholds {
  roi: RoiThresholds
}

export const DEFAULT_INSIGHT_THRESHOLDS: InsightThresholds = {
  roi: {
    min: 0.1,
    low: 0.8,
    target: 1.2,
    high: 2.0,
  },
}
