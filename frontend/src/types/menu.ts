import type { LucideIcon } from 'lucide-react'

export interface MenuItem {
  id: string
  label: string
  icon?: LucideIcon
  path?: string
  children?: MenuItem[]
  panelKey?: string
}

export const ROLE_LABELS: Record<string, string> = {
  super_admin: '超级管理员',
  admin: '管理员',
  optimizer: '投放人员',
  designer: '设计师',
  analyst: '分析人员',
  viewer: '访客',
}
