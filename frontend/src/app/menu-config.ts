import {
  LayoutDashboard,
  Megaphone,
  Image,
  BarChart3,
  ArrowLeftRight,
  Settings,
  PlusCircle,
  FileText,
  FolderOpen,
  LineChart,
  Eye,
  GitCompare,
  Database,
  Users,
  ClipboardList,
  TrendingUp,
  Monitor,
  Shield,
  SlidersHorizontal,
  Redo2,
  Film,
} from 'lucide-react'
import type { MenuItem } from '@/types/menu'

export const menuConfig: MenuItem[] = [
  { id: 'dashboard', label: '首页概览', icon: LayoutDashboard, path: '/dashboard', panelKey: 'dashboard' },
  {
    id: 'ads-mgmt',
    label: '投放管理',
    icon: Megaphone,
    children: [
      { id: 'ads', label: '广告数据', icon: BarChart3, path: '/ads', panelKey: 'ads_data' },
      { id: 'tiktok-console', label: 'TikTok操作台', icon: Monitor, path: '/console/tiktok', panelKey: 'tiktok_console' },
      { id: 'meta-console', label: 'Meta操作台', icon: Monitor, path: '/console/meta', panelKey: 'meta_console' },
      { id: 'ads-create', label: '新建广告', icon: PlusCircle, path: '/ads/create', panelKey: 'ad_create' },
      { id: 'templates', label: '模板管理', icon: FileText, path: '/templates', panelKey: 'template_mgmt' },
    ],
  },
  {
    id: 'media',
    label: '素材中心',
    icon: Image,
    children: [
      { id: 'creatives', label: '素材库', icon: FolderOpen, path: '/creatives', panelKey: 'creatives' },
      { id: 'creative-analysis', label: '素材分析', icon: LineChart, path: '/creative-analysis', panelKey: 'creative_analysis' },
    ],
  },
  {
    id: 'analytics',
    label: '数据分析',
    icon: BarChart3,
    children: [
      { id: 'overview', label: '数据总览', icon: Eye, path: '/overview', panelKey: 'overview' },
      { id: 'channel-analysis', label: '渠道分析', icon: GitCompare, path: '/channel-analysis', panelKey: 'channel_analysis' },
      { id: 'biz-analysis', label: '业务分析', icon: TrendingUp, path: '/biz-analysis', panelKey: 'biz_analysis' },
      { id: 'returned-conversion', label: '广告回传分析', icon: Redo2, path: '/returned-conversion', panelKey: 'returned_conversion' },
      { id: 'drama-analysis', label: '剧级分析', icon: Film, path: '/drama-analysis', panelKey: 'drama_analysis' },
    ],
  },
  {
    id: 'compare-group',
    label: '数据对比',
    icon: ArrowLeftRight,
    children: [
      { id: 'data-compare', label: '数据对比', icon: GitCompare, path: '/data-compare', panelKey: 'data_compare' },
    ],
  },
  {
    id: 'system-group',
    label: '系统管理',
    icon: Settings,
    children: [
      { id: 'data-source', label: '数据源配置', icon: Database, path: '/data-source', panelKey: 'data_source' },
      { id: 'user-mgmt', label: '用户权限', icon: Users, path: '/user-mgmt', panelKey: 'user_mgmt' },
      { id: 'role-perm', label: '角色权限管理', icon: Shield, path: '/role-perm', panelKey: 'role_perm' },
      { id: 'insight-config', label: 'ROI 阈值配置', icon: SlidersHorizontal, path: '/insight-config', panelKey: 'insight_config' },
      { id: 'oplog', label: '操作日志', icon: ClipboardList, path: '/oplog', panelKey: 'oplog' },
    ],
  },
]
