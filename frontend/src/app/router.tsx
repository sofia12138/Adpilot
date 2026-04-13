import { createBrowserRouter, Navigate } from 'react-router-dom'
import { MainLayout } from '@/components/layout/main-layout'
import { AuthGuard } from '@/components/auth/AuthGuard'

import LoginPage from '@/pages/LoginPage'
import ForbiddenPage from '@/pages/ForbiddenPage'
import DashboardPage from '@/pages/DashboardPage'
import AdsPage from '@/pages/AdsPage'
import TikTokConsolePage from '@/pages/TikTokConsolePage'
import MetaConsolePage from '@/pages/MetaConsolePage'
import AdsCreatePage from '@/pages/AdsCreatePage'
import TemplatesPage from '@/pages/TemplatesPage'
import CreativesPage from '@/pages/CreativesPage'
import CreativeAnalysisPage from '@/pages/CreativeAnalysisPage'
import OverviewPage from '@/pages/OverviewPage'
import ChannelAnalysisPage from '@/pages/ChannelAnalysisPage'
import BizAnalysisPage from '@/pages/BizAnalysisPage'
import DataComparePage from '@/pages/DataComparePage'
import DataSourcePage from '@/pages/DataSourcePage'
import UserMgmtPage from '@/pages/UserMgmtPage'
import RolePermPage from '@/pages/RolePermPage'
import InsightConfigPage from '@/pages/InsightConfigPage'
import OplogPage from '@/pages/OplogPage'
import ReturnedConversionPage from '@/pages/ReturnedConversionPage'
import DramaOverviewPage from '@/pages/DramaOverviewPage'
import DesignerPerformancePage from '@/pages/DesignerPerformancePage'
import OptimizerPerformancePage from '@/pages/OptimizerPerformancePage'
import OptimizerDirectoryPage from '@/pages/OptimizerDirectoryPage'

function G({ panelKey, children }: { panelKey: string; children: React.ReactNode }) {
  return <AuthGuard panelKey={panelKey}>{children}</AuthGuard>
}

export const router = createBrowserRouter([
  { path: '/login', element: <LoginPage /> },
  {
    path: '/',
    element: <MainLayout />,
    children: [
      { index: true, element: <Navigate to="/dashboard" replace /> },
      { path: 'forbidden', element: <ForbiddenPage /> },
      { path: 'dashboard',          element: <G panelKey="dashboard"><DashboardPage /></G> },
      { path: 'ads',                element: <G panelKey="ads_data"><AdsPage /></G> },
      { path: 'console/tiktok',     element: <G panelKey="tiktok_console"><TikTokConsolePage /></G> },
      { path: 'console/meta',       element: <G panelKey="meta_console"><MetaConsolePage /></G> },
      { path: 'ads/create',         element: <G panelKey="ad_create"><AdsCreatePage /></G> },
      { path: 'templates',          element: <G panelKey="template_mgmt"><TemplatesPage /></G> },
      { path: 'creatives',          element: <G panelKey="creatives"><CreativesPage /></G> },
      { path: 'creative-analysis',  element: <G panelKey="creative_analysis"><CreativeAnalysisPage /></G> },
      { path: 'overview',           element: <G panelKey="overview"><OverviewPage /></G> },
      { path: 'channel-analysis',   element: <G panelKey="channel_analysis"><ChannelAnalysisPage /></G> },
      { path: 'biz-analysis',         element: <G panelKey="biz_analysis"><BizAnalysisPage /></G> },
      { path: 'returned-conversion', element: <G panelKey="returned_conversion"><ReturnedConversionPage /></G> },
      { path: 'drama-analysis',          element: <G panelKey="drama_analysis"><DramaOverviewPage /></G> },
      { path: 'designer-performance',    element: <G panelKey="designer_performance"><DesignerPerformancePage /></G> },
      { path: 'optimizer-performance',   element: <G panelKey="optimizer_performance"><OptimizerPerformancePage /></G> },
      { path: 'data-compare',        element: <G panelKey="data_compare"><DataComparePage /></G> },
      { path: 'data-source',        element: <G panelKey="data_source"><DataSourcePage /></G> },
      { path: 'user-mgmt',          element: <G panelKey="user_mgmt"><UserMgmtPage /></G> },
      { path: 'role-perm',          element: <G panelKey="role_perm"><RolePermPage /></G> },
      { path: 'insight-config',    element: <G panelKey="insight_config"><InsightConfigPage /></G> },
      { path: 'oplog',              element: <G panelKey="oplog"><OplogPage /></G> },
      { path: 'optimizer-directory', element: <G panelKey="optimizer_directory"><OptimizerDirectoryPage /></G> },
    ],
  },
])
