import { Card, CardContent } from '@/components/ui/card'
import { PageContainer } from './page-container'
import { PageHeader } from './page-header'
import { PackageOpen } from 'lucide-react'

interface Props {
  title: string
  description?: string
}

export function PlaceholderPage({ title, description }: Props) {
  return (
    <PageContainer>
      <PageHeader title={title} description={description} />
      <Card>
        <CardContent className="flex flex-col items-center justify-center py-24 text-muted-foreground">
          <PackageOpen className="w-12 h-12 mb-3" strokeWidth={1.2} />
          <p className="text-sm">功能开发中，即将上线</p>
        </CardContent>
      </Card>
    </PageContainer>
  )
}
