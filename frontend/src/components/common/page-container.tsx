import type { ReactNode } from 'react'

interface Props {
  children: ReactNode
}

export function PageContainer({ children }: Props) {
  return <div className="max-w-7xl mx-auto">{children}</div>
}
