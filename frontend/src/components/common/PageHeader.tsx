interface Props {
  title: string
  description?: string
  action?: React.ReactNode
}

export function PageHeader({ title, description, action }: Props) {
  return (
    <div className="flex items-start justify-between mb-7">
      <div>
        <h1 className="text-xl font-bold text-gray-900 tracking-tight">{title}</h1>
        {description && <p className="mt-1.5 text-sm text-gray-400 leading-relaxed">{description}</p>}
      </div>
      {action && <div>{action}</div>}
    </div>
  )
}
