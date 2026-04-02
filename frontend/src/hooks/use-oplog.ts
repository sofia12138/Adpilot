import { useQuery } from '@tanstack/react-query'
import { fetchOplog, type OplogEntry } from '@/services/oplog'

export function useOplog(page = 1, pageSize = 30) {
  return useQuery<{ list: OplogEntry[]; total: number }>({
    queryKey: ['oplog', page, pageSize],
    queryFn: () => fetchOplog(page, pageSize),
  })
}
