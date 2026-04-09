import { useQuery } from '@tanstack/react-query'
import { fetchAdAccounts } from '@/services/accounts-mgmt'

export function useAdAccounts() {
  return useQuery({
    queryKey: ['ad-accounts'],
    queryFn: () => fetchAdAccounts().then(r => r.data),
  })
}
