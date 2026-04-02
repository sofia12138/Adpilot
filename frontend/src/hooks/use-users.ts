import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  fetchUsers,
  createUser,
  updateUser,
  deleteUser,
  type UserInfo,
  type CreateUserBody,
  type UpdateUserBody,
} from '@/services/users'

const KEY = ['users'] as const

export function useUsers() {
  return useQuery<UserInfo[]>({
    queryKey: KEY,
    queryFn: fetchUsers,
  })
}

export function useCreateUser() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (body: CreateUserBody) => createUser(body),
    onSuccess: () => qc.invalidateQueries({ queryKey: KEY }),
  })
}

export function useUpdateUser() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: ({ username, body }: { username: string; body: UpdateUserBody }) =>
      updateUser(username, body),
    onSuccess: () => qc.invalidateQueries({ queryKey: KEY }),
  })
}

export function useDeleteUser() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (username: string) => deleteUser(username),
    onSuccess: () => qc.invalidateQueries({ queryKey: KEY }),
  })
}
