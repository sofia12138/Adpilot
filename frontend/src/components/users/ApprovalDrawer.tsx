import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { X, CheckCircle2, XCircle } from 'lucide-react'
import { fetchApplications, approveApplication, rejectApplication, withdrawApplication } from '@/services/userPaymentService'
import { ApiError } from '@/services/api'
import type { AnomalyApplication, ApplicationStatus } from '@/types/userPayment'

interface Props {
  open: boolean
  onClose: () => void
  currentUser: string
  canApprove: boolean   // super_admin
}

const STATUS_LABEL: Record<ApplicationStatus, { text: string; cls: string }> = {
  pending:   { text: '待审批', cls: 'bg-blue-50 text-blue-700 border-blue-200' },
  approved:  { text: '已通过', cls: 'bg-green-50 text-green-700 border-green-200' },
  rejected:  { text: '已拒绝', cls: 'bg-red-50 text-red-600 border-red-200' },
  withdrawn: { text: '已撤回', cls: 'bg-gray-50 text-gray-500 border-gray-200' },
}

export function ApprovalDrawer({ open, onClose, currentUser, canApprove }: Props) {
  const qc = useQueryClient()
  const [tab, setTab] = useState<ApplicationStatus | 'all'>('pending')
  const [reviewNote, setReviewNote] = useState<Record<number, string>>({})
  const [errorMsg, setErrorMsg] = useState<string | null>(null)

  const { data, isLoading, refetch } = useQuery({
    queryKey: ['applications', tab],
    queryFn: () => fetchApplications({
      status: tab === 'all' ? undefined : tab,
      page: 1, page_size: 100,
    }),
    enabled: open,
    refetchInterval: 10000,
  })

  const approveMu = useMutation({
    mutationFn: (id: number) => approveApplication(id, reviewNote[id] ?? ''),
    onSuccess: () => {
      refetch()
      qc.invalidateQueries({ queryKey: ['user-payment-users'] })
      qc.invalidateQueries({ queryKey: ['user-payment-kpi'] })
      qc.invalidateQueries({ queryKey: ['ops-users-pending-count'] })
    },
    onError: handleError,
  })
  const rejectMu = useMutation({
    mutationFn: (id: number) => rejectApplication(id, reviewNote[id] ?? ''),
    onSuccess: () => {
      refetch()
      qc.invalidateQueries({ queryKey: ['user-payment-users'] })
      qc.invalidateQueries({ queryKey: ['ops-users-pending-count'] })
    },
    onError: handleError,
  })
  const withdrawMu = useMutation({
    mutationFn: (id: number) => withdrawApplication(id),
    onSuccess: () => {
      refetch()
      qc.invalidateQueries({ queryKey: ['user-payment-users'] })
      qc.invalidateQueries({ queryKey: ['ops-users-pending-count'] })
    },
    onError: handleError,
  })

  function handleError(e: unknown) {
    if (e instanceof ApiError) setErrorMsg(e.message)
    else if (e instanceof Error) setErrorMsg(e.message)
    else setErrorMsg('操作失败')
    setTimeout(() => setErrorMsg(null), 4000)
  }

  if (!open) return null

  const items = data?.items ?? []

  return (
    <div className="fixed inset-0 z-50 flex justify-end bg-black/30">
      <div className="bg-white w-[640px] max-w-full h-full shadow-xl flex flex-col">
        <div className="flex items-center justify-between px-4 py-3 border-b border-card-border shrink-0">
          <div>
            <h3 className="text-sm font-semibold text-gray-900">白名单审批工单</h3>
            <p className="text-xs text-gray-500 mt-0.5">
              当前用户：<span className="font-mono">{currentUser || '未登录'}</span>
              <span className="ml-2 text-gray-400">| {canApprove ? 'super_admin 可审批' : '仅 super_admin 可审批'}</span>
            </p>
          </div>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-700">
            <X className="w-4 h-4" />
          </button>
        </div>

        {/* Tab */}
        <div className="flex items-center gap-1 px-4 py-2 border-b border-card-border text-xs shrink-0">
          {(['pending', 'approved', 'rejected', 'withdrawn', 'all'] as const).map(t => (
            <button
              key={t}
              onClick={() => setTab(t)}
              className={`px-3 py-1 rounded ${
                tab === t ? 'bg-gray-100 text-gray-900 font-medium' : 'text-gray-500 hover:text-gray-800'
              }`}
            >
              {t === 'all' ? '全部' : STATUS_LABEL[t].text}
              {t === 'pending' && data && data.pending_count > 0 && (
                <span className="ml-1 inline-flex items-center justify-center min-w-[16px] h-[16px] rounded-full bg-red-500 text-white text-[10px] px-1">
                  {data.pending_count}
                </span>
              )}
            </button>
          ))}
        </div>

        {errorMsg && (
          <div className="px-4 py-2 bg-red-50 border-b border-red-200 text-xs text-red-600 shrink-0">
            {errorMsg}
          </div>
        )}

        <div className="flex-1 overflow-y-auto p-4 space-y-3">
          {isLoading && <div className="text-xs text-gray-400 text-center py-8">加载中…</div>}
          {!isLoading && items.length === 0 && (
            <div className="text-xs text-gray-400 text-center py-8">暂无工单</div>
          )}
          {items.map(app => (
            <AppCard
              key={app.id}
              app={app}
              currentUser={currentUser}
              canApprove={canApprove}
              note={reviewNote[app.id] ?? ''}
              onNoteChange={(v) => setReviewNote(s => ({ ...s, [app.id]: v }))}
              onApprove={() => approveMu.mutate(app.id)}
              onReject={() => rejectMu.mutate(app.id)}
              onWithdraw={() => withdrawMu.mutate(app.id)}
              pending={approveMu.isPending || rejectMu.isPending || withdrawMu.isPending}
            />
          ))}
        </div>
      </div>
    </div>
  )
}

interface CardProps {
  app: AnomalyApplication
  currentUser: string
  canApprove: boolean
  note: string
  onNoteChange: (v: string) => void
  onApprove: () => void
  onReject: () => void
  onWithdraw: () => void
  pending: boolean
}

function AppCard({ app, currentUser, canApprove, note, onNoteChange, onApprove, onReject, onWithdraw, pending }: CardProps) {
  const meta = STATUS_LABEL[app.status]
  const isApplicant = app.applicant_user === currentUser
  const canAct = app.status === 'pending'
  const canSelfReview = canApprove && !isApplicant && canAct
  const canSelfWithdraw = isApplicant && canAct

  return (
    <div className="border border-card-border rounded-lg p-3 text-xs space-y-2">
      <div className="flex items-start justify-between">
        <div className="space-y-0.5">
          <div>
            <span className="text-gray-500">工单 #</span>
            <span className="font-mono font-medium">{app.id}</span>
            <span className="ml-2 text-gray-500">目标用户</span>
            <span className="font-mono font-medium text-blue-600 ml-1">{app.target_user_id}</span>
          </div>
          <div className="text-gray-500">
            <span>{app.action === 'add' ? '加入' : '移除'}</span>
            <span className="mx-1">·</span>
            <span>{app.requested_tag}</span>
            <span className="mx-1">·</span>
            <span>申请人：<span className="font-mono">{app.applicant_user}</span></span>
          </div>
          <div className="text-gray-400 text-[10px]">
            申请时间：{app.applied_at?.replace('T', ' ').slice(0, 16)}
          </div>
        </div>
        <span className={`px-2 py-0.5 rounded border ${meta.cls} text-[10px]`}>{meta.text}</span>
      </div>

      <div className="bg-gray-50 rounded p-2 text-gray-700">
        <span className="text-gray-500">理由：</span>
        {app.reason || <span className="text-gray-300">（空）</span>}
      </div>

      {app.status !== 'pending' && (
        <div className="text-gray-500 text-[11px]">
          <span>审批人：<span className="font-mono">{app.reviewer_user || '—'}</span></span>
          <span className="ml-2 text-gray-400">{app.reviewed_at?.replace('T', ' ').slice(0, 16)}</span>
          {app.review_note && (
            <div className="mt-1 text-gray-600">备注：{app.review_note}</div>
          )}
        </div>
      )}

      {canAct && (canSelfReview || canSelfWithdraw) && (
        <div className="flex items-center gap-2 pt-1">
          {canSelfReview && (
            <input
              type="text"
              placeholder="审批备注（可选）"
              value={note}
              onChange={e => onNoteChange(e.target.value)}
              className="flex-1 px-2 py-1 text-xs border border-gray-200 rounded focus:outline-none focus:border-blue-300"
            />
          )}
          {canSelfReview && (
            <>
              <button
                onClick={onApprove}
                disabled={pending}
                className="inline-flex items-center gap-1 px-2 py-1 rounded bg-green-50 hover:bg-green-100 text-green-700 border border-green-200 text-xs disabled:opacity-40"
              >
                <CheckCircle2 className="w-3 h-3" />
                通过
              </button>
              <button
                onClick={onReject}
                disabled={pending}
                className="inline-flex items-center gap-1 px-2 py-1 rounded bg-red-50 hover:bg-red-100 text-red-700 border border-red-200 text-xs disabled:opacity-40"
              >
                <XCircle className="w-3 h-3" />
                拒绝
              </button>
            </>
          )}
          {canSelfWithdraw && (
            <button
              onClick={onWithdraw}
              disabled={pending}
              className="px-2 py-1 rounded border border-gray-200 hover:bg-gray-50 text-gray-600 text-xs disabled:opacity-40"
            >
              撤回
            </button>
          )}
        </div>
      )}

      {canAct && isApplicant && !canApprove && (
        <div className="text-[10px] text-gray-400">仅 super_admin 可审批，请耐心等待</div>
      )}
      {canAct && isApplicant && canApprove && (
        <div className="text-[10px] text-orange-500">申请人不能审批自己的工单（强制约束）</div>
      )}
    </div>
  )
}
