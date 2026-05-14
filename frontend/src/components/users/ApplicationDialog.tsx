import { useEffect, useState } from 'react'
import { useMutation } from '@tanstack/react-query'
import { X } from 'lucide-react'
import { submitApplication } from '@/services/userPaymentService'
import { ApiError } from '@/services/api'

interface Props {
  open: boolean
  user_id: number | null
  onClose: () => void
  onSuccess: () => void
}

export function ApplicationDialog({ open, user_id, onClose, onSuccess }: Props) {
  const [reason, setReason] = useState('')
  const [errorMsg, setErrorMsg] = useState<string | null>(null)

  useEffect(() => {
    if (open) {
      setReason('')
      setErrorMsg(null)
    }
  }, [open, user_id])

  const mutation = useMutation({
    mutationFn: async () => {
      if (!user_id) throw new Error('user_id 缺失')
      return submitApplication({
        target_user_id: user_id,
        requested_tag: 'whitelist',
        action: 'add',
        reason: reason.trim(),
      })
    },
    onSuccess: () => {
      onSuccess()
      onClose()
    },
    onError: (e: unknown) => {
      if (e instanceof ApiError) setErrorMsg(e.message)
      else if (e instanceof Error) setErrorMsg(e.message)
      else setErrorMsg('提交失败')
    },
  })

  if (!open || !user_id) return null

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="bg-white rounded-xl w-[480px] max-w-[90vw] shadow-xl">
        <div className="flex items-center justify-between px-4 py-3 border-b border-card-border">
          <div>
            <h3 className="text-sm font-semibold text-gray-900">申请加入白名单</h3>
            <p className="text-xs text-gray-500 mt-0.5">
              目标用户：<span className="font-mono text-gray-700">{user_id}</span>
              <span className="ml-2 text-gray-400">| 需要另一个 super_admin 审批通过后才生效</span>
            </p>
          </div>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-700">
            <X className="w-4 h-4" />
          </button>
        </div>

        <div className="p-4 space-y-3">
          <div>
            <label className="block text-xs text-gray-600 mb-1">
              申请理由 <span className="text-red-500">*</span>
            </label>
            <textarea
              value={reason}
              onChange={e => setReason(e.target.value)}
              rows={4}
              placeholder="如：内部测试号 / 已确认非真实用户 / 测试环境调试…"
              className="w-full px-3 py-2 text-sm border border-gray-200 rounded-lg focus:outline-none focus:border-blue-400"
            />
            <p className="text-[10px] text-gray-400 mt-1">
              理由会同步写入工单和白名单记录，便于审计追溯
            </p>
          </div>

          {errorMsg && (
            <div className="px-3 py-2 rounded-lg bg-red-50 border border-red-200 text-xs text-red-600">
              {errorMsg}
            </div>
          )}
        </div>

        <div className="flex items-center justify-end gap-2 px-4 py-3 border-t border-card-border">
          <button
            onClick={onClose}
            className="px-3 py-1.5 rounded text-xs text-gray-600 hover:bg-gray-50"
          >
            取消
          </button>
          <button
            onClick={() => mutation.mutate()}
            disabled={!reason.trim() || mutation.isPending}
            className="px-3 py-1.5 rounded text-xs bg-blue-500 text-white hover:bg-blue-600 disabled:opacity-40"
          >
            {mutation.isPending ? '提交中…' : '提交申请'}
          </button>
        </div>
      </div>
    </div>
  )
}
