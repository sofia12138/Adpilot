import { useState } from 'react'
import { X, Search, CheckCircle2, AlertTriangle, Loader2 } from 'lucide-react'
import { verifyToken, addAdAccount } from '@/services/accounts-mgmt'

interface Props {
  onClose: () => void
  onSuccess: () => void
}

type Step = 'form' | 'select'

interface DiscoveredAccount {
  account_id: string
  account_name: string
  currency?: string
  timezone?: string
  status?: string
}

export default function AddAccountModal({ onClose, onSuccess }: Props) {
  const [step, setStep] = useState<Step>('form')
  const [platform, setPlatform] = useState<'tiktok' | 'meta'>('tiktok')
  const [accessToken, setAccessToken] = useState('')
  const [appId, setAppId] = useState('')
  const [appSecret, setAppSecret] = useState('')
  const [verifying, setVerifying] = useState(false)
  const [verifyError, setVerifyError] = useState('')
  const [discovered, setDiscovered] = useState<DiscoveredAccount[]>([])
  const [selected, setSelected] = useState<Set<string>>(new Set())
  const [adding, setAdding] = useState(false)
  const [addError, setAddError] = useState('')

  const handleVerify = async () => {
    if (!accessToken.trim()) {
      setVerifyError('请输入 Access Token')
      return
    }
    setVerifying(true)
    setVerifyError('')
    try {
      const result = await verifyToken({
        platform,
        access_token: accessToken.trim(),
        app_id: appId.trim(),
        app_secret: appSecret.trim(),
      })
      if (!result.valid) {
        setVerifyError(result.error || 'Token 验证失败')
        return
      }
      if (result.accounts.length === 0) {
        setVerifyError('Token 有效，但未找到可用的广告账户')
        return
      }
      setDiscovered(result.accounts)
      setSelected(new Set(result.accounts.map(a => a.account_id)))
      setStep('select')
    } catch (e: any) {
      setVerifyError(e.message || '验证失败')
    } finally {
      setVerifying(false)
    }
  }

  const toggleAccount = (id: string) => {
    setSelected(prev => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }

  const handleAdd = async () => {
    if (selected.size === 0) return
    setAdding(true)
    setAddError('')
    try {
      for (const acct of discovered.filter(a => selected.has(a.account_id))) {
        await addAdAccount({
          platform,
          account_id: acct.account_id,
          account_name: acct.account_name,
          access_token: accessToken.trim(),
          app_id: appId.trim(),
          app_secret: appSecret.trim(),
          currency: acct.currency || 'USD',
          timezone: acct.timezone || 'UTC',
        })
      }
      onSuccess()
    } catch (e: any) {
      setAddError(e.message || '添加失败')
    } finally {
      setAdding(false)
    }
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="bg-white rounded-2xl shadow-xl w-full max-w-lg mx-4 max-h-[90vh] flex flex-col">
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-100">
          <h3 className="text-base font-semibold text-gray-900">
            {step === 'form' ? '添加广告账户' : '选择要添加的账户'}
          </h3>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600 transition">
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Body */}
        <div className="px-6 py-5 overflow-y-auto flex-1 space-y-4">
          {step === 'form' ? (
            <>
              {/* Platform */}
              <div>
                <label className="block text-xs font-medium text-gray-500 mb-1.5">广告平台</label>
                <div className="flex gap-3">
                  {(['tiktok', 'meta'] as const).map(p => (
                    <button
                      key={p}
                      onClick={() => setPlatform(p)}
                      className={`flex-1 py-2.5 rounded-lg border text-sm font-medium transition ${
                        platform === p
                          ? 'border-blue-500 bg-blue-50 text-blue-700'
                          : 'border-gray-200 text-gray-500 hover:border-gray-300'
                      }`}
                    >
                      {p === 'tiktok' ? 'TikTok' : 'Meta (Facebook)'}
                    </button>
                  ))}
                </div>
              </div>

              {/* Access Token */}
              <div>
                <label className="block text-xs font-medium text-gray-500 mb-1.5">Access Token *</label>
                <textarea
                  value={accessToken}
                  onChange={e => setAccessToken(e.target.value)}
                  placeholder="粘贴平台 Access Token"
                  rows={3}
                  className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:border-blue-400 transition resize-none font-mono text-xs"
                />
              </div>

              {/* TikTok specific */}
              {platform === 'tiktok' && (
                <div className="grid grid-cols-2 gap-3">
                  <div>
                    <label className="block text-xs font-medium text-gray-500 mb-1.5">App ID</label>
                    <input
                      value={appId}
                      onChange={e => setAppId(e.target.value)}
                      placeholder="TikTok App ID"
                      className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:border-blue-400 transition"
                    />
                  </div>
                  <div>
                    <label className="block text-xs font-medium text-gray-500 mb-1.5">App Secret</label>
                    <input
                      value={appSecret}
                      onChange={e => setAppSecret(e.target.value)}
                      type="password"
                      placeholder="TikTok App Secret"
                      className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:border-blue-400 transition"
                    />
                  </div>
                </div>
              )}

              {verifyError && (
                <div className="flex items-center gap-2 text-sm text-red-600 bg-red-50 rounded-lg px-3 py-2">
                  <AlertTriangle className="w-4 h-4 shrink-0" />
                  {verifyError}
                </div>
              )}
            </>
          ) : (
            <>
              <p className="text-xs text-gray-500">
                Token 验证通过，发现 {discovered.length} 个广告账户。勾选要添加的账户：
              </p>
              <div className="space-y-2 max-h-64 overflow-y-auto">
                {discovered.map(acct => (
                  <label
                    key={acct.account_id}
                    className={`flex items-center gap-3 px-3 py-2.5 rounded-lg border cursor-pointer transition ${
                      selected.has(acct.account_id)
                        ? 'border-blue-300 bg-blue-50/50'
                        : 'border-gray-200 hover:border-gray-300'
                    }`}
                  >
                    <input
                      type="checkbox"
                      checked={selected.has(acct.account_id)}
                      onChange={() => toggleAccount(acct.account_id)}
                      className="rounded border-gray-300"
                    />
                    <div className="flex-1 min-w-0">
                      <div className="text-sm font-medium text-gray-800 truncate">
                        {acct.account_name || acct.account_id}
                      </div>
                      <div className="text-xs text-gray-400 font-mono">{acct.account_id}</div>
                    </div>
                    {acct.currency && (
                      <span className="text-xs text-gray-400">{acct.currency}</span>
                    )}
                    {acct.status && (
                      <span className={`text-[10px] px-1.5 py-0.5 rounded ${
                        acct.status === 'ACTIVE' ? 'bg-green-50 text-green-600' : 'bg-gray-100 text-gray-500'
                      }`}>
                        {acct.status}
                      </span>
                    )}
                  </label>
                ))}
              </div>
              {addError && (
                <div className="flex items-center gap-2 text-sm text-red-600 bg-red-50 rounded-lg px-3 py-2">
                  <AlertTriangle className="w-4 h-4 shrink-0" />
                  {addError}
                </div>
              )}
            </>
          )}
        </div>

        {/* Footer */}
        <div className="flex items-center justify-end gap-3 px-6 py-4 border-t border-gray-100">
          {step === 'select' && (
            <button
              onClick={() => setStep('form')}
              className="px-4 py-2 text-sm text-gray-600 hover:text-gray-800 transition"
            >
              返回
            </button>
          )}
          <button
            onClick={onClose}
            className="px-4 py-2 text-sm text-gray-500 hover:text-gray-700 transition"
          >
            取消
          </button>
          {step === 'form' ? (
            <button
              onClick={handleVerify}
              disabled={verifying || !accessToken.trim()}
              className="inline-flex items-center gap-1.5 px-5 py-2 rounded-lg text-sm font-medium bg-blue-600 text-white hover:bg-blue-700 transition disabled:opacity-50"
            >
              {verifying ? <Loader2 className="w-4 h-4 animate-spin" /> : <Search className="w-4 h-4" />}
              验证并查找账户
            </button>
          ) : (
            <button
              onClick={handleAdd}
              disabled={adding || selected.size === 0}
              className="inline-flex items-center gap-1.5 px-5 py-2 rounded-lg text-sm font-medium bg-blue-600 text-white hover:bg-blue-700 transition disabled:opacity-50"
            >
              {adding ? <Loader2 className="w-4 h-4 animate-spin" /> : <CheckCircle2 className="w-4 h-4" />}
              添加 {selected.size} 个账户
            </button>
          )}
        </div>
      </div>
    </div>
  )
}
