import { useNavigate } from 'react-router-dom'
import { ShieldOff } from 'lucide-react'

export default function ForbiddenPage() {
  const navigate = useNavigate()
  return (
    <div className="min-h-[60vh] flex flex-col items-center justify-center text-center">
      <ShieldOff className="w-16 h-16 text-gray-300 mb-4" strokeWidth={1.2} />
      <h1 className="text-xl font-semibold text-gray-700 mb-2">无访问权限</h1>
      <p className="text-sm text-gray-400 mb-6 max-w-sm">
        你没有权限访问此页面，请联系管理员开通。
      </p>
      <button
        onClick={() => navigate('/dashboard')}
        className="px-5 py-2 bg-blue-500 text-white text-sm rounded-lg hover:bg-blue-600 transition"
      >
        返回首页
      </button>
    </div>
  )
}
