import { useState, useRef } from 'react'
import { PageHeader } from '@/components/common/PageHeader'
import { SectionCard } from '@/components/common/SectionCard'
import { Upload, File, X, CheckCircle } from 'lucide-react'

interface FileItem {
  id: string
  file: File
  status: 'pending' | 'uploading' | 'done'
}

export default function UploadPage() {
  const [files, setFiles] = useState<FileItem[]>([])
  const inputRef = useRef<HTMLInputElement>(null)

  function handleDrop(e: React.DragEvent) {
    e.preventDefault()
    addFiles(Array.from(e.dataTransfer.files))
  }

  function addFiles(newFiles: File[]) {
    const items: FileItem[] = newFiles.map(f => ({
      id: `${Date.now()}-${Math.random().toString(36).slice(2)}`,
      file: f,
      status: 'pending',
    }))
    setFiles(prev => [...prev, ...items])
  }

  function removeFile(id: string) {
    setFiles(prev => prev.filter(f => f.id !== id))
  }

  function handleUpload() {
    alert('上传功能即将上线（骨架版）')
  }

  return (
    <div className="max-w-3xl mx-auto">
      <PageHeader title="素材上传" description="上传视频或图片素材到广告平台" />

      <SectionCard title="选择文件" className="mb-6">
        <div
          onDragOver={e => e.preventDefault()}
          onDrop={handleDrop}
          onClick={() => inputRef.current?.click()}
          className="border-2 border-dashed border-gray-200 rounded-xl py-12 flex flex-col items-center justify-center cursor-pointer hover:border-blue-300 hover:bg-blue-50/30 transition-colors"
        >
          <Upload className="w-8 h-8 text-gray-300 mb-3" />
          <p className="text-sm text-gray-500">点击或拖拽文件到此处</p>
          <p className="text-xs text-gray-400 mt-1">支持 MP4, MOV, PNG, JPG, GIF</p>
          <input
            ref={inputRef}
            type="file"
            multiple
            accept="video/*,image/*"
            className="hidden"
            onChange={e => e.target.files && addFiles(Array.from(e.target.files))}
          />
        </div>
      </SectionCard>

      {files.length > 0 && (
        <SectionCard title={`已选文件（${files.length}）`} className="mb-6">
          <div className="space-y-2">
            {files.map(item => (
              <div key={item.id} className="flex items-center gap-3 px-3 py-2.5 bg-gray-50 rounded-lg">
                <File className="w-4 h-4 text-gray-400 shrink-0" />
                <div className="flex-1 min-w-0">
                  <p className="text-sm text-gray-700 truncate">{item.file.name}</p>
                  <p className="text-xs text-gray-400">{(item.file.size / 1024 / 1024).toFixed(1)} MB</p>
                </div>
                {item.status === 'done' ? (
                  <CheckCircle className="w-4 h-4 text-green-500 shrink-0" />
                ) : (
                  <button onClick={() => removeFile(item.id)} className="p-1 hover:bg-gray-200 rounded transition">
                    <X className="w-3.5 h-3.5 text-gray-400" />
                  </button>
                )}
              </div>
            ))}
          </div>
        </SectionCard>
      )}

      {files.length > 0 && (
        <div className="flex justify-end">
          <button
            onClick={handleUpload}
            className="px-6 py-2.5 bg-blue-500 hover:bg-blue-600 text-white text-sm font-medium rounded-xl shadow-sm shadow-blue-500/20 transition-all flex items-center gap-2"
          >
            <Upload className="w-4 h-4" />
            开始上传
          </button>
        </div>
      )}
    </div>
  )
}
