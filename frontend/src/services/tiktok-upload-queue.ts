/**
 * TikTok 素材上传队列：可配置并发（1~3）的轻量队列。
 * 复用 uploadTikTokVideo（XHR multipart 单文件），不引入新依赖。
 *
 * 用法：
 *   const q = new UploadQueue(1)
 *   q.enqueue([{ file, advertiserId, durationSec }, ...])
 *   q.subscribe(() => render(q.getTasks()))
 */

import { uploadTikTokVideo, type UploadResult } from './tiktok-materials'

export type UploadTaskStatus =
  | 'queued'
  | 'uploading'
  | 'success'
  | 'failed'
  | 'canceled'

export interface UploadTask {
  /** 客户端唯一 ID（任务级，与后端 record_id 无关） */
  id: string
  file: File
  advertiserId: string
  /** 浏览器侧读取到的时长（秒），可能为 null（读不到时） */
  durationSec: number | null
  status: UploadTaskStatus
  /** 浏览器→后端 上传百分比，0~100 */
  browserPct: number
  /** 失败原因 */
  error?: string
  /** 后端记录 ID（成功后回填） */
  recordId?: number
  /** 创建时间（毫秒） */
  createdAt: number
  /** 完成时间（毫秒） */
  finishedAt?: number
  /** 取消句柄；任务运行中才有 */
  abort?: () => void
  /** 任务级预校验错误（不会被发送到后端），命中后直接置 failed 不入队 */
  validationError?: string
}

export interface UploadQueueSnapshot {
  tasks: UploadTask[]
  running: number
  queued: number
  succeeded: number
  failed: number
  canceled: number
  total: number
  active: boolean
}

type Listener = () => void

let _idSeq = 0
function nextId(): string {
  _idSeq += 1
  return `task-${Date.now().toString(36)}-${_idSeq.toString(36)}`
}

export class UploadQueue {
  private _tasks: UploadTask[] = []
  private _concurrency: number
  private _running = 0
  private _listeners = new Set<Listener>()
  /** 串行化通知，避免在 React 渲染中抛错 */
  private _notifyScheduled = false

  constructor(concurrency = 1) {
    this._concurrency = clampConcurrency(concurrency)
  }

  // ── public API ───────────────────────────────────────────────

  setConcurrency(n: number) {
    this._concurrency = clampConcurrency(n)
    this._pump()
    this._notify()
  }

  getConcurrency(): number {
    return this._concurrency
  }

  getTasks(): UploadTask[] {
    return this._tasks.slice()
  }

  /** 已有进行中或排队中的任务 */
  hasActive(): boolean {
    return this._tasks.some(t => t.status === 'uploading' || t.status === 'queued')
  }

  /** 当前快照（一次性聚合，便于 UI 渲染） */
  snapshot(): UploadQueueSnapshot {
    let running = 0, queued = 0, succeeded = 0, failed = 0, canceled = 0
    for (const t of this._tasks) {
      switch (t.status) {
        case 'uploading': running += 1; break
        case 'queued':    queued += 1;  break
        case 'success':   succeeded += 1; break
        case 'failed':    failed += 1;  break
        case 'canceled':  canceled += 1; break
      }
    }
    return {
      tasks: this._tasks.slice(),
      running, queued, succeeded, failed, canceled,
      total: this._tasks.length,
      active: running > 0 || queued > 0,
    }
  }

  subscribe(cb: Listener): () => void {
    this._listeners.add(cb)
    return () => { this._listeners.delete(cb) }
  }

  /**
   * 入队若干任务。每个 input 必须带 file/advertiserId/durationSec。
   * 若同时带 validationError，会被立即置为 failed（不会上传），但仍出现在列表里。
   */
  enqueue(inputs: Array<{
    file: File
    advertiserId: string
    durationSec: number | null
    validationError?: string
  }>): UploadTask[] {
    const created: UploadTask[] = []
    for (const it of inputs) {
      const t: UploadTask = {
        id: nextId(),
        file: it.file,
        advertiserId: it.advertiserId,
        durationSec: it.durationSec,
        status: it.validationError ? 'failed' : 'queued',
        browserPct: 0,
        error: it.validationError,
        validationError: it.validationError,
        createdAt: Date.now(),
        finishedAt: it.validationError ? Date.now() : undefined,
      }
      this._tasks.push(t)
      created.push(t)
    }
    this._pump()
    this._notify()
    return created
  }

  /** 失败任务重试：重置状态后重新放入队列尾部（保留原 task id 与 file 对象） */
  retry(taskId: string): boolean {
    const t = this._tasks.find(x => x.id === taskId)
    if (!t) return false
    if (t.status === 'uploading' || t.status === 'queued') return false
    if (t.validationError) return false  // 校验失败的任务不能重试，需用户重新选文件
    t.status = 'queued'
    t.browserPct = 0
    t.error = undefined
    t.recordId = undefined
    t.finishedAt = undefined
    this._pump()
    this._notify()
    return true
  }

  cancelTask(taskId: string): boolean {
    const t = this._tasks.find(x => x.id === taskId)
    if (!t) return false
    if (t.status === 'uploading') {
      try { t.abort?.() } catch { /* noop */ }
      // 不立即改状态，等 XHR 回调收尾，避免 race
      return true
    }
    if (t.status === 'queued') {
      t.status = 'canceled'
      t.finishedAt = Date.now()
      this._notify()
      return true
    }
    return false
  }

  cancelAll() {
    for (const t of this._tasks) {
      if (t.status === 'queued') {
        t.status = 'canceled'
        t.finishedAt = Date.now()
      } else if (t.status === 'uploading') {
        try { t.abort?.() } catch { /* noop */ }
      }
    }
    this._notify()
  }

  /** 清理已完成（success/failed/canceled），保留进行中/队列中 */
  clearFinished() {
    this._tasks = this._tasks.filter(
      t => t.status === 'queued' || t.status === 'uploading',
    )
    this._notify()
  }

  // ── internals ────────────────────────────────────────────────

  private _pump() {
    while (this._running < this._concurrency) {
      const next = this._tasks.find(t => t.status === 'queued')
      if (!next) return
      this._start(next)
    }
  }

  private _start(task: UploadTask) {
    task.status = 'uploading'
    task.browserPct = 0
    this._running += 1
    this._notify()

    const { promise, abort } = uploadTikTokVideo(
      task.advertiserId,
      task.file,
      task.durationSec,
      (pct) => {
        task.browserPct = pct
        this._notify()
      },
    )
    task.abort = abort

    promise.then((res: UploadResult) => {
      this._finish(task, res)
    }).catch((err: unknown) => {
      // uploadTikTokVideo 内部已把异常包成 { success: false, error }，
      // 这里兜底只为应对未知 reject 路径。
      this._finish(task, { success: false, error: String((err as Error)?.message ?? err) })
    })
  }

  private _finish(task: UploadTask, res: UploadResult) {
    this._running = Math.max(0, this._running - 1)
    task.abort = undefined
    task.finishedAt = Date.now()

    if (res.success) {
      task.status = 'success'
      task.browserPct = 100
      task.recordId = res.data?.id
      task.error = undefined
    } else {
      const err = res.error ?? '上传失败'
      const isAbort = err === '已取消' || err === '连接中断，请重试'
      task.status = isAbort ? 'canceled' : 'failed'
      task.error = err
    }

    this._pump()
    this._notify()
  }

  private _notify() {
    if (this._notifyScheduled) return
    this._notifyScheduled = true
    queueMicrotask(() => {
      this._notifyScheduled = false
      this._listeners.forEach(cb => {
        try { cb() } catch { /* swallow listener errors */ }
      })
    })
  }
}

function clampConcurrency(n: number): number {
  if (!Number.isFinite(n)) return 1
  return Math.min(3, Math.max(1, Math.floor(n)))
}
