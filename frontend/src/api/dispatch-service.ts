import { request } from './client'
import type { DispatchBoard } from '@/data/dispatch'

async function readJson<T>(response: Response, fallback: string): Promise<T> {
  if (!response.ok) {
    let detail = fallback
    try {
      const payload = (await response.json()) as { detail?: string }
      if (payload.detail) detail = payload.detail
    } catch {
      // 保留兜底文案
    }
    throw new Error(detail)
  }
  return (await response.json()) as T
}

export async function loadDispatchBoard(): Promise<DispatchBoard> {
  return readJson<DispatchBoard>(
    await request('/api/dispatch_board'),
    '分发看板读取失败',
  )
}

export type TargetCoverage = Pick<
  DispatchBoard,
  'summary' | 'targets' | 'coverage' | 'missing' | 'source_events'
>

export async function loadTargetCoverage(): Promise<TargetCoverage> {
  return readJson<TargetCoverage>(
    await request('/api/dispatch_board/target-coverage'),
    '目标覆盖数据集读取失败',
  )
}

export async function recordReceipt(payload: {
  event_id: string
  target: string
  channel?: string
  received_at?: string
  note?: string
}): Promise<{ ok: boolean; message: string }> {
  const response = await request('/api/dispatch_board/receipts', {
    method: 'POST',
    body: JSON.stringify(payload),
  })
  return readJson<{ ok: boolean; message: string }>(response, '回执登记失败')
}

export async function claimMissing(operator: string): Promise<{ ok: boolean; message: string }> {
  const response = await request('/api/dispatch_board/claims', {
    method: 'POST',
    body: JSON.stringify({ operator }),
  })
  return readJson<{ ok: boolean; message: string }>(response, '缺投领取失败')
}

export async function registerReplays(claimId: string): Promise<{ ok: boolean; message: string }> {
  const response = await request(`/api/dispatch_board/claims/${claimId}/replays`, {
    method: 'POST',
    body: JSON.stringify({}),
  })
  return readJson<{ ok: boolean; message: string }>(response, '补发登记失败')
}

export async function resetBoard(): Promise<{ ok: boolean; message: string }> {
  const response = await request('/api/dispatch_board/reset', { method: 'POST' })
  return readJson<{ ok: boolean; message: string }>(response, '看板重置失败')
}
