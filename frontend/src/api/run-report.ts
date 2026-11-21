/** 综合运行报告接口：全部按运行编号取数，不再使用固定样本或当天模拟进度。 */

import { request } from './client'

export type ReceiptType = 'quality_gate' | 'partition_progress'

export type ReceiptMeta = {
  revision: number
  received_at: string
  content_hash: string
  label: string
}

export type QualityRuleFact = {
  rule_id: string
  rule_name: string
  check_field: string
  check_type: string
  threshold: number
  level: string
  total_rows: number
  passed_rows: number
  failed_rows: number
  bad_row_rate: number
  failed_ratio: number
  threshold_exceeded: boolean
  blocked: boolean
  validation_run_id?: number
}

export type QualitySummary = {
  available: boolean
  ready: boolean
  source: string
  decision: 'RELEASE' | 'BLOCKED' | 'PENDING'
  overall_pass: boolean
  computed_decision: string
  validation_run_id: number | null
  validation_batch: string
  target_table: string
  validation_status: string
  rules: QualityRuleFact[]
  warnings: string[]
  total_rows: number
  passed_rows: number
  failed_rows: number
  bad_row_rate: number
}

export type PartitionFact = {
  partition: string
  batch_date: string
  status: string
  total_rows: number
  passed_rows: number
  failed_rows: number
  bad_row_rate: number
  validation_run_id: number | null
}

export type ProgressSummary = {
  available: boolean
  expected_partitions: number
  received_partitions: number
  completed_partitions: number
  failed_partitions: number
  completion_pct: number
  success_pct: number
  complete: boolean
  total_rows: number
  passed_rows: number
  failed_rows: number
  bad_row_rate: number
  partitions: PartitionFact[]
}

export type AlertItem = {
  source: string
  rule_id: string
  rule_name: string
  level: string
  message: string
}

export type AlertSummary = {
  total: number
  by_level: Record<string, number>
  highest_level: string
  items: AlertItem[]
}

export type RunReportData = {
  run_id: string
  pipeline_id: string
  batch_date: string
  state: 'waiting' | 'partial' | 'complete' | 'inconsistent'
  generated_at: string | null
  quality: QualitySummary
  progress: ProgressSummary
  alerts: AlertSummary
  alert_items: AlertItem[]
  health: string
  issues: string[]
  evidence: {
    run_id: string
    pipeline_id: string
    batch_date: string
    same_round: boolean
    received_receipts: ReceiptType[]
    missing_receipts: ReceiptType[]
    partition_dates: string[]
    receipts: Record<string, ReceiptMeta>
  }
  receipt_summary: {
    received: ReceiptType[]
    missing: ReceiptType[]
    labels: Record<string, string>
  }
}

export type ConfirmedReport = {
  pipeline_id: string
  run_id: string
  report_id: number
  confirmed_at: string
  batch_date: string
  report: RunReportData & { published?: boolean }
}

export type RunReportResponse = {
  run: {
    pipeline_id: string
    run_id: string
    batch_date: string
    expected_partitions: number
    created_at: string
    updated_at: string
    confirmed_at: string | null
  }
  state: RunReportData['state']
  received_receipts: ReceiptType[]
  missing_receipts: ReceiptType[]
  receipts: Record<string, ReceiptMeta>
  current: RunReportData
  last_confirmed: {
    report_id: number
    run_id?: string
    confirmed_at: string
    batch_date: string
    report: RunReportData
  } | null
  last_confirmed_scope: 'same_run' | 'previous_run' | 'none'
  has_confirmed_report: boolean
}

export type RunSummary = {
  pipeline_id: string
  run_id: string
  batch_date: string
  expected_partitions: number
  confirmed_at: string | null
  received_receipts: ReceiptType[]
  missing_receipts: ReceiptType[]
  receipt_revisions: Record<string, number>
}

async function readJson<T>(response: Response, fallback: string): Promise<T> {
  if (!response.ok) {
    let detail = fallback
    try {
      const payload = (await response.json()) as { detail?: string }
      if (payload?.detail) detail = payload.detail
    } catch {
      /* 保留兜底文案 */
    }
    throw new Error(detail)
  }
  return (await response.json()) as T
}

export async function listRuns(pipelineId?: string): Promise<RunSummary[]> {
  const query = pipelineId ? `?${new URLSearchParams({ pipeline_id: pipelineId })}` : ''
  const response = await request(`/api/runs${query}`)
  const payload = await readJson<{ items: RunSummary[] }>(response, '运行清单读取失败')
  return payload.items
}

export async function loadRunReport(
  pipelineId: string,
  runId: string,
): Promise<RunReportResponse> {
  const query = new URLSearchParams({ pipeline_id: pipelineId })
  const response = await request(`/api/runs/${encodeURIComponent(runId)}/report?${query}`)
  return readJson<RunReportResponse>(response, '综合报告读取失败')
}

export async function loadConfirmedReport(
  pipelineId: string,
  runId: string,
  reportId?: number,
): Promise<ConfirmedReport> {
  const query = new URLSearchParams({ pipeline_id: pipelineId })
  if (reportId !== undefined) query.set('report_id', String(reportId))
  const response = await request(
    `/api/runs/${encodeURIComponent(runId)}/report/confirmed?${query}`,
  )
  return readJson<ConfirmedReport>(response, '已确认报告读取失败')
}

export async function confirmRunReport(
  pipelineId: string,
  runId: string,
): Promise<{ ok: boolean; message: string; report_id: number; report: RunReportData }> {
  const response = await request(
    `/api/runs/${encodeURIComponent(runId)}/report/confirm`,
    { method: 'POST', body: JSON.stringify({ pipeline_id: pipelineId }) },
  )
  return readJson(response, '综合报告发布失败')
}

export async function submitReceipt(
  pipelineId: string,
  runId: string,
  receiptType: ReceiptType,
  payload: Record<string, unknown>,
): Promise<{ ok: boolean; message: string; report: RunReportResponse }> {
  const response = await request(
    `/api/runs/${encodeURIComponent(runId)}/receipts`,
    {
      method: 'POST',
      body: JSON.stringify({ pipeline_id: pipelineId, receipt_type: receiptType, payload }),
    },
  )
  return readJson(response, '回执提交失败')
}
