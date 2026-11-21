<template>
  <section class="page" data-module="run_report">
    <header class="page-head">
      <div>
        <h2>综合运行报告</h2>
        <p class="page-desc">
          按运行编号关联实际回执：质量门禁与分区证据先后到达时可复核当前进度，已确认的报告单独留存。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" data-testid="refresh-report" type="button" @click="reload">
          刷新当前报告
        </button>
        <button
          class="btn primary"
          data-testid="confirm-report"
          type="button"
          :disabled="!canConfirm || busy"
          @click="publish"
        >
          发布综合报告
        </button>
      </div>
    </header>

    <form class="filter-bar" @submit.prevent="openRun">
      <label class="filter-item">
        <span>管线标识</span>
        <input v-model="pipelineInput" data-testid="pipeline-input" placeholder="选择或输入管线" />
      </label>
      <label class="filter-item">
        <span>运行编号</span>
        <input v-model="runInput" data-testid="run-input" placeholder="例如 ORD-RUN-20261118" />
      </label>
      <button class="btn primary" data-testid="open-run" type="submit">查看运行</button>
    </form>

    <div class="run-picker">
      <span class="muted">已登记运行：</span>
      <button
        v-for="item in runs"
        :key="`${item.pipeline_id}-${item.run_id}`"
        class="chip"
        :class="{ active: item.run_id === runId }"
        type="button"
        :data-testid="`pick-${item.run_id}`"
        @click="pick(item)"
      >
        {{ item.run_id }} · {{ item.batch_date }}
        <em v-if="item.missing_receipts.length">缺 {{ item.missing_receipts.length }} 份回执</em>
        <em v-else>回执齐全</em>
      </button>
      <span v-if="!runs.length" class="muted">暂无登记运行</span>
    </div>

    <p v-if="errorMessage" class="error-text" data-testid="report-error">{{ errorMessage }}</p>
    <p v-if="notice" class="notice-text" data-testid="report-notice">{{ notice }}</p>

    <template v-if="report">
      <div class="stat-row">
        <article class="stat-card">
          <span class="stat-label">运行状态</span>
          <strong class="stat-value" data-testid="report-state">{{ stateLabel(report.current.state) }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">批次日期</span>
          <strong class="stat-value" data-testid="report-batch-date">{{ report.current.batch_date }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">实际坏行率</span>
          <strong class="stat-value" data-testid="report-bad-rate">{{ percent(report.current.quality.bad_row_rate) }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">分区完成度</span>
          <strong class="stat-value" data-testid="report-progress">{{ report.current.progress.completion_pct }}%</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">告警</span>
          <strong class="stat-value" data-testid="report-alerts">{{ report.current.alerts.total }}</strong>
        </article>
      </div>

      <section class="panel" data-testid="receipt-status">
        <h3>回执到达情况</h3>
        <div class="receipt-grid">
          <article
            v-for="type in receiptTypes"
            :key="type"
            class="receipt-card"
            :class="report.received_receipts.includes(type) ? 'received' : 'missing'"
            :data-testid="`receipt-${type}`"
          >
            <strong>{{ receiptLabel(type) }}</strong>
            <span v-if="report.receipts[type]">
              已收到 · 修订 {{ report.receipts[type].revision }} · {{ formatTime(report.receipts[type].received_at) }}
            </span>
            <span v-else>尚未收到，报告暂不采用该类证据</span>
          </article>
        </div>
        <p class="muted" data-testid="same-round">
          证据同轮：{{ report.current.evidence.same_round ? '是' : '否' }}
          · 分区日期：{{ report.current.evidence.partition_dates.join('、') || '暂无' }}
        </p>
        <ul v-if="report.current.issues.length" class="issue-list" data-testid="report-issues">
          <li v-for="issue in report.current.issues" :key="issue">{{ issue }}</li>
        </ul>
      </section>

      <section class="panel" data-testid="quality-panel">
        <h3>质量结论</h3>
        <p>
          <strong>{{ report.current.quality.decision }}</strong>
          <span v-if="report.current.quality.validation_batch" class="muted">
            · 执行批次 {{ report.current.quality.validation_batch }}
          </span>
          <span v-if="report.current.quality.target_table" class="muted">
            · {{ report.current.quality.target_table }}
          </span>
        </p>
        <p class="muted">
          总行数 {{ number(report.current.quality.total_rows) }}
          · 通过 {{ number(report.current.quality.passed_rows) }}
          · 失败 {{ number(report.current.quality.failed_rows) }}
          · 坏行率 {{ percent(report.current.quality.bad_row_rate) }}
          · 推算结论 {{ report.current.quality.computed_decision }}
        </p>
        <ul v-if="report.current.quality.warnings.length" class="issue-list">
          <li v-for="warning in report.current.quality.warnings" :key="warning">{{ warning }}</li>
        </ul>
        <table v-if="report.current.quality.rules.length" class="data-table">
          <thead>
            <tr>
              <th>规则</th><th>校验字段</th><th>总行数</th><th>失败行数</th>
              <th>失败率</th><th>阈值</th><th>结论</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="rule in report.current.quality.rules" :key="rule.rule_id">
              <td>{{ rule.rule_name }}</td>
              <td>{{ rule.check_field || '—' }}</td>
              <td>{{ number(rule.total_rows) }}</td>
              <td>{{ number(rule.failed_rows) }}</td>
              <td>{{ percent(rule.failed_ratio) }}</td>
              <td>{{ percent(rule.threshold) }}</td>
              <td>{{ rule.blocked ? '阻断' : rule.threshold_exceeded ? '超阈值' : '通过' }}</td>
            </tr>
          </tbody>
        </table>
      </section>

      <section class="panel" data-testid="progress-panel">
        <h3>分区进度</h3>
        <p v-if="!report.current.progress.available" class="muted">
          分区证据尚未到达，当前只展示已登记的期望分区数
          {{ report.current.progress.expected_partitions }}。
        </p>
        <p v-else class="muted">
          已完成 {{ report.current.progress.completed_partitions }}/{{ report.current.progress.expected_partitions }}
          · 失败分区 {{ report.current.progress.failed_partitions }}
          · 累计失败行 {{ number(report.current.progress.failed_rows) }}
        </p>
        <table v-if="report.current.progress.partitions.length" class="data-table">
          <thead>
            <tr>
              <th>分区</th><th>日期</th><th>状态</th><th>总行数</th>
              <th>失败行数</th><th>坏行率</th>
            </tr>
          </thead>
          <tbody>
            <tr
              v-for="part in report.current.progress.partitions"
              :key="part.partition"
              :data-testid="`partition-${part.partition}`"
            >
              <td>{{ part.partition }}</td>
              <td>{{ part.batch_date }}</td>
              <td>{{ part.status === 'failed' ? '失败' : part.status === 'running' ? '进行中' : '成功' }}</td>
              <td>{{ number(part.total_rows) }}</td>
              <td>{{ number(part.failed_rows) }}</td>
              <td>{{ percent(part.bad_row_rate) }}</td>
            </tr>
          </tbody>
        </table>
      </section>

      <section class="panel" data-testid="alert-panel">
        <h3>告警摘要</h3>
        <p v-if="!report.current.alert_items.length" class="muted">当前没有由实际回执触发的告警。</p>
        <ul v-else class="alert-list">
          <li v-for="alert in report.current.alert_items" :key="`${alert.source}-${alert.rule_id}`">
            <span class="level" :class="`level-${alert.level.toLowerCase()}`">{{ alert.level }}</span>
            {{ alert.message }}
          </li>
        </ul>
      </section>

      <section class="panel" data-testid="confirmed-panel">
        <h3>上次完整结果</h3>
        <p v-if="!report.last_confirmed" class="muted">还没有已确认的报告。</p>
        <template v-else>
          <p class="muted">
            {{ report.last_confirmed_scope === 'same_run' ? '本次运行' : '同管线上一次运行' }}
            {{ report.last_confirmed.run_id || report.run.run_id }}
            · 确认于 {{ formatTime(report.last_confirmed.confirmed_at) }}
            · 批次日期 {{ report.last_confirmed.batch_date }}
            · 坏行率 {{ percent(report.last_confirmed.report.quality.bad_row_rate) }}
            · 分区完成度 {{ report.last_confirmed.report.progress.completion_pct }}%
          </p>
          <button class="link" data-testid="open-confirmed" type="button" @click="openConfirmed">
            读取数据库中已确认的报告 #{{ report.last_confirmed.report_id }}
          </button>
        </template>
        <div v-if="confirmed" class="confirmed-body" data-testid="confirmed-body">
          <h4>已确认报告 #{{ confirmed.report_id }}</h4>
          <p class="muted">
            {{ confirmed.run_id }} · {{ confirmed.batch_date }} · 确认于 {{ formatTime(confirmed.confirmed_at) }}
          </p>
          <p>
            质量结论 <strong>{{ confirmed.report.quality.decision }}</strong>
            · 总行数 {{ number(confirmed.report.quality.total_rows) }}
            · 失败行数 {{ number(confirmed.report.quality.failed_rows) }}
            · 坏行率 <strong data-testid="confirmed-bad-rate">{{ percent(confirmed.report.quality.bad_row_rate) }}</strong>
            · 分区完成度 {{ confirmed.report.progress.completion_pct }}%
          </p>
        </div>
      </section>
    </template>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  confirmRunReport,
  listRuns,
  loadConfirmedReport,
  loadRunReport,
  type ConfirmedReport,
  type ReceiptType,
  type RunReportResponse,
  type RunSummary,
} from '@/api/run-report'

const receiptTypes: ReceiptType[] = ['quality_gate', 'partition_progress']
const receiptLabels: Record<ReceiptType, string> = {
  quality_gate: '质量门禁回执',
  partition_progress: '分区进度回执',
}

const runs = ref<RunSummary[]>([])
const pipelineInput = ref('')
const runInput = ref('')
const report = ref<RunReportResponse | null>(null)
const confirmed = ref<ConfirmedReport | null>(null)
const errorMessage = ref('')
const notice = ref('')
const busy = ref(false)

const runId = computed(() => report.value?.run.run_id ?? '')
const canConfirm = computed(() => report.value?.current.state === 'complete')

function receiptLabel(type: ReceiptType) {
  return receiptLabels[type]
}

function stateLabel(state: string) {
  return {
    waiting: '等待回执',
    partial: '回执不全',
    complete: '证据齐全',
    inconsistent: '证据不一致',
  }[state] ?? state
}

function number(value: number) {
  return value.toLocaleString('zh-CN')
}

function percent(value: number) {
  return `${(value * 100).toFixed(4)}%`
}

function formatTime(value: string | null) {
  if (!value) return '—'
  return value.replace('T', ' ')
}

async function loadRuns() {
  try {
    runs.value = await listRuns()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '运行清单读取失败'
  }
}

async function openRun() {
  if (!pipelineInput.value || !runInput.value) {
    errorMessage.value = '请同时填写管线标识和运行编号'
    return
  }
  await fetchReport(pipelineInput.value.trim(), runInput.value.trim())
}

async function pick(item: RunSummary) {
  pipelineInput.value = item.pipeline_id
  runInput.value = item.run_id
  await fetchReport(item.pipeline_id, item.run_id)
}

async function fetchReport(pipelineId: string, id: string) {
  errorMessage.value = ''
  notice.value = ''
  confirmed.value = null
  try {
    report.value = await loadRunReport(pipelineId, id)
  } catch (error) {
    report.value = null
    errorMessage.value = error instanceof Error ? error.message : '综合报告读取失败'
  }
}

async function reload() {
  if (!report.value) {
    await loadRuns()
    return
  }
  await fetchReport(report.value.run.pipeline_id, report.value.run.run_id)
  await loadRuns()
}

async function publish() {
  if (!report.value) return
  busy.value = true
  errorMessage.value = ''
  notice.value = ''
  try {
    const result = await confirmRunReport(
      report.value.run.pipeline_id,
      report.value.run.run_id,
    )
    notice.value = `${result.message}（#${result.report_id}）`
    await fetchReport(report.value.run.pipeline_id, report.value.run.run_id)
    await loadRuns()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '综合报告发布失败'
  } finally {
    busy.value = false
  }
}

async function openConfirmed() {
  if (!report.value) return
  const target = report.value.last_confirmed
  if (!target) return
  errorMessage.value = ''
  try {
    confirmed.value = await loadConfirmedReport(
      report.value.run.pipeline_id,
      report.value.run.run_id,
      target.report_id,
    )
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '已确认报告读取失败'
  }
}

onMounted(async () => {
  await loadRuns()
  if (runs.value.length) {
    const latest = runs.value[0]
    pipelineInput.value = latest.pipeline_id
    runInput.value = latest.run_id
    await fetchReport(latest.pipeline_id, latest.run_id)
  }
})
</script>

<style scoped>
.panel { background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 12px 16px; margin-bottom: 12px; }
.panel h3 { margin: 0 0 8px; font-size: 15px; }
.panel h4 { margin: 12px 0 4px; font-size: 14px; }
.muted { color: var(--muted); font-size: 13px; }
.notice-text { color: #027a48; font-size: 13px; }
.run-picker { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; margin-bottom: 12px; }
.chip { border: 1px solid var(--border); background: #fff; border-radius: 16px; padding: 4px 12px; cursor: pointer; font-size: 13px; }
.chip.active { border-color: var(--brand); color: var(--brand); }
.chip em { font-style: normal; color: var(--muted); margin-left: 6px; font-size: 12px; }
.receipt-grid { display: flex; gap: 12px; flex-wrap: wrap; }
.receipt-card { flex: 1; min-width: 220px; border-radius: 8px; padding: 10px 12px; font-size: 13px; }
.receipt-card strong { display: block; margin-bottom: 4px; }
.receipt-card.received { background: #ecfdf3; border: 1px solid #abefc6; }
.receipt-card.missing { background: #fffaeb; border: 1px solid #fedf89; }
.issue-list { margin: 8px 0 0; padding-left: 20px; color: #b54708; font-size: 13px; }
.alert-list { margin: 0; padding-left: 20px; font-size: 13px; }
.level { display: inline-block; border-radius: 4px; padding: 0 6px; margin-right: 6px; font-size: 11px; color: #fff; }
.level-critical { background: #d92d20; }
.level-warn { background: #dc6803; }
.level-info { background: #1570ef; }
.confirmed-body { margin-top: 10px; padding: 10px 12px; background: #f8fafc; border: 1px dashed var(--border); border-radius: 8px; }
</style>
