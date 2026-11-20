<template>
  <section class="page" data-module="dispatch_board">
    <header class="page-head">
      <div>
        <h2>分发看板</h2>
        <p class="page-desc">
          源表事件明细按初投冻结目标展开，接收凭证聚合为分发回执；逐目标核对缺投与补发。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" data-testid="reset-board" type="button" :disabled="busy" @click="handleReset">
          重置演练
        </button>
        <button class="btn primary" data-testid="claim-missing" type="button" :disabled="busy || !board?.missing.length" @click="handleClaim">
          领取缺投明细
        </button>
      </div>
    </header>

    <p v-if="errorMessage" class="error-text" data-testid="board-error">{{ errorMessage }}</p>
    <p v-if="notice" class="notice-text" data-testid="board-notice">{{ notice }}</p>

    <div class="stat-row">
      <article v-for="item in summaryCards" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value" :data-testid="`stat-${item.key}`">{{ item.value }}</strong>
      </article>
    </div>

    <h3 class="section-title">目标覆盖</h3>
    <table class="data-table" data-testid="target-table">
      <thead>
        <tr>
          <th>目标通道</th>
          <th>预期目标数</th>
          <th>已完整接收</th>
          <th>缺投</th>
          <th>实际补发</th>
          <th>覆盖率</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in board?.targets ?? []" :key="row.target" :data-testid="`target-${row.target}`">
          <td>{{ row.target }}</td>
          <td>{{ row.expected }}</td>
          <td>{{ row.complete }}</td>
          <td :class="{ 'error-text': row.missing > 0 }">{{ row.missing }}</td>
          <td>{{ row.replays }}</td>
          <td>{{ coveragePercent(row) }}%</td>
        </tr>
      </tbody>
    </table>

    <h3 class="section-title">目标覆盖数据集（逐目标）</h3>
    <form class="filter-bar" @submit.prevent>
      <label class="filter-item">
        <span>按源事件筛选</span>
        <select v-model="eventFilter" data-testid="filter-event">
          <option value="">全部源事件</option>
          <option v-for="event in board?.source_events ?? []" :key="event.event_id" :value="event.event_id">
            {{ event.event_id }}
          </option>
        </select>
      </label>
      <label class="filter-item">
        <span>按接收状态筛选</span>
        <select v-model="statusFilter" data-testid="filter-status">
          <option value="">全部状态</option>
          <option value="缺投">仅缺投</option>
          <option value="已接收">仅已接收</option>
        </select>
      </label>
    </form>
    <table class="data-table" data-testid="coverage-table">
      <thead>
        <tr>
          <th>源事件</th>
          <th>源坐标</th>
          <th>操作</th>
          <th>目标通道</th>
          <th>冻结预期</th>
          <th>接收状态</th>
          <th>回执类别</th>
          <th>补发次数</th>
          <th>领取单</th>
          <th>登记回执</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="row in filteredCoverage"
          :key="row.dispatch_id"
          :data-testid="`coverage-${row.event_id}-${row.target}`"
          :data-status="row.coverage_status"
        >
          <td>{{ row.event_id }}</td>
          <td class="mono">{{ row.binlog_file }}:{{ row.binlog_pos }}</td>
          <td>{{ row.operation }}</td>
          <td>{{ row.target }}</td>
          <td>{{ row.rule_name }} v{{ row.rule_version }}</td>
          <td :class="{ 'error-text': !row.received }">{{ row.coverage_status }}</td>
          <td>{{ row.receipt_kind ?? '—' }}</td>
          <td>{{ row.replay_count }}</td>
          <td>{{ row.claim_id ?? '—' }}</td>
          <td class="row-actions">
            <button
              class="link"
              type="button"
              :data-testid="`receipt-${row.event_id}-${row.target}`"
              :disabled="busy || row.received"
              @click="handleLateReceipt(row)"
            >
              {{ row.received ? '已接收' : '登记回执' }}
            </button>
          </td>
        </tr>
        <tr v-if="!filteredCoverage.length">
          <td colspan="10" class="empty-state">没有符合筛选条件的目标覆盖记录</td>
        </tr>
      </tbody>
    </table>

    <h3 class="section-title">源事件与回放坐标</h3>
    <table class="data-table" data-testid="source-event-table">
      <thead>
        <tr>
          <th>事件ID</th>
          <th>来源表</th>
          <th>操作</th>
          <th>Binlog 坐标</th>
          <th>事务ID</th>
          <th>事件时间</th>
          <th>可回放</th>
          <th>冻结目标</th>
          <th>完整接收</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="event in board?.source_events ?? []" :key="event.event_id">
          <td>{{ event.event_id }}</td>
          <td>{{ event.source_db }}.{{ event.source_table }}</td>
          <td>{{ event.operation }}</td>
          <td class="mono">{{ event.binlog_file }}:{{ event.binlog_pos }}</td>
          <td>{{ event.tx_id }}</td>
          <td>{{ event.event_time }}</td>
          <td>{{ event.replayable ? '是' : '否' }}</td>
          <td>{{ event.frozen_targets.join(' / ') }}</td>
          <td>{{ event.complete_pairs }} / {{ event.expected_pairs }}</td>
        </tr>
      </tbody>
    </table>

    <h3 class="section-title">领取单与实际重放</h3>
    <p class="hint-text">
      回放范围以领取时的目标清单为准；领取后、重放前到达的确认记为迟到确认，
      会把该目标移出本轮重放，但不计入实际补发次数。
    </p>
    <table class="data-table" data-testid="claim-table">
      <thead>
        <tr>
          <th>领取单</th>
          <th>值守人</th>
          <th>领取时间</th>
          <th>目标数</th>
          <th>待重放</th>
          <th>已补发</th>
          <th>迟到关闭</th>
          <th>实际补发</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="claim in board?.claims ?? []" :key="claim.claim_id" :data-testid="`claim-${claim.claim_id}`">
          <td>{{ claim.claim_id }}</td>
          <td>{{ claim.operator }}</td>
          <td>{{ claim.claimed_at }}</td>
          <td>{{ claim.total }}</td>
          <td>{{ claim.pending }}</td>
          <td>{{ claim.replayed }}</td>
          <td>{{ claim.closed_by_late_receipt }}</td>
          <td>{{ claim.actual_replays }}</td>
          <td class="row-actions">
            <button
              class="link"
              type="button"
              :data-testid="`replay-${claim.claim_id}`"
              :disabled="busy || claim.pending === 0"
              @click="handleReplay(claim.claim_id)"
            >
              登记实际补发
            </button>
          </td>
        </tr>
        <tr v-if="!board?.claims.length">
          <td colspan="9" class="empty-state">尚未领取缺投目标</td>
        </tr>
      </tbody>
    </table>
    <div v-if="latestClaim" class="claim-detail" data-testid="claim-detail">
      <h4>领取清单明细：{{ latestClaim.claim_id }}</h4>
      <ul>
        <li v-for="pair in latestClaim.pairs" :key="`${pair.event_id}-${pair.target}`">
          {{ pair.event_id }} → {{ pair.target }}：{{ pair.state }}
          （补发次数 {{ pair.replay_count }}<span v-if="pair.closed_by_receipt">，迟到确认 {{
            pair.closed_by_receipt }}</span>）
        </li>
      </ul>
    </div>

    <h3 class="section-title">路由改配与冻结对照</h3>
    <table class="data-table" data-testid="drift-table">
      <thead>
        <tr>
          <th>源事件</th>
          <th>路由规则</th>
          <th>当前版本</th>
          <th>当前目标</th>
          <th>初投冻结目标</th>
          <th>是否漂移</th>
          <th>备注</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in board?.config_drift ?? []" :key="row.event_id">
          <td>{{ row.event_id }}</td>
          <td>{{ row.rule_name }}</td>
          <td>v{{ row.rule_version }}</td>
          <td>{{ row.current_targets.join(' / ') }}</td>
          <td>{{ row.frozen_targets.join(' / ') }}</td>
          <td>{{ row.changed ? '已改配（不回溯）' : '一致' }}</td>
          <td>{{ row.note || '—' }}</td>
        </tr>
      </tbody>
    </table>

    <h3 class="section-title">回执流水</h3>
    <table class="data-table" data-testid="receipt-table">
      <thead>
        <tr>
          <th>回执ID</th>
          <th>源事件</th>
          <th>目标</th>
          <th>类别</th>
          <th>到达时间</th>
          <th>计入完整接收</th>
          <th>说明</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="receipt in board?.receipts ?? []" :key="receipt.receipt_id">
          <td>{{ receipt.receipt_id }}</td>
          <td>{{ receipt.event_id }}</td>
          <td>{{ receipt.target }}</td>
          <td>{{ receipt.kind }}</td>
          <td>{{ receipt.received_at }}</td>
          <td>{{ receipt.counted_as_complete ? '是' : '否' }}</td>
          <td>{{ receipt.dedup_reason || receipt.channel || '—' }}</td>
        </tr>
      </tbody>
    </table>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  claimMissing,
  loadDispatchBoard,
  recordReceipt,
  registerReplays,
  resetBoard,
} from '@/api/dispatch-service'
import type { CoverageRow, DispatchBoard, TargetSummary } from '@/data/dispatch'

const board = ref<DispatchBoard | null>(null)
const busy = ref(false)
const errorMessage = ref('')
const notice = ref('')
const eventFilter = ref('')
const statusFilter = ref('')

const summaryCards = computed(() => {
  const summary = board.value?.summary
  return [
    { key: 'expected', label: '预期收发对数', value: summary?.expected_pairs ?? 0 },
    { key: 'complete', label: '完整接收对数', value: summary?.complete_pairs ?? 0 },
    { key: 'missing', label: '当前缺投对数', value: summary?.missing_pairs ?? 0 },
    { key: 'unclaimed', label: '待领取缺投', value: summary?.unclaimed_pairs ?? 0 },
    { key: 'late', label: '迟到确认数', value: summary?.late_receipts ?? 0 },
    { key: 'duplicate', label: '重复确认数', value: summary?.duplicate_receipts ?? 0 },
    { key: 'replays', label: '实际补发次数', value: summary?.actual_replays ?? 0 },
  ]
})

const filteredCoverage = computed(() => {
  const rows = board.value?.coverage ?? []
  return rows.filter((row) => {
    if (eventFilter.value && row.event_id !== eventFilter.value) return false
    if (statusFilter.value && row.coverage_status !== statusFilter.value) return false
    return true
  })
})

const latestClaim = computed(() => {
  const claims = board.value?.claims ?? []
  return claims.length ? claims[claims.length - 1] : null
})

function coveragePercent(row: TargetSummary): number {
  return Math.round((row.complete / Math.max(row.expected, 1)) * 100)
}

async function reload() {
  board.value = await loadDispatchBoard()
}

async function runTask(task: () => Promise<string>) {
  busy.value = true
  errorMessage.value = ''
  notice.value = ''
  try {
    notice.value = await task()
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '操作失败'
  } finally {
    busy.value = false
  }
}

function handleClaim() {
  void runTask(async () => (await claimMissing('晚班值守')).message)
}

function handleReplay(claimId: string) {
  void runTask(async () => (await registerReplays(claimId)).message)
}

function handleReset() {
  void runTask(async () => (await resetBoard()).message)
}

function handleLateReceipt(row: CoverageRow) {
  void runTask(async () => {
    const result = await recordReceipt({
      event_id: row.event_id,
      target: row.target,
      channel: `kafka://${row.target}/${row.source_table}`,
      note: '看板手工登记回执',
    })
    return result.message
  })
}

onMounted(async () => {
  try {
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '分发看板读取失败'
  }
})
</script>
