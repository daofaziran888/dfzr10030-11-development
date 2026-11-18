<template>
  <section class="page" data-module="parser">
    <header class="page-head">
      <div>
        <h2>格式解析器管理</h2>
        <p class="page-desc">维护解析配置，并读取接入通道服务写入的同一份通道接入批次包：按输入格式选择字段映射、审核批次并编排接入通道后续处理。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">登记解析配置</button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">导出格式解析器清单</button>
      </div>
    </header>

    <div class="cb-tabs" data-testid="cb-tabs">
      <button v-for="tab in tabs" :key="tab.key" type="button" class="cb-tab"
              :class="{ active: activeTab === tab.key }" :data-testid="`tab-${tab.key}`"
              @click="switchTab(tab.key)">{{ tab.label }}</button>
    </div>

    <!-- ============ Tab1：原有解析配置 CRUD ============ -->
    <div v-show="activeTab === 'config'">
      <div class="stat-row">
        <article v-for="item in stats" :key="item.label" class="stat-card">
          <span class="stat-label">{{ item.label }}</span>
          <strong class="stat-value">{{ item.value }}</strong>
        </article>
      </div>

      <form class="filter-bar" @submit.prevent="reload">
        <label v-for="field in filterFields" :key="field" class="filter-item">
          <span>{{ field }}</span>
          <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
        </label>
        <button class="btn" data-testid="query" type="submit">查询</button>
        <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
      </form>

      <table class="data-table">
        <thead>
          <tr>
            <th v-for="column in columns" :key="column">{{ column }}</th>
            <th>可执行动作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
            <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
            <td class="row-actions">
              <button
                v-for="action in actions"
                :key="action"
                class="link"
                type="button"
                :data-testid="`action-${row.id}-${action}`"
                @click="runAction(action, row)"
              >
                {{ action }}
              </button>
            </td>
          </tr>
          <tr v-if="!rows.length">
            <td :colspan="columns.length + 1" class="empty-state">暂无格式解析器数据，可先登记解析配置</td>
          </tr>
        </tbody>
      </table>

      <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
        <form class="modal" @submit.prevent="submitCreate">
          <h3>登记解析配置</h3>
          <label v-for="field in columns" :key="field" class="create-field">
            <span>{{ field }}</span>
            <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
          </label>
          <div class="page-actions">
            <button class="btn" type="button" @click="showCreate = false">取消</button>
            <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
              {{ creating ? '提交中…' : '确认登记' }}
            </button>
          </div>
        </form>
      </div>

      <footer class="page-foot">
        <span>共 {{ total }} 条格式解析器记录</span>
        <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
      </footer>
    </div>

    <!-- ============ Tab2：批次包（按批次选择映射） ============ -->
    <div v-show="activeTab === 'batches'">
      <div class="cb-toolbar">
        <label class="cb-field"><span>按输入格式过滤（据此选择字段映射）</span>
          <select v-model="batchFormat" data-testid="batch-format-filter" @change="loadBatches">
            <option value="">全部格式</option>
            <option value="JSON">JSON</option><option value="CSV">CSV</option><option value="XML">XML</option>
          </select>
        </label>
        <button class="btn" type="button" @click="loadBatches">刷新批次包</button>
      </div>
      <p class="cb-hint">解析端只读取「背压完整」的 leaf 批次；数据与接入通道页同一份，不落地第二份副本。</p>
      <table class="data-table" data-testid="parser-batch-table">
        <thead><tr>
          <th>批次编号</th><th>通道</th><th>输入格式</th><th>分片数</th><th>大小/阈值</th>
          <th>审核状态</th><th>建议解析配置</th><th>操作</th>
        </tr></thead>
        <tbody>
          <tr v-for="b in batches" :key="b.批次编号" :data-testid="`pbatch-row-${b.批次编号}`">
            <td>{{ b.批次编号 }}</td><td>{{ b.通道编号 }}</td><td>{{ b.输入格式 }}</td>
            <td>{{ b.分片数量 }}</td><td>{{ b.批次大小 }} / {{ b.背压阈值 }}</td>
            <td><span class="cb-pill" :class="pillClass(b.审核状态)">{{ b.审核状态 }}</span></td>
            <td>{{ suggestMap[b.批次编号] || '匹配中…' }}</td>
            <td class="row-actions">
              <button class="link" type="button" :data-testid="`pbatch-detail-${b.批次编号}`" @click="openDetail(b.批次编号)">批次快照</button>
            </td>
          </tr>
          <tr v-if="!batches.length"><td colspan="8" class="empty-state">暂无背压完整的 leaf 批次</td></tr>
        </tbody>
      </table>
    </div>

    <!-- ============ Tab3：待审核工作台 ============ -->
    <div v-show="activeTab === 'review'">
      <div class="cb-toolbar">
        <label class="cb-field"><span>审核状态</span>
          <select v-model="reviewStatus" @change="loadReviews">
            <option value="">全部</option><option value="待审核">待审核</option>
            <option value="已通过">已通过</option><option value="已驳回">已驳回</option>
          </select>
        </label>
        <button class="btn" type="button" @click="loadReviews">刷新工作台</button>
      </div>
      <table class="data-table" data-testid="review-table">
        <thead><tr>
          <th>工作台项</th><th>批次编号</th><th>通道</th><th>输入格式</th>
          <th>匹配解析/字段映射</th><th>审核状态</th><th>编排指令</th><th>操作</th>
        </tr></thead>
        <tbody>
          <tr v-for="r in reviews" :key="r.id" :data-testid="`review-row-${r.工作台项编号}`">
            <td>{{ r.工作台项编号 }}</td><td>{{ r.批次编号 }}</td><td>{{ r.通道编号 }}</td>
            <td>{{ r.输入格式 }}</td>
            <td>{{ r.解析编号 || '暂无匹配' }}<br /><small>{{ r.字段映射摘要 }}</small></td>
            <td><span class="cb-pill" :class="pillClass(r.审核状态)">{{ r.审核状态 }}</span></td>
            <td>{{ r.编排指令 || '—' }}</td>
            <td class="row-actions">
              <template v-if="r.审核状态 === '待审核'">
                <button class="link" type="button" :data-testid="`review-pass-${r.批次编号}`" @click="doReview(r, '已通过')">通过</button>
                <button class="link danger-link" type="button" :data-testid="`review-reject-${r.批次编号}`" @click="doReview(r, '已驳回')">驳回</button>
              </template>
              <button v-if="r.审核状态 === '已通过' && !r.编排指令" class="link" type="button"
                      :data-testid="`orchestrate-${r.批次编号}`" @click="doOrchestrate(r)">编排后续处理</button>
              <span v-else-if="r.编排指令" class="cb-hint">已编排</span>
            </td>
          </tr>
          <tr v-if="!reviews.length"><td colspan="8" class="empty-state">暂无工作台项</td></tr>
        </tbody>
      </table>
      <p v-if="workMessage" class="cb-ok" data-testid="work-message">{{ workMessage }}</p>
    </div>

    <!-- 批次快照详情 -->
    <div v-if="detail" class="modal-backdrop" data-testid="parser-detail-modal">
      <div class="modal cb-detail">
        <h3>批次包 {{ detail.批次编号 }}</h3>
        <dl class="cb-dl">
          <dt>通道 / 格式</dt><dd>{{ detail.通道编号 }} / {{ detail.输入格式 }}</dd>
          <dt>合并策略</dt><dd>{{ strategyLabel(detail.合并策略) }}</dd>
          <dt>批次大小 / 背压阈值</dt><dd>{{ detail.批次大小 }} / {{ detail.背压阈值 }}</dd>
          <dt>数据版本</dt><dd>{{ detail.数据版本 }}</dd>
          <dt>背压切断点</dt><dd>{{ detail.切断点说明 }}</dd>
          <dt>通道快照</dt><dd><code>{{ JSON.stringify(detail.通道快照) }}</code></dd>
          <dt>建议解析配置</dt><dd>{{ detail.建议解析配置 ? detail.建议解析配置.解析编号 + '（' + detail.建议解析配置.字段映射 + '）' : '暂无同格式解析配置' }}</dd>
        </dl>
        <h4>分片来源（{{ detail.分片明细.length }}）</h4>
        <table class="data-table cb-inner-table">
          <thead><tr><th>分片编号</th><th>通道</th><th>输入格式</th><th>队列序号</th><th>大小</th><th>状态</th></tr></thead>
          <tbody>
            <tr v-for="s in detail.分片明细" :key="s.分片编号">
              <td>{{ s.分片编号 }}</td><td>{{ s.通道编号 }}</td><td>{{ s.输入格式 }}</td>
              <td>{{ s.队列序号 }}</td><td>{{ s.分片大小 }}</td><td>{{ s.分片状态 }}</td>
            </tr>
          </tbody>
        </table>
        <div style="display:flex;justify-content:flex-end;margin-top:12px">
          <button class="btn primary" type="button" @click="detail = null">关闭</button>
        </div>
      </div>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>

const ENDPOINT = '/api/parser'
const columns = ["解析编号", "输入格式", "输出格式", "字段映射", "类型转换", "空值处理", "异常策略", "解析状态"]
const actions = ["提交配置", "启用解析", "废弃解析"]
const statuses = ["草稿", "已配置", "生效中", "已废弃"]
const metricNames = ["解析器总数", "生效中解析器", "已废弃解析器"]

const tabs = [
  { key: 'config', label: '解析配置' },
  { key: 'batches', label: '通道批次包（按批次选映射）' },
  { key: 'review', label: '待审核工作台' },
]
const activeTab = ref('config')

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const stats = computed(() => [
  { label: metricNames[0], value: total.value },
  { label: metricNames[1], value: rows.value.filter((row) => row.status === statuses[1]).length },
  { label: metricNames[2], value: rows.value.filter((row) => row.status === statuses[statuses.length - 1]).length },
])

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value).toString()
  try {
    const response = await request(`${ENDPOINT}/export?${query}`)
    if (!response.ok) throw new Error('导出失败，请稍后重试')
    const url = URL.createObjectURL(await response.blob())
    const link = document.createElement('a')
    link.href = url
    link.download = 'parser.csv'
    link.click()
    URL.revokeObjectURL(url)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(columns.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const response = await request(ENDPOINT, {
      method: 'POST', body: JSON.stringify(createValues.value),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '登记失败')
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    if (!response.ok) {
      throw new Error('格式解析器动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '格式解析器操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('解析配置列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '格式解析器列表读取失败'
  }
}

// ---- 通道接入批次包：解析侧只读同一份 ----
const batches = ref<any[]>([])
const batchFormat = ref('')
const detail = ref<any>(null)
const suggestMap = ref<Record<string, string>>({})

function strategyLabel(key: string) {
  return key === 'channel_first' ? '按通道合并（背压前置）' : key === 'format_first' ? '按格式成批（格式前置）' : key
}
function pillClass(status: string) {
  if (status === '超限拦截' || status === '已驳回') return 'danger'
  if (status === '已通过') return 'ok'
  return 'pending'
}
async function loadBatches() {
  const qs = batchFormat.value ? `?input_format=${encodeURIComponent(batchFormat.value)}` : ''
  const response = await request(`${ENDPOINT}/channel-batches/list${qs}`)
  const payload = await response.json()
  batches.value = payload.items ?? []
  const map: Record<string, string> = {}
  await Promise.all(batches.value.map(async (b) => {
    const d = await (await request(`${ENDPOINT}/channel-batches/${b.批次编号}/detail`)).json()
    map[b.批次编号] = d.建议解析配置 ? `${d.建议解析配置.解析编号}（${d.输入格式} 映射）` : '暂无同格式配置'
  }))
  suggestMap.value = map
}
async function openDetail(batchNo: string) {
  const response = await request(`${ENDPOINT}/channel-batches/${batchNo}/detail`)
  detail.value = await response.json()
}

// ---- 待审核工作台 ----
const reviews = ref<any[]>([])
const reviewStatus = ref('待审核')
const workMessage = ref('')
async function loadReviews() {
  const qs = reviewStatus.value ? `?status=${encodeURIComponent(reviewStatus.value)}` : ''
  const response = await request(`${ENDPOINT}/review-workbench/list${qs}`)
  const payload = await response.json()
  reviews.value = payload.items ?? []
}
async function doReview(row: any, action: string) {
  workMessage.value = ''
  const response = await request(`${ENDPOINT}/channel-batches/${row.批次编号}/review`, {
    method: 'POST', body: JSON.stringify({ action }),
  })
  const payload = await response.json()
  if (!payload.ok) { workMessage.value = payload.message; return }
  workMessage.value = payload.message
  await loadReviews(); await loadBatches()
}
async function doOrchestrate(row: any) {
  workMessage.value = ''
  const response = await request(`${ENDPOINT}/channel-batches/${row.批次编号}/orchestrate`, {
    method: 'POST', body: JSON.stringify({ action: '编排' }),
  })
  const payload = await response.json()
  if (!payload.ok) { workMessage.value = payload.message; return }
  workMessage.value = payload.message
  await loadReviews()
}

function switchTab(key: string) {
  activeTab.value = key
  if (key === 'batches') void loadBatches()
  if (key === 'review') void loadReviews()
}

onMounted(reload)
</script>

<style scoped>
.cb-tabs { display: flex; gap: 8px; margin: 8px 0 16px; }
.cb-tab { border: 1px solid var(--border); background: #fff; border-radius: 6px; padding: 6px 16px; cursor: pointer; font-size: 13px; }
.cb-tab.active { background: var(--brand); border-color: var(--brand); color: #fff; }
.cb-toolbar { display: flex; flex-wrap: wrap; gap: 12px; align-items: flex-end; margin: 10px 0; }
.cb-field span { display: block; font-size: 12px; color: var(--muted); margin-bottom: 4px; }
.cb-field select { padding: 6px 10px; border: 1px solid var(--border); border-radius: 6px; font-size: 13px; min-width: 260px; }
.cb-pill { display: inline-block; padding: 2px 8px; border-radius: 999px; font-size: 12px; background: #fef9c3; color: #854d0e; }
.cb-pill.danger { background: #fee2e2; color: #b42318; }
.cb-pill.ok { background: #dcfce7; color: #166534; }
.cb-hint { font-size: 12px; color: var(--muted); }
.cb-ok { color: #166534; }
.danger-link { color: #b42318; }
.cb-detail { width: min(820px, 100%); }
.cb-dl { display: grid; grid-template-columns: 130px 1fr; gap: 6px 12px; margin: 8px 0; }
.cb-dl dt { color: var(--muted); font-size: 12px; }
.cb-dl dd { margin: 0; font-size: 13px; }
.cb-dl code { background: #f1f5f9; padding: 2px 6px; border-radius: 4px; word-break: break-all; }
.cb-inner-table { margin-top: 6px; }
</style>
