<template>
  <section class="page" data-module="ingestion">
    <header class="page-head">
      <div><h2>数据接入通道</h2><p class="page-desc">维护接入通道，并按输入格式与背压阈值把多通道分片合并成可核对的通道接入批次包：每批输入都可追溯到通道、格式与背压状态快照。</p></div>
      <div class="page-actions">
        <button class="btn primary" type="button" data-testid="open-create" @click="openCreate">登记接入通道</button>
        <button class="btn" type="button" data-testid="export" @click="exportRows">导出作业清单</button>
      </div>
    </header>

    <div class="cb-tabs" data-testid="cb-tabs">
      <button v-for="tab in tabs" :key="tab.key" type="button" class="cb-tab"
              :class="{ active: activeTab === tab.key }" :data-testid="`tab-${tab.key}`"
              @click="switchTab(tab.key)">{{ tab.label }}</button>
    </div>

    <!-- ============ Tab1：原有通道时间线 ============ -->
    <div v-show="activeTab === 'timeline'">
      <div class="filter-bar">
        <label><span>开始日期</span><input type="date" v-model="dateFrom" /></label>
        <label><span>结束日期</span><input type="date" v-model="dateTo" /></label>
        <button class="btn" type="button" @click="reload">查询</button>
      </div>
      <div class="timeline-wrap">
        <div v-for="item in timelineItems" :key="item.id" class="timeline-row">
          <span class="timeline-label">{{ item.name || item.label }}</span>
          <div class="timeline-track">
            <div class="timeline-bar" :class="`status-${item.status}`" :style="{ left: barLeft(item) + '%', width: barWidth(item) + '%' }" :data-testid="`bar-${item.id}`" :title="`${item.start} ~ ${item.end}`" @click="selectItem(item)">&nbsp;</div>
          </div>
          <span style="font-size:12px;color:var(--muted);margin-left:8px;">{{ item.start }} ~ {{ item.end }}</span>
        </div>
        <p v-if="!timelineItems.length" style="text-align:center;color:var(--muted)">暂无作业记录</p>
      </div>
      <div v-if="selectedItem" class="timeline-log" data-testid="timeline-log">
        <strong>{{ selectedItem.name || selectedItem.label }}</strong> — 状态：{{ selectedItem.status }}
        <div style="margin-top:4px;">{{ selectedItem.logs || '暂无日志' }}</div>
      </div>
    </div>

    <!-- ============ Tab2：缓冲队列分片 ============ -->
    <div v-show="activeTab === 'shards'">
      <div class="cb-toolbar">
        <label class="cb-field"><span>通道编号</span>
          <select v-model="shardChannel" data-testid="shard-channel-filter" @change="loadShards">
            <option value="">全部通道</option>
            <option v-for="c in channels" :key="c.通道编号" :value="c.通道编号">{{ c.通道编号 }} · {{ c.通道名称 }}</option>
          </select>
        </label>
        <button class="btn" type="button" @click="loadShards">刷新队列</button>
        <button class="btn primary" type="button" data-testid="open-register-shard" @click="openShardForm">登记输入分片</button>
      </div>
      <table class="data-table" data-testid="shard-table">
        <thead><tr>
          <th>分片编号</th><th>通道编号</th><th>输入格式</th><th>队列序号</th>
          <th>分片大小</th><th>背压阈值</th><th>入队时间</th><th>分片状态</th>
        </tr></thead>
        <tbody>
          <tr v-for="s in shards" :key="s.id" :data-testid="`shard-row-${s.id}`" :data-status="s.分片状态"
              :class="{ 'row-blocked': s.分片状态 === '超限拦截', 'row-batched': s.分片状态 === '已入批' }">
            <td>{{ s.分片编号 }}</td><td>{{ s.通道编号 }}</td><td>{{ s.输入格式 }}</td>
            <td>{{ s.队列序号 }}</td><td>{{ s.分片大小 }}</td><td>{{ s.背压阈值 }}</td>
            <td>{{ s.入队时间 }}</td>
            <td><span class="cb-pill" :class="pillClass(s.分片状态)">{{ s.分片状态 }}</span></td>
          </tr>
          <tr v-if="!shards.length"><td colspan="8" class="empty-state">缓冲队列暂无分片</td></tr>
        </tbody>
      </table>
      <p class="cb-hint">分片必须绑定通道与输入格式；单分片超过通道背压阈值即置「超限拦截」，永远不会进入完成批次。</p>

      <div v-if="showShardForm" class="modal-backdrop" data-testid="shard-modal">
        <form class="modal" @submit.prevent="submitShard">
          <h3>登记输入分片（绑定通道与格式）</h3>
          <label class="create-field"><span>通道编号</span>
            <select v-model="shardForm.通道编号" required data-testid="shard-form-channel">
              <option value="" disabled>请选择通道</option>
              <option v-for="c in channels" :key="c.通道编号" :value="c.通道编号">
                {{ c.通道编号 }} · {{ c.通道名称 }}（阈值 {{ c.背压阈值 }}）
              </option>
            </select>
          </label>
          <label class="create-field"><span>输入格式</span>
            <input v-model="shardForm.输入格式" list="format-options" required data-testid="shard-form-format" placeholder="如 JSON / CSV / XML" />
          </label>
          <datalist id="format-options"><option value="JSON" /><option value="CSV" /><option value="XML" /></datalist>
          <label class="create-field"><span>分片大小</span>
            <input v-model.number="shardForm.分片大小" type="number" min="1" required data-testid="shard-form-size" />
          </label>
          <p v-if="shardMessage" class="error-text">{{ shardMessage }}</p>
          <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:12px">
            <button class="btn" type="button" @click="showShardForm = false">取消</button>
            <button class="btn primary" type="submit" data-testid="submit-shard">提交分片</button>
          </div>
        </form>
      </div>
    </div>

    <!-- ============ Tab3：通道接入批次包 ============ -->
    <div v-show="activeTab === 'batches'">
      <div class="cb-build-panel" data-testid="build-panel">
        <div class="cb-build-title">多选接入通道整组构建（成功/失败逐台记录）</div>
        <div class="cb-channel-pick">
          <label v-for="c in channels" :key="c.通道编号" class="cb-check" :data-testid="`pick-${c.通道编号}`">
            <input type="checkbox" :value="c.通道编号" v-model="pickedChannels" />
            <span>{{ c.通道编号 }} · {{ c.通道名称 }}</span>
            <small>格式 {{ c.输入格式 }} ｜ 阈值 {{ c.背压阈值 }} ｜ 队列 {{ c.缓冲队列 }}</small>
          </label>
        </div>
        <div class="cb-toolbar">
          <label class="cb-field"><span>合并策略</span>
            <select v-model="strategy" data-testid="strategy-select">
              <option value="channel_first">按通道合并（背压边界前置，阈值处切开批次）</option>
              <option value="format_first">按输入格式成批（格式前置决定子批次结构）</option>
            </select>
          </label>
          <button class="btn" type="button" data-testid="preview-batch" @click="previewBuild">切批预演（不落库）</button>
          <button class="btn primary" type="button" data-testid="build-batch" @click="buildNow">整组写入批次包</button>
        </div>
        <p v-if="buildMessage" :class="buildOk ? 'cb-ok' : 'error-text'" data-testid="build-message">{{ buildMessage }}</p>

        <div v-if="preview" class="cb-preview" data-testid="preview-box">
          <strong>预演结果（{{ preview.策略说明 }}）— 与另一策略对比可看到批次包结构差异</strong>
          <div v-for="ch in preview.通道预演" :key="ch.通道编号" class="cb-preview-ch">
            <div class="cb-preview-head">{{ ch.通道编号 }}（阈值 {{ ch.背压阈值 }}，待入批 {{ ch.待入批分片数 }}）
              <span v-if="ch.超限拦截分片.length" class="cb-pill danger">超限拦截：{{ ch.超限拦截分片.join('、') }}</span>
            </div>
            <table class="data-table cb-inner-table">
              <thead><tr><th>预演 leaf 批次</th><th>输入格式</th><th>分片来源</th><th>批次大小/阈值</th></tr></thead>
              <tbody>
                <tr v-for="(leaf, idx) in ch.预演leaf批次" :key="idx">
                  <td>#{{ idx + 1 }}</td><td>{{ leaf.输入格式 }}</td>
                  <td>{{ leaf.分片编号列表.join('、') }}</td>
                  <td :class="leaf.批次大小 > ch.背压阈值 ? 'error-text' : ''">{{ leaf.批次大小 }} / {{ ch.背压阈值 }}</td>
                </tr>
                <tr v-if="!ch.预演leaf批次.length"><td colspan="4" class="empty-state">无背压完整分片可成批</td></tr>
              </tbody>
            </table>
          </div>
        </div>
      </div>

      <h3 class="cb-subtitle">通道接入批次包（接入通道服务与格式解析服务读同一份）</h3>
      <div class="cb-toolbar">
        <label class="cb-field"><span>批次层级</span>
          <select v-model="batchLevel" @change="loadBatches">
            <option value="">全部</option><option value="leaf">leaf 解析批次</option>
            <option value="channel">通道汇总批次</option><option value="group">整组批次</option>
          </select>
        </label>
        <button class="btn" type="button" @click="loadBatches">刷新批次</button>
        <button class="btn ghost" type="button" data-testid="load-records" @click="loadRecords">查看构建记录</button>
      </div>
      <table class="data-table" data-testid="batch-table">
        <thead><tr>
          <th>批次编号</th><th>层级</th><th>通道</th><th>输入格式</th><th>合并策略</th>
          <th>分片数</th><th>大小/背压阈值</th><th>状态</th><th>数据版本</th><th>操作</th>
        </tr></thead>
        <tbody>
          <tr v-for="b in batches" :key="b.id" :data-testid="`batch-row-${b.批次编号}`">
            <td>{{ b.批次编号 }}</td><td>{{ b.批次层级 }}</td>
            <td>{{ b.通道编号 }}</td><td>{{ b.输入格式 }}</td>
            <td>{{ strategyLabel(b.合并策略) }}</td>
            <td>{{ b.分片数量 }}</td>
            <td>{{ b.批次大小 }}<template v-if="b.背压阈值"> / {{ b.背压阈值 }}</template></td>
            <td><span class="cb-pill" :class="pillClass(b.审核状态)">{{ b.批次状态 }} · {{ b.审核状态 }}</span></td>
            <td>{{ b.数据版本 }}</td>
            <td><button class="link" type="button" :data-testid="`detail-${b.批次编号}`" @click="openDetail(b.批次编号)">查看快照</button></td>
          </tr>
          <tr v-if="!batches.length"><td colspan="10" class="empty-state">暂无批次包，请先整组构建</td></tr>
        </tbody>
      </table>

      <div v-if="records.length" class="cb-records" data-testid="records-box">
        <h3 class="cb-subtitle">构建记录（逐台留痕）</h3>
        <table class="data-table">
          <thead><tr><th>构建记录</th><th>策略</th><th>成功通道</th><th>失败通道</th><th>逐台结果</th><th>时间</th></tr></thead>
          <tbody>
            <tr v-for="r in records" :key="r.id">
              <td>{{ r.构建记录编号 }}</td><td>{{ strategyLabel(r.合并策略) }}</td>
              <td>{{ r.成功通道.join('、') || '—' }}</td>
              <td :class="r.失败通道.length ? 'error-text' : ''">{{ r.失败通道.join('、') || '—' }}</td>
              <td><span v-for="(x, i) in r.结果明细" :key="i" class="cb-pill" :class="x.ok ? 'ok' : 'danger'">
                {{ x.通道编号 }}{{ x.ok ? '成功' : '失败' }}</span></td>
              <td>{{ r.构建时间 }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <!-- 批次详情：分片来源 + 背压切断点 + 快照 -->
    <div v-if="detail" class="modal-backdrop" data-testid="detail-modal">
      <div class="modal cb-detail">
        <h3>批次包 {{ detail.批次编号 }} 快照</h3>
        <dl class="cb-dl">
          <dt>批次层级</dt><dd>{{ detail.批次层级 }}</dd>
          <dt>通道 / 格式</dt><dd>{{ detail.通道编号 }} / {{ detail.输入格式 }}</dd>
          <dt>合并策略</dt><dd>{{ strategyLabel(detail.合并策略) }}</dd>
          <dt>批次大小 / 背压阈值</dt><dd>{{ detail.批次大小 }} / {{ detail.背压阈值 ?? '—' }}</dd>
          <dt>数据版本</dt><dd>{{ detail.数据版本 }}</dd>
          <dt>背压切断点</dt><dd>{{ detail.切断点说明 }}</dd>
          <dt>通道快照</dt><dd><code>{{ JSON.stringify(detail.通道快照) }}</code></dd>
          <dt>背压快照</dt><dd><code>{{ JSON.stringify(detail.背压快照) }}</code></dd>
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
        <div v-if="detail.工作台项" class="cb-review-box">
          <h4>解析侧待审核工作台项</h4>
          <p>工作台项 {{ detail.工作台项.工作台项编号 }} ｜ 审核状态 {{ detail.工作台项.审核状态 }}
             ｜ 匹配解析 {{ detail.工作台项.解析编号 || '暂无' }}</p>
          <p v-if="detail.工作台项.编排指令">编排指令：{{ detail.工作台项.编排指令 }}</p>
        </div>
        <div style="display:flex;justify-content:flex-end;margin-top:12px">
          <button class="btn primary" type="button" @click="detail = null">关闭</button>
        </div>
      </div>
    </div>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate"><h3>登记接入通道</h3>
        <label v-for="field in columns.slice(0,3)" :key="field" class="create-field"><span>{{ field }}</span><input v-model="createValues[field]" :placeholder="`请输入${field}`" required /></label>
        <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:12px">
          <button class="btn" type="button" @click="showCreate=false">取消</button>
          <button class="btn primary" type="submit" data-testid="submit-create">提交</button>
        </div>
      </form>
    </div>
  </section>
</template>
<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { request } from '@/api/client'
const ENDPOINT = '/api/ingestion'
const columns = ["通道编号", "通道名称", "输入格式", "解析引擎", "并发数量", "背压阈值", "缓冲队列", "通道状态"]
const tabs = [
  { key: 'timeline', label: '通道时间线' },
  { key: 'shards', label: '缓冲队列分片' },
  { key: 'batches', label: '通道接入批次包' },
]
const activeTab = ref('timeline')

// ---- 时间线（保留原逻辑） ----
const timelineItems = ref<any[]>([])
const selectedItem = ref<any>(null)
const dateFrom = ref(''); const dateTo = ref('')
const errorMessage = ref(''); const showCreate = ref(false)
const createValues = ref<Record<string,string>>({})
const allItems = ref<any[]>([])
function barLeft(item: any) { const all = allItems.value.filter(i => i.start && i.end); if(!all.length) return 0; const min = Math.min(...all.map(i => new Date(i.start).getTime())); const max = Math.max(...all.map(i => new Date(i.end).getTime())); const range = max - min || 86400000; return ((new Date(item.start).getTime() - min) / range) * 100 }
function barWidth(item: any) { const all = allItems.value.filter(i => i.start && i.end); if(!all.length) return 20; const min = Math.min(...all.map(i => new Date(i.start).getTime())); const max = Math.max(...all.map(i => new Date(i.end).getTime())); const range = max - min || 86400000; return Math.max(5, ((new Date(item.end).getTime() - new Date(item.start).getTime()) / range) * 100) }
function selectItem(item: any) { selectedItem.value = selectedItem.value?.id === item.id ? null : item }
async function reload() {
  try { const res = await request(ENDPOINT); const data = await res.json(); allItems.value = data.items || []; applyFilter() } catch(e) { errorMessage.value = '加载失败' }
}
function applyFilter() {
  let items = allItems.value
  if(dateFrom.value) items = items.filter((i: any) => i.start >= dateFrom.value)
  if(dateTo.value) items = items.filter((i: any) => i.end <= dateTo.value)
  timelineItems.value = items; selectedItem.value = null
}
function openCreate() { createValues.value = {}; showCreate.value = true }
async function submitCreate() {
  try { const res = await request(ENDPOINT, { method: 'POST', body: JSON.stringify({ values: createValues.value }) }); if(!res.ok) throw new Error('创建失败'); showCreate.value = false; await reload() } catch(e: any) { errorMessage.value = e.message }
}
function exportRows() { window.open(`${ENDPOINT}/export`, '_blank') }

// ---- 通道（批次构建时多选） ----
type Channel = Record<string, any>
const channels = ref<Channel[]>([])
async function loadChannels() {
  const res = await request(ENDPOINT); const data = await res.json()
  // 时间线接口只有摘要，明细从列表接口的种子行取：这里直接用分片/批次接口里的通道编号不全，改用 detail
  const ids = (data.items || []).map((i: any) => i.id)
  const details = await Promise.all(ids.map((id: number) => request(`${ENDPOINT}/${id}`).then(r => r.json())))
  channels.value = details
}

// ---- 分片 ----
const shards = ref<any[]>([])
const shardChannel = ref('')
const showShardForm = ref(false)
const shardForm = ref<Record<string, any>>({ 通道编号: '', 输入格式: '', 分片大小: undefined })
const shardMessage = ref('')
async function loadShards() {
  const qs = shardChannel.value ? `?channel=${encodeURIComponent(shardChannel.value)}` : ''
  const res = await request(`${ENDPOINT}/shards/list${qs}`)
  const data = await res.json(); shards.value = data.items || []
}
function openShardForm() { shardForm.value = { 通道编号: shardChannel.value || '', 输入格式: '', 分片大小: undefined }; shardMessage.value = ''; showShardForm.value = true }
async function submitShard() {
  shardMessage.value = ''
  const res = await request(`${ENDPOINT}/shards`, { method: 'POST', body: JSON.stringify({ values: shardForm.value }) })
  const payload = await res.json()
  if (!payload.ok) { shardMessage.value = payload.message; return }
  showShardForm.value = false; await loadShards()
}

// ---- 批次构建 ----
const pickedChannels = ref<string[]>([])
const strategy = ref('channel_first')
const preview = ref<any>(null)
const buildMessage = ref(''); const buildOk = ref(true)
const batches = ref<any[]>([])
const batchLevel = ref('')
const records = ref<any[]>([])
const detail = ref<any>(null)

function strategyLabel(key: string) {
  return key === 'channel_first' ? '按通道合并（背压前置）' : key === 'format_first' ? '按格式成批（格式前置）' : key
}
function pillClass(status: string) {
  if (status === '超限拦截' || status === '已驳回') return 'danger'
  if (status === '已入批' || status === '已通过') return 'ok'
  return 'pending'
}
async function previewBuild() {
  buildMessage.value = ''
  if (!pickedChannels.value.length) { buildOk.value = false; buildMessage.value = '请至少勾选一个接入通道'; return }
  const res = await request(`${ENDPOINT}/batches/preview`, {
    method: 'POST', body: JSON.stringify({ 通道编号列表: pickedChannels.value, 合并策略: strategy.value }),
  })
  const payload = await res.json()
  if (!payload.ok) { buildOk.value = false; buildMessage.value = payload.message; return }
  preview.value = payload.entry
}
async function buildNow() {
  buildMessage.value = ''
  if (!pickedChannels.value.length) { buildOk.value = false; buildMessage.value = '请至少勾选一个接入通道'; return }
  const res = await request(`${ENDPOINT}/batches/build`, {
    method: 'POST', body: JSON.stringify({ 通道编号列表: pickedChannels.value, 合并策略: strategy.value }),
  })
  const payload = await res.json()
  buildOk.value = payload.ok
  const lines = (payload.entry?.results || []).map((r: any) =>
    `${r.通道编号}：${r.ok ? '成功' : '失败'}${r.ok ? `（${r.leaf批次?.length ?? 0} 个 leaf 批次，版本 ${r.数据版本}）` : `（${r.message}）`}`)
  buildMessage.value = `${payload.message}｜${lines.join('；')}`
  preview.value = null
  await loadBatches(); await loadShards(); await loadChannels()
}
async function loadBatches() {
  const qs = batchLevel.value ? `?level=${encodeURIComponent(batchLevel.value)}` : ''
  const res = await request(`${ENDPOINT}/batches/list${qs}`)
  const data = await res.json(); batches.value = data.items || []
}
async function loadRecords() {
  const res = await request(`${ENDPOINT}/batches/build-records`)
  const data = await res.json(); records.value = data.items || []
}
async function openDetail(batchNo: string) {
  const res = await request(`${ENDPOINT}/batches/${batchNo}/detail`)
  detail.value = await res.json()
}
function switchTab(key: string) {
  activeTab.value = key
  if (key === 'shards') void loadShards()
  if (key === 'batches') { void loadBatches(); void loadChannels() }
}

onMounted(() => { void reload(); void loadChannels() })
</script>

<style scoped>
.cb-tabs { display: flex; gap: 8px; margin: 8px 0 16px; }
.cb-tab { border: 1px solid var(--border); background: #fff; border-radius: 6px; padding: 6px 16px; cursor: pointer; font-size: 13px; }
.cb-tab.active { background: var(--brand); border-color: var(--brand); color: #fff; }
.cb-toolbar { display: flex; flex-wrap: wrap; gap: 12px; align-items: flex-end; margin: 10px 0; }
.cb-field span { display: block; font-size: 12px; color: var(--muted); margin-bottom: 4px; }
.cb-field select, .cb-field input { padding: 6px 10px; border: 1px solid var(--border); border-radius: 6px; font-size: 13px; min-width: 220px; }
.cb-build-panel { background: #fff; border: 1px solid var(--border); border-radius: 10px; padding: 14px 16px; margin-bottom: 16px; }
.cb-build-title { font-weight: 600; margin-bottom: 10px; }
.cb-channel-pick { display: flex; flex-wrap: wrap; gap: 10px; margin-bottom: 12px; }
.cb-check { display: flex; flex-direction: column; gap: 2px; border: 1px solid var(--border); border-radius: 8px; padding: 8px 12px; cursor: pointer; min-width: 240px; }
.cb-check small { color: var(--muted); }
.cb-pill { display: inline-block; padding: 2px 8px; border-radius: 999px; font-size: 12px; background: #e2e8f0; color: #334155; }
.cb-pill.danger { background: #fee2e2; color: #b42318; }
.cb-pill.ok { background: #dcfce7; color: #166534; }
.cb-pill.pending { background: #fef9c3; color: #854d0e; }
.row-blocked { background: #fef2f2; }
.row-batched { color: var(--muted); }
.cb-hint { font-size: 12px; color: var(--muted); margin-top: 8px; }
.cb-preview { border-top: 1px dashed var(--border); margin-top: 12px; padding-top: 10px; }
.cb-preview-ch { margin: 10px 0; }
.cb-preview-head { font-size: 13px; margin-bottom: 6px; display: flex; gap: 10px; align-items: center; }
.cb-inner-table { margin-top: 6px; }
.cb-subtitle { margin: 18px 0 8px; font-size: 15px; }
.cb-records { margin-top: 16px; }
.cb-ok { color: #166534; }
.cb-detail { width: min(820px, 100%); }
.cb-dl { display: grid; grid-template-columns: 130px 1fr; gap: 6px 12px; margin: 8px 0; }
.cb-dl dt { color: var(--muted); font-size: 12px; }
.cb-dl dd { margin: 0; font-size: 13px; }
.cb-dl code { background: #f1f5f9; padding: 2px 6px; border-radius: 4px; word-break: break-all; }
.cb-review-box { background: #f8fafc; border: 1px solid var(--border); border-radius: 8px; padding: 10px 12px; margin-top: 12px; }
</style>
