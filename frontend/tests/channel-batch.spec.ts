import { expect, test } from '@playwright/test'

/**
 * 通道接入批次包端到端验收：
 * - 接入通道页：分片绑定通道/格式、超背压拦截、两种合并边界预演、整组构建逐台记录、
 *   批次包快照（分片来源/背压切断点/通道与背压快照）、构建记录；
 * - 格式解析页：读同一份批次包、按输入格式过滤、待审核工作台审核与编排；
 * - 硬底线：超阈值分片不进任何完成批次；leaf 全部背压完整且通道可追溯。
 */
test.describe('通道接入批次包：多输入通道批量接入', () => {
  const browserErrors: string[] = []

  test.beforeEach(async ({ page }) => {
    browserErrors.length = 0
    page.on('pageerror', (e) => browserErrors.push(e.message))
    page.on('response', (r) => {
      if (r.url().includes('/api/') && r.status() >= 400) browserErrors.push(`${r.status()} ${r.url()}`)
    })
  })

  test.afterEach(() => {
    expect(browserErrors).toEqual([])
  })

  test('接入通道页：分片登记、双边界预演、整组构建与快照可核对', async ({ page }) => {
    await page.goto('/ingestion')

    // 原有时间线能力保持不变
    await expect(page.locator('.timeline-bar').first()).toBeVisible()

    // 切到缓冲队列分片
    await page.getByTestId('tab-shards').click()
    await expect(page.getByTestId('shard-table')).toBeVisible()
    const seedShardRows = page.locator('[data-testid^="shard-row-"]')
    await expect(seedShardRows).toHaveCount(11)
    // 种子里的超阈值分片已被拦截
    await expect(page.locator('[data-testid="shard-row-4"][data-status="超限拦截"]')).toHaveCount(1)

    // 登记一个超阈值分片：通道2阈值80，登记95
    await page.getByTestId('open-register-shard').click()
    await page.getByTestId('shard-modal').waitFor()
    await page.getByTestId('shard-form-channel').selectOption('INGE-0002')
    await page.getByTestId('shard-form-format').fill('XML')
    await page.getByTestId('shard-form-size').fill('95')
    await page.getByTestId('submit-shard').click()
    await expect(page.locator('[data-status="超限拦截"]')).toHaveCount(3)

    // 切到批次包页，先预演两种边界
    await page.getByTestId('tab-batches').click()
    await page.getByTestId('pick-INGE-0001').check()
    await page.getByTestId('pick-INGE-0002').check()

    await page.getByTestId('strategy-select').selectOption('channel_first')
    await page.getByTestId('preview-batch').click()
    await expect(page.getByTestId('preview-box')).toBeVisible()
    const cfText = await page.getByTestId('preview-box').innerText()

    await page.getByTestId('strategy-select').selectOption('format_first')
    await page.getByTestId('preview-batch').click()
    const ffText = await page.getByTestId('preview-box').innerText()
    expect(cfText).not.toBe(ffText)
    // 两种边界下超阈值分片都只出现在拦截提示里，不进入 leaf 表格
    await expect(page.getByTestId('preview-box')).toContainText('超限拦截')

    // 整组写入批次包
    await page.getByTestId('build-batch').click()
    await expect(page.getByTestId('build-message')).toContainText('成功')
    const batchRows = page.locator('[data-testid^="batch-row-CIB-"]')
    await expect(batchRows.first()).toBeVisible()
    expect(await batchRows.count()).toBeGreaterThanOrEqual(6)

    // 构建记录逐台留痕
    await page.getByTestId('load-records').click()
    await expect(page.getByTestId('records-box')).toBeVisible()
    await expect(page.getByTestId('records-box').locator('.cb-pill.ok').first()).toBeVisible()

    // 打开 leaf 批次快照，核对通道/背压/分片来源/切断点
    const firstLeaf = page.locator('[data-testid^="batch-row-CIB-"]', { hasText: 'leaf' }).first()
    const batchNo = (await firstLeaf.getAttribute('data-testid'))!.replace('batch-row-', '')
    await page.getByTestId(`detail-${batchNo}`).click()
    const modal = page.getByTestId('detail-modal')
    await expect(modal).toBeVisible()
    await expect(modal).toContainText('背压切断点')
    await expect(modal).toContainText('通道快照')
    await expect(modal).toContainText('背压快照')
    await expect(modal.locator('table tbody tr').first()).toBeVisible()
  })

  test('格式解析页：读同一份批次包、按格式过滤、工作台审核与编排', async ({ page, request }) => {
    // 先通过接口构建一批数据，保证工作台有待审核项
    const build = await request.post('/api/ingestion/batches/build', {
      data: { 通道编号列表: ['INGE-0001', 'INGE-0002'], 合并策略: 'channel_first' },
    })
    expect(build.ok()).toBeTruthy()

    await page.goto('/parser')
    // 原有解析配置 CRUD 保持可用
    await expect(page.locator('tbody tr[data-testid^="row-"]')).toHaveCount(3)

    await page.getByTestId('tab-batches').click()
    const leafRows = page.locator('[data-testid^="pbatch-row-"]')
    await expect(leafRows.first()).toBeVisible()
    expect(await leafRows.count()).toBeGreaterThanOrEqual(4)

    // 按输入格式过滤，解析端据此选择字段映射
    await page.getByTestId('batch-format-filter').selectOption('JSON')
    const jsonRows = page.locator('[data-testid^="pbatch-row-"]')
    expect(await jsonRows.count()).toBeGreaterThanOrEqual(1)
    for (const row of await jsonRows.all()) {
      await expect(row).toContainText('JSON')
    }
    await page.getByTestId('batch-format-filter').selectOption('')

    // 看批次快照
    const firstBatch = leafRows.first()
    const batchNo = (await firstBatch.getAttribute('data-testid'))!.replace('pbatch-row-', '')
    await page.getByTestId(`pbatch-detail-${batchNo}`).click()
    await expect(page.getByTestId('parser-detail-modal')).toContainText('建议解析配置')
    // 工作台：通过后才能编排，驳回的批次不能编排
    await page.getByTestId('tab-review').click()
    const passButton = page.getByTestId(`review-pass-${batchNo}`)
    await passButton.click()
    await expect(page.getByTestId('work-message')).toContainText('已通过')

    const orch = page.getByTestId(`orchestrate-${batchNo}`)
    await orch.click()
    await expect(page.getByTestId('work-message')).toContainText('已编排')
  })

  test('硬约束：超阈值分片永不入完成批次，同版本重复提交被拦截', async ({ request }) => {
    // 登记超阈值分片
    const shard = await request.post('/api/ingestion/shards', {
      data: { values: { 通道编号: 'INGE-0003', 输入格式: 'XML', 分片大小: 9999 } },
    })
    const shardBody = await shard.json()
    expect(shardBody.ok).toBeTruthy()
    expect(shardBody.entry.分片状态).toBe('超限拦截')
    const blockedNo = shardBody.entry.分片编号

    const build = await request.post('/api/ingestion/batches/build', {
      data: { 通道编号列表: ['INGE-0003'], 合并策略: 'channel_first' },
    })
    const body = await build.json()
    expect(body.ok).toBeTruthy()
    expect(body.entry.results[0].超限拦截分片).toContain(blockedNo)

    // 所有 leaf 批次都不含该分片，且背压完整
    const batches = await (await request.get('/api/parser/channel-batches/list')).json()
    for (const b of batches.items) {
      expect(b.分片编号列表).not.toContain(blockedNo)
      expect(b.批次大小).toBeLessThanOrEqual(b.背压阈值)
      expect(b.批次状态).toBe('背压完整')
    }

    // 同一数据版本重复提交仅一次成功
    const version = body.entry.results[0].数据版本
    const dup = await request.post('/api/ingestion/batches/build', {
      data: { 通道编号列表: ['INGE-0003'], 合并策略: 'channel_first', 数据版本: { 'INGE-0003': version } },
    })
    const dupBody = await dup.json()
    expect(dupBody.ok).toBeFalsy()
    expect(dupBody.entry.results[0].ok).toBeFalsy()
  })
})
