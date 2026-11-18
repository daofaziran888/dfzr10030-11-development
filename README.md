# 多源数据采集与RPA自动化平台

面向网页抓取、API接入、增量采集、RPA流程编排、数据校验与采集监控的数据工程平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/            每个业务模块一个页面
│   ├── src/api/              统一请求封装
│   ├── src/stores/           会话与筛选状态
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/          每个业务模块一组接口
│   ├── app/services/         业务规则与状态流转
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 数据源配置 | `datasource` | 数据源 | 源端编号、连接类型、接入地址 |
| 网页采集规则 | `crawler` | 采集规则 | 规则编号、目标网址、页面选择器 |
| API接入管理 | `apiclient` | API客户端 | 客户端编号、接口名称、请求方法 |
| 增量采集 | `incremental` | 增量游标 | 游标编号、来源标识、增量字段 |
| RPA任务编排 | `robot_task` | RPA任务 | 任务编号、任务名称、页面定位器 |
| 数据接入通道 | `ingestion` | 接入通道 | 通道编号、通道名称、输入格式 |
| 格式解析器 | `parser` | 解析配置 | 解析编号、输入格式、输出格式 || 采集结果校验 | `validator` | 校验规则 | 规则编号、校验字段、校验类型 |
| 采集去重 | `dedup` | 去重策略 | 策略编号、输入来源、去重键 |
| 采集调度 | `schedule` | 调度计划 | 计划编号、关联任务、调度周期 |
| 采集监控 | `monitor` | 监控面板 | 面板编号、采集源总数、今日采集量 |
| 采集日志 | `log_viewer` | 日志条目 | 日志编号、关联任务、日志级别 |
| 凭证管理 | `auth` | 认证凭证 | 凭证编号、凭证类型、关联数据源 |
| 采集转换管道 | `transform` | 转换节点 | 节点编号、输入来源、输出目标 |
| 数据流转编排 | `flow` | 流转管道 | 管道编号、管道名称、起始节点 |
| 数据血缘追踪 | `lineage` | 血缘关系 | 关系编号、上游节点、下游节点 |
| 采集报告 | `report` | 采集报告 | 报告编号、采集源、统计周期 |

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.vue`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 通道接入批次包（多输入通道批量接入）

解决“三种格式同时进入不同通道、页面只能看到通道状态、无法核对每批数据由哪里组成”的问题。
输入分片必须绑定通道与输入格式，按输入格式和背压阈值合并成可核对的**通道接入批次包**，
每个 leaf 批次都带通道快照、背压快照、分片来源和切断点说明，可追溯到通道、格式与背压状态。

### 服务边界

- 领域规则集中在 `backend/app/services/channel_batch.py`，不挂路由；
- 接入通道服务（`ingestion`）是写入方：登记分片、预演/构建批次包，同一动作同步更新缓冲队列；
- 格式解析服务（`parser`）只读同一份 `ingestion_batch`，回写待审核工作台项并编排后续处理；
- 内部表 `ingestion_shard` / `ingestion_batch` / `review_workbench` / `batch_build_record`
  不进运营概览，两个业务页面读同一份批次数据，不存在第二份副本。

### 两条硬底线（两种合并策略都必须守住）

1. 解析端只能读取“背压完整”的批次：单分片超过通道背压阈值即置「超限拦截」，永不进入完成批次；
2. 每个 leaf 批次通道可追溯、输入格式单一，避免解析端收到无法解释的混合数据。

### 两种合并边界（页面可预演对比，批次包结果不同）

| 策略 | 含义 | 前置决定因素 |
| --- | --- | --- |
| `channel_first` | 按通道合并分片，在阈值处切开批次 | 背压边界前置决定切断位置 |
| `format_first` | 按输入格式分别成批，再汇总通道批次 | 输入格式前置决定子批次结构 |

其余规则：多选通道整组写入、成功/失败逐台记录；同一数据版本同时提交仅一次成功，重复提交拦住；
背压控制直接且批次数量较少，阈值变更按适用范围重算，历史批次与历史分片不可变。

### 接口一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/ingestion/shards/list` | 缓冲队列分片（可按通道过滤） |
| POST | `/api/ingestion/shards` | 登记输入分片（绑定通道与格式，超阈值即拦截） |
| POST | `/api/ingestion/batches/preview` | 切批预演，不落库 |
| POST | `/api/ingestion/batches/build` | 整组写入批次包，逐台记录 |
| GET | `/api/ingestion/batches/list` | 批次包列表（可按层级/通道过滤） |
| GET | `/api/ingestion/batches/{批次编号}/detail` | 批次快照、分片来源与背压切断点 |
| GET | `/api/ingestion/batches/build-records` | 构建记录 |
| GET | `/api/parser/channel-batches/list` | 解析侧读同一份 leaf 批次（可按格式过滤） |
| GET | `/api/parser/channel-batches/{批次编号}/detail` | 解析侧批次快照与建议解析配置 |
| GET | `/api/parser/review-workbench/list` | 待审核工作台项 |
| POST | `/api/parser/channel-batches/{批次编号}/review` | 审核（通过/驳回） |
| POST | `/api/parser/channel-batches/{批次编号}/orchestrate` | 审核通过后编排接入通道后续处理 |

领域规则验证：`cd backend && .venv/bin/python -I tests/test_channel_batch.py`；
端到端验收脚本：`frontend/tests/channel-batch.spec.ts`。

领域：数据工程
子方向：数据采集、爬虫与RPA
