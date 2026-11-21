# ETL编排与数据质量中心

面向数据仓库ETL作业场景，提供流水线编排、质量规则、依赖调度和回刷补偿的ETL治理平台

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/views/RunReportView.vue      综合运行报告（按运行编号复核回执）
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/routers/run_report.py 运行回执与综合报告接口
│   ├── app/engine/run_report.py  回执归一化与报告计算（纯计算）
│   ├── app/services/run_report.py 回执/报告持久化（SQLite）
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
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
| 流水线定义 | `pipeline_def` | ETL流水线 | 流水线名称、处理阶段、输入源表 |
| 任务节点 | `task_node` | 处理节点 | 节点名称、所属管线、节点类型 |
| 质量规则 | `quality_rule` | 校验规则 | 规则名称、目标表、校验字段 |
| 质量执行 | `validation_run` | 校验批次 | 批次标识、目标表、校验分区 |
| 依赖调度 | `depend_scheduler` | 调度配置 | 配置名称、目标管线、触发方式 |
| 滑动窗口 | `sliding_window` | 窗口配置 | 窗口名称、目标表、窗口字段 |
| 回刷补偿 | `rerun_compensation` | 重跑任务 | 任务名称、目标管线、重跑范围 |
| 水位追踪 | `watermark_tracker` | 水位记录 | 管线标识、分区字段、当前水位 |
| 资源池 | `resource_pool` | 计算资源 | 池名称、总核数、总内存 |
| 血缘图谱 | `lineage_graph` | 血缘边 | 源节点、目标节点、传递字段 |
| 异常捕获 | `exception_capture` | 异常记录 | 发生时间、所属管线、异常类型 |
| 模板仓库 | `template_store` | 处理模板 | 模板名称、处理类型、输入Schema |
| 分区扫描 | `partition_scan` | 扫描报告 | 报告时间、目标表、分区总数 |
| 配置迁移 | `config_migration` | 迁移任务 | 任务名称、源环境、目标环境 |
| Schema演进 | `schema_evolution` | 变更记录 | 变更时间、目标表、变更类型 |
| 回填追踪 | `backfill_tracker` | 回填进度 | 追踪ID、回填任务、当前分区 |
| 成本分析 | `cost_analysis` | 成本快照 | 快照日期、管线标识、计算时长 |
| 告警策略 | `alert_policy` | 告警规则 | 规则名称、监控对象、告警条件 |

## 综合运行报告

综合报告按“管线 + 运行编号”关联实际运行回执，不再使用固定样本和按当天时钟
推算的模拟进度。一次运行会先后收到两份回执：

| 回执 | `receipt_type` | 来源 |
| --- | --- | --- |
| 质量门禁回执 | `quality_gate` | 质量执行批次（可引用 `validation_run_id` 与库内记录逐项核对） |
| 分区进度回执 | `partition_progress` | 逐分区实际通过/失败行数与完成度 |

关键约束：

- 回执按 `(pipeline_id, run_id)` 落库；重复登记或重复投递不会多记一条运行。
  回执内容有修订时保留历史版本，报告只取最新一版。
- 所有证据必须属于同一轮：回执日期、分区日期、质量执行记录的校验分区都要与
  运行日期一致，否则返回 409 且不写库，杜绝拼接两个日期的数据。
- 门禁汇总与分区汇总的行数必须自洽；不一致时报告状态为 `inconsistent`，不允许发布。
- 只有 `state=complete` 的完整报告能发布为确认快照；确认后补入新回执只更新
  “当前视图”，已确认快照单独留存，可按 `report_id` 回看。
- 进程重启后库表仍在（SQLite，默认 `backend/var/run_reports.sqlite3`，
  可用环境变量 `RUN_REPORT_DB` 改路径）；启动时只会补齐缺失的演示批次。

接口：

```text
POST /api/runs                                     登记运行（幂等）
GET  /api/runs                                     运行清单 + 已收到/缺少的回执
POST /api/runs/{run_id}/receipts                   按运行编号提交实际回执
GET  /api/runs/{run_id}/report                     当前综合报告（核验运行编号归属）
POST /api/runs/{run_id}/report/confirm             发布完整报告
GET  /api/runs/{run_id}/report/confirmed           读取已确认报告（可带 report_id）
GET  /api/business/pipelines/{pipeline_id}/run-report?run_id=…   管线维度兼容入口
```

内置两批实际订单运行（`ORD-RUN-20261118` 已确认，`ORD-RUN-20261117` 只有门禁回执），
前端“综合运行报告”页面可以直接复核“上次完整结果 + 当前实际回执”。

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数仓与数据湖
