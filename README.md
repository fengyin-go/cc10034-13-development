# 实时数仓CDC变更捕获中心

面向实时数仓建设场景，提供Binlog解析、变更事件捕获、DDL追踪、SCD处理和下游分发同步的CDC治理平台

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
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
| Binlog解析 | `binlog_parser` | 解析配置 | 配置名称、源数据库、解析模式 |
| 变更事件 | `change_event` | 变更记录 | 事件ID、来源表、操作类型 |
| DDL追踪 | `ddl_tracker` | DDL记录 | 记录ID、变更数据库、变更表名 |
| Schema同步 | `schema_sync` | 同步任务 | 任务名称、源表、目标表 |
| 采集通道 | `cdc_channel` | 通道实例 | 通道名称、Binlog源、输出目标 |
| 检查点管理 | `checkpoint_manager` | 检查点 | 检查点ID、所属通道、Binlog文件 |
| 位点存储 | `position_store` | 位点记录 | 记录ID、数据库实例、Binlog名称 |
| 格式适配 | `format_adapter` | 适配规则 | 规则名称、输入格式、输出格式 |
| 缓慢变化维 | `scd_handler` | SCD配置 | 配置名称、维度表、SCD类型 |
| 去重窗口 | `dedup_window` | 窗口配置 | 配置名称、去重键、窗口大小 |
| 路由分发 | `route_dispatcher` | 路由规则 | 规则名称、匹配条件、目标通道 |
| 分发看板 | `dispatch_board` | 目标覆盖 | 源事件、目标通道、接收状态 |
| 回填位点 | `backfill_position` | 回填任务 | 任务名称、源位点、目标位点 |
| 延迟监控 | `lag_monitor` | 延迟指标 | 指标时间、所属通道、延迟秒数 |
| 快照比对 | `snapshot_compare` | 比对报告 | 报告名称、源表名、目标表名 |
| 事件过滤 | `event_filter` | 过滤规则 | 规则名称、过滤类型、匹配表达式 |
| 分区同步 | `partition_sync` | 同步配置 | 配置名称、源分区策略、目标分区策略 |
| 审计轨迹 | `audit_trail` | 审计日志 | 审计时间、操作类型、目标对象 |
| 故障切换 | `failover_group` | 切换组 | 组名、主通道、备用通道 |

## 分发看板

`/dispatch_board` 是路由分发的逐目标核对视图，数据由
`backend/app/services/dispatch_board.py` 聚合：

- **源事件集合**由变更事件服务提供（`ChangeEventService.replayable_event_set`），
  保留 Binlog 文件/位点/GTID 和变更前后镜像，可按坐标回放。
- **预期目标在初投时冻结**（`INITIAL_DISPATCH`）。之后 `route_dispatcher` 改配
  只影响后续事件，看板用 `config_drift` 把“当前配置”和“冻结预期”并排展示，
  已产生的收发证据不会被抹掉。
- **接收凭证按 `(源事件, 目标)` 去重**。初投确认、迟到确认、补发确认并发到达时
  只计一次完整接收，其余落回执流水并标记 `重复确认`。
- **回放范围由领取清单决定**。`POST /api/dispatch_board/claims` 领取仍缺投的
  `(事件, 目标)`；领取之后、重放之前到达的确认记为 `迟到确认`，把该目标移出本轮
  重放，但不计入实际补发次数。

相关接口（固定路径，注册在 `/api/{module}` 之前）：

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| GET | `/api/dispatch_board` | 整块看板：汇总、目标覆盖、缺投、领取单、回执、源事件 |
| GET | `/api/dispatch_board/target-coverage` | 路由分发视角的目标覆盖数据集 |
| GET | `/api/dispatch_board/source-events` | 可回放的源事件集合 |
| GET | `/api/dispatch_board/missing` | 当前仍缺投的明细 |
| POST | `/api/dispatch_board/receipts` | 登记接收凭证（幂等） |
| POST | `/api/dispatch_board/claims` | 领取缺投目标，形成本轮重放范围 |
| POST | `/api/dispatch_board/claims/{id}/replays` | 登记实际补发 |
| POST | `/api/dispatch_board/route-config` | 改配演练，验证冻结预期不被改写 |
| POST | `/api/dispatch_board/reset` | 恢复初投演示状态 |

演示数据里 `shop.inventory_adjustments` 的同一条 UPDATE（`evt-inv-20261119-0001`）
初投给 `warehouse-east` 与 `warehouse-west`；西侧回执默认缺失，用来演示
“领取后、重放前到达的迟到确认”。

## 测试

```bash
cd backend
.venv/bin/python -m unittest discover -s tests -v
```

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数仓与数据湖
