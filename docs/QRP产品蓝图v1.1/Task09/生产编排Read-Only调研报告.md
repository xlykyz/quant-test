> **归档说明（收入 Task09 时补充）**
>
> 本文以下正文原样保留 2026-09-09 运维 Agent 的 read-only 调研结果。文中涉及当时服务器工作区 branch / SHA / ahead-behind 的描述，是**调查时点事实**，不随之后远端 `develop/v1.1` 合并结果回写；用于保留证据链，不应被解释为当前 Git 状态。

# QRP 生产编排 Read-Only Inventory

- 调查日期：2026-09-09（周三）
- 目的：还原「现在实际上是谁、在什么时间、以什么入口运行哪些 QRP pipeline」，为 Task09 Contract Graph Audit 提供事实基线
- 范围：只调查事实，不修改生产环境，不设计 Task09，不迁移调度 authority
- 执行方式：全程 read-only。未执行 systemctl enable/disable/start/stop/restart，未改 timer/service/cron/Hermes job/env/数据库，未运行任何生产 pipeline，未改 repo，未 commit/push

---

## 1. Executive Summary

**当前 QRP 日常生产主要由谁调度？**

单一 authority：系统级 systemd `qrp-atlas-jobs.service`（`enabled; active (running) since 2026-08-30 22:53 CST`，uptime 1 周 2 天，主 PID 1237010）。它没有配 timer，而是常驻进程自扫（`--poll-interval-seconds 5 --scheduler-id production --max-workers 4`）。

**QRP Job Runtime 实际接管了多少？**

100%。29 个生产 job 全部 `enabled=true`，运行时库累计 7,115 条 run 记录（2026-08-02 → 2026-09-09）。`scheduler_cursor` 的 `production` 最后扫描于 9/9 12:01 CST，`job_service_lease` 心跳 9/9 12:02 CST——服务确实在活跃运行，不是「配置存在」。

**Hermes 还负责哪些？**

QRP 部分为零。14 个 Hermes job 中 13 个 `enabled=false`，唯一 active 的 `eadada323c3c`（每周系统健康检查，周一 06:00）与 QRP 无关。11 个 Hermes job 已被 live manifest 在各自 `description` 字段里明确引用为迁移来源，可逐条对回。

**当前 Daily Pipeline 的主要真实入口是什么？**

```
/home/claire/projects/qrp-atlas/.venv/bin/qrp-atlas-jobs \
  --env-file /home/claire/apps/qrp-atlas/qrp-atlas.env \
  serve --production-jobs /home/claire/apps/qrp-atlas/pipeline/production-job-definitions.json \
  --scheduler-id production --service-name qrp-atlas-jobs \
  --poll-interval-seconds 5 --max-workers 4
```

working directory `~/projects/qrp-atlas`；环境变量全部来自 `/home/claire/apps/qrp-atlas/qrp-atlas.env`（变量名：`QRP_API_CORS_ORIGINS / QRP_API_HOST / QRP_API_PORT / QRP_AUTH_MODE / QRP_AUTH_SESSION_TTL_SECONDS / QRP_DATA_DIR / QRP_DUCKDB_PATH / QRP_EPISODE_DB_PATH / QRP_HOME / QRP_IRM_QA_DUCKDB_PATH / QRP_JOB_RUNTIME_DB_PATH / QRP_JOB_RUNTIME_DIR / QRP_LOCAL_DISPLAY_NAME / QRP_LOCAL_USERNAME / QRP_LOG_LEVEL / QRP_POOL_DB_PATH / QRP_REMOTE_ACCESS_DB_PATH / QRP_RUNTIME_ENV / TUSHARE_TOKEN`）。运行时库位于 `/home/claire/data/qrp-atlas/runtime/job/job_runtime.sqlite3`。

**是否存在重复调度或 authority 重叠？**

没有活跃的重复调度，不存在双写风险。但存在三处 authority 分裂：

1. 生产权威 manifest 在 repo 之外（`/home/claire/apps/qrp-atlas/pipeline/`，最后修改 8/30 22:39），**未被 git 跟踪**；
2. repo 里另有一份同名文件，内容完全不同且 2 个 job 均 `enabled=false`；
3. 旧 Hermes job 定义 + `~/.hermes/scripts/` 下 legacy wrapper 仍在盘上。

**Task09 前最关键的生产编排缺口是什么？**

按严重度：

1. **`etf-daily-close` 25/25 全部 FAILED，零成功**（`ETF_DAILY_API_PARTIAL`）——生产从未跑通，并连锁 BLOCK 下游 `etf-adj-factor-close` 25 次，后者从未执行过一次。
2. **`stock-basic-morning` 今天 08:10 FAILED**（`STOCK_BASIC_API_FAILED`），累计 3/27 失败。
3. **repo 与生产已漂移**：live 29 jobs vs repo 2 jobs（全 disabled）；repo 当前在 `feature/v1.1-task08`，落后 `origin/develop/v1.1` 9 个提交。
4. **manifest 未纳入 git**：生产权威定义不可复现、无版本历史，无法固定审计基线。
5. **System B 未闭环**：authorization / score / strategy / target 在 repo 只有 `src/qrp_atlas/strategies/` 下的策略代码，**没有任何 Pipeline contract 注册**，因此不可能出现在每日契约图链上。

---

## 2. Production Job Inventory

统一项：Authority = `qrp-atlas-jobs.service`（系统级）｜时区 `Asia/Shanghai`｜Working Dir `~/projects/qrp-atlas`｜Entrypoint 为对应 pipeline 的 formal Contract（executor 由 contract 声明）｜Env `~/apps/qrp-atlas/qrp-atlas.env`。

「Last Run」取 2026-09-08（周一，最近一个交易日）；9/9 是周三交易日，早盘批（08:00–09:15）已跑，收盘批尚未到点。

### 2.1 基础资料（早盘）

| Job | Schedule | Last Run | 备注 |
|---|---|---|---|
| index-basic-morning | `0 8 * * 1-5` | ✅ 9/9 08:00（26/26 ok） | — |
| stock-basic-morning | `10 8 * * 1-5` | ❌ 9/9 08:10 `STOCK_BASIC_API_FAILED` | 累计 24 ok / 3 fail |
| suspend-d-ingest-morning | `15 9 * * 1-5` | ✅ 9/9 09:15（26/26 ok） | 另有 14 条无 `job_pipeline_id` 的 legacy 记录（8/3，已停用） |

### 2.2 行情收盘

| Job | Schedule | Last Run | 备注 |
|---|---|---|---|
| market-daily-close | `15 16 * * 1-5` | ✅ 9/8 16:15（28/29 ok） | 整条链的根依赖 |
| adj-factor-close | `25 16 * * 1-5` | ✅ 9/8 16:25（23/30 ok） | deps: market_daily_update |
| **etf-daily-close** | `30 16 * * 1-5` | ❌ **0/25，从未成功** | `ETF_DAILY_API_PARTIAL`；最新失败 9/8 16:30 |
| **etf-adj-factor-close** | `45 16 * * 1-5` | ⛔ **BLOCK 25/25，从未执行** | `dependency etf-daily-close latest status is FAILED` |
| limit-step-close | `40 16 * * 1-5` | ✅ 9/8 16:40（24/25 ok） | — |
| ths-daily-close | `50 16 * * 1-5` | ✅ 9/8 16:50（25/25 ok） | — |
| zt-dt-pool-close | `30 16 * * 1-5` | ✅ 9/8 16:30（28/30 ok） | 已从 Hermes 的 15:30 调整到 16:30 |
| stk-high-shock-close | `0 17 * * 1-5` | ✅ 9/8 17:00（25/25 ok） | — |
| daily-basic-close | `15 17 * * 1-5` | ✅ 9/8 17:15（28/29 ok） | deps: market_daily_update |
| index-daily-close | `0 20 * * 1-5` | ✅ 9/8 20:00（33/35 ok） | — |

### 2.3 基本面

| Job | Schedule | Last Run | 备注 |
|---|---|---|---|
| fundamentals-daily-announcement | `0 18 * * 1-5` | ✅ 9/8 18:00（22/25 ok） | params: `{"mode":"ann_date","tables":"all"}` |
| earnings-forecast-daily-announcement | `15 18 * * 1-5` | ✅ 9/8 18:15（25/25 ok） | params: `{"mode":"ann_date"}` |

### 2.4 System B

| Job | Schedule | Last Run | 备注 |
|---|---|---|---|
| system-b-readiness-daily | `30 18 * * 1-5` | ✅ 9/8 18:30（20 ok / 5 BLOCK） | deps: market_daily_update, adj_factor_daily, suspend_d_ingest |
| system-b-state-daily | `40 18 * * 1-5` | ✅ 9/8 18:40（20 ok / 5 BLOCK） | deps: system_b_state_readiness |
| system-b-episode-daily | `50 18 * * 1-5` | ✅ 9/8 18:56（20 ok / 5 BLOCK） | deps: system_b_state_daily |
| system-b-pool-height-daily | `0 19 * * 1-5` | ✅ 9/8 19:05（20 ok / 5 BLOCK） | deps: system_b_episode_rebuild |
| system-b-pool-capacity-daily | `10 19 * * 1-5` | ✅ 9/8 19:12（20 ok / 5 BLOCK） | deps: system_b_episode_rebuild |
| system-b-pool-recognition-daily | `20 19 * * 1-5` | ✅ 9/8 19:22（20 ok / 5 BLOCK） | deps: system_b_episode_rebuild |

6 个 System B job 的 5 次 BLOCK 全部集中在 8/24（10:30–11:20），近 7 天 BLOCKED 数为 0，链路目前健康。

### 2.5 研究数据

| Job | Schedule | Last Run | 备注 |
|---|---|---|---|
| cninfo-research-morning | `0 8 * * *` | ✅ 9/9 08:00（37 ok / 1 fail） | 对应 Hermes 83895a6a24a7 |
| cninfo-research-noon-evening | `0 12,21 * * *` | ✅ 9/9 12:00（69 ok / 6 fail） | 对应 Hermes e56ff366b299；6 次失败均为 `CNINFO_PROVIDER_TOTAL_MISMATCH`（8/25–9/1） |
| cninfo-research-afternoon | `15 15 * * *` | ✅ 9/8 15:15（36 ok / 1 fail） | 对应 Hermes 053db3ea5a9f |
| research-stock-report-morning | `0 7 * * *` | ✅ 9/8 07:00（38/38 ok） | params: `{"incremental":"true"}` |
| research-stock-report-evening | `0 19 * * *` | ✅ 9/8 19:02（34 ok / 3 fail） | 9/2 失败 `RESEARCH_REPORT_PDF_PROVIDER_FAILED` |
| research-industry-report-morning | `0 7 * * *` | ✅ 9/8 07:00（38/38 ok） | params: `{"incremental":"true"}` |
| research-industry-report-evening | `0 19 * * *` | ✅ 9/8 19:03（34 ok / 3 fail） | 9/2 失败 `RESEARCH_INDUSTRY_PDF_PROVIDER_FAILED` |

### 2.6 Canary

| Job | Schedule | Last Run | 备注 |
|---|---|---|---|
| irm-qa-incremental | `*/5 8-21 * * *` | ✅ 9/9 12:00（5830 ok / 406 fail / 6236 total） | 失败主因 `IRM_PROVIDER_PARTIAL_PAGE_OVERLAP`，集中在 9/8 早盘 |

### 2.7 全时统计

- 状态：SUCCESS 6,590 ｜ FAILED 469 ｜ BLOCKED 55 ｜ TIMED_OUT 1
- 触发方式：SCHEDULED 7,065 ｜ MANUAL 45 ｜ RETRY 5
- definition_version 混用：`1.0.0 / 1.1.0 / 1.1.1 / 1.2.0`
- run 记录时间跨度：2026-08-02 19:43 UTC → 2026-09-09 04:00 UTC

### 2.8 manifest 结构特征

- 29 个 job，schema 字段仅：`job_id / pipeline_id / enabled / schedule / timezone / parameters / name / description`
- **manifest 本身没有 `dependencies` 字段**——依赖关系隐式来自各 Contract 的声明，Job Runtime 在执行时解析
- 3 个 pipeline 被多个 job 复用（预期的多时间窗，非重复调度）：`cninfo_research_visit_ingest ×3`、`research_stock_report_ingest ×2`、`research_industry_report_ingest ×2`

---

## 3. System B Daily Chain（真实运行顺序）

```
market-daily-close              16:15   [market_daily_update]
        ↓
adj-factor-close                16:25   [adj_factor_daily]
        ↓                                  ↘
suspend-d-ingest-morning        09:15   [suspend_d_ingest]
        ↓                                  ↙
system-b-readiness-daily        18:30   [system_b_state_readiness]
        ↓                                  ← deps: market_daily_update, adj_factor_daily, suspend_d_ingest
system-b-state-daily            18:40   [system_b_state_daily]
        ↓                                  ← deps: system_b_state_readiness
system-b-episode-daily          18:50   [system_b_episode_rebuild]
        ↓                                  ← deps: system_b_state_daily
┌────────────────┬────────────────┬─────────────────────┐
pool-height      pool-capacity    pool-recognition
 19:00           19:10            19:20
└────────────────┴────────────────┴─────────────────────┘
                 ← 三者 deps 均为 system_b_episode_rebuild

【真实链到此为止】
```

### 不在链上的阶段（逐项标记）

| 阶段 | 标记 | 事实 |
|---|---|---|
| authorization | REPO_ONLY（且非 contract） | 仅有 `src/qrp_atlas/strategies/builtin/system_b_authorization.py`，`code="system_b_authorization"`，无任何 `PipelineContract` 注册。生产不可能在每日链上跑它 |
| score | REPO_ONLY | 无独立 score contract；相关逻辑在 `strategies/builtin/system_b_decision.py` |
| strategy | REPO_ONLY | 同 `system_b_decision.py` / `system_b_portfolio.py`，属策略层，非 pipeline 层 |
| target | REPO_ONLY | `system_b_portfolio.py` 内 `authorization` 字段；无独立 pipeline |
| pools（height / capacity / recognition） | **RUNNING** | 已调度且 9/8 全部成功 |
| episode | **RUNNING** | 已调度且 9/8 成功 |
| state / readiness | **RUNNING** | 已调度且 9/8 成功 |
| theme | REPO_ONLY | `theme_m4_production`、`theme_m5_production` 已注册 contract，但生产 manifest 未调度 |
| role | REPO_ONLY | 无独立 role contract；`industry_membership_ingest` / `index_component_ingest` 已注册未调度 |
| asset rank | REPO_ONLY | `system_b_asset_rank_daily` 已注册未调度 |
| theme rank | REPO_ONLY | `system_b_theme_rank_daily` 已注册未调度 |

### 一条在生产结构上不可满足的依赖边

`system_b_asset_rank_daily` 的声明依赖为：
`system_b_episode_rebuild, system_b_pool_height, system_b_pool_capacity, system_b_pool_recognition, dc_hot_ingest, ths_hot_ingest`

其中 `dc_hot_ingest` 与 `ths_hot_ingest` **在生产 manifest 中完全未调度**。这条边一旦启用就无法满足。同理 `theme_m5_production` ← `dc_hot_ingest + ths_hot_ingest`，`system_b_theme_rank_daily` ← `theme_m4_production + dc_hot_ingest + ths_hot_ingest`。

---

## 4. Repo × Production Gap

Repo 基准：`feature/v1.1-task08` @ `1317650c`（2026-09-09 08:47 +0800），工作区干净。
`origin/develop/v1.1` @ `441a04cb`（2026-09-09 08:11 +0800）。当前分支落后 develop/v1.1 **9 个提交**，领先 0。

### A. Repo-only（已注册 contract，生产未调度，10 个）

| Contract pipeline_id | 状态 |
|---|---|
| `dc_hot_ingest` | 已注册未调度；被 asset_rank / theme_m5 / theme_rank 依赖 |
| `ths_hot_ingest` | 已注册未调度；被 asset_rank / theme_m5 / theme_rank 依赖 |
| `theme_m4_production` | 已注册未调度；被 theme_rank 依赖 |
| `theme_m5_production` | 已注册未调度 |
| `market_m6_production` | 已注册未调度 |
| `industry_membership_ingest` | 已注册未调度 |
| `index_component_ingest` | 已注册未调度 |
| `pit_backfill` | 已注册未调度 |
| `system_b_asset_rank_daily` | 已注册未调度；依赖在生产不可满足（见上） |
| `system_b_theme_rank_daily` | 已注册未调度；依赖在生产不可满足 |

### B. Production-only

0 个。25 个在跑的 pipeline 全部有正式 Contract 注册。不存在「生产在跑但 repo 无定义」的情况。

### C. Legacy Hermes（已迁移，定义仍残留）

11 个 Hermes job 已映射进 live manifest（可在各 job `description` 中逐条对上），全部 `enabled=false`，输出目录最后活动 7/31–8/2：

| Hermes job | 名称 | 迁移目标 |
|---|---|---|
| b8c670f02f9b | qrp-atlas 日更数据 | market-daily-close + adj-factor-close（拆成两段） |
| 4afb74bd0769 | daily-basic-1715 | daily-basic-close |
| 3c40deda0c79 | zt_dt_pool_daily | zt-dt-pool-close（schedule 15:30 → 16:30） |
| 0450c10ccb5f | 指数日更-20点 | index-daily-close |
| 83895a6a24a7 | cninfo-main-update | cninfo-research-morning |
| e56ff366b299 | cninfo-incremental-noon | cninfo-research-noon-evening |
| 053db3ea5a9f | cninfo-incremental-afternoon | cninfo-research-afternoon |
| 2b3c60fc1bcc | research-stock-0700 | research-stock-report-morning |
| cd3ce52ff14a | research-stock-1900 | research-stock-report-evening |
| 3591486225dc | research-industry-0700 | research-industry-report-morning |
| 16a55246bddc | research-industry-1900 | research-industry-report-evening |

未映射 3 个：

| Hermes job | 名称 | 现状 |
|---|---|---|
| 56d22829661c | 每日管线执行总结 | disabled；原功能是汇总 Hermes cron 状态，authority 迁移后语义已失效 |
| 3fdbd779c4da | irm_qa 每五分钟增量更新 | disabled；能力由 `irm-qa-incremental`（runtime canary）承接 |
| eadada323c3c | 每周系统健康检查 | **仍 active**（周一 06:00），非 QRP，不属于本次迁移范围 |

### D. QRP Runtime

29/29 jobs ｜ 100% authority ｜ 25 distinct pipelines ｜ 7,115 run 记录 ｜ 服务 lease 心跳活跃。

### E. Duplicate / Conflict

无活跃重复调度，无双写风险。3 个 pipeline 被多 job 复用属预期多时间窗（见 2.8）。

### F. 已废弃但仍残留

`/home/claire/.hermes/scripts/`：
`pipeline_daily_run.sh`（调用 `python -m qrp_atlas.pipeline.daily_update.run`）、`daily_basic_run.sh`、`fetch_index_daily.sh`、`fetch_zt_dt_pool.sh`、`irm_qa_daily_update.sh`、`cninfo_cron.py`、`cninfo_cron_main.sh`、`cninfo_cron_incr.sh`、`cninfo_catchup.py`、`research_stock_0700.sh`、`research_stock_1900.sh`、`research_industry_0700.sh`、`research_industry_1900.sh`、`backfill_missing_dates.sh`

未被任何活跃调度器引用，但 Hermes job `b8c670f02f9b` 的 prompt 仍指向 `pipeline_daily_run.sh`。

另有 `/home/claire/projects/qrp-atlas-pipeline-refactor/scripts/run_pit_backfill_{finish,bg,offline_loop}.sh`，属无 remote 的残留工作区。

### G. repo 状态描述与生产现实不一致（最大项）

| 项 | repo（deploy/pipeline/production-job-definitions.json） | live（apps/qrp-atlas/pipeline/...） |
|---|---|---|
| job 数 | 2 | 29 |
| enabled | 2 个全 `false` | 29 个全 `true` |
| research schedule | `0 7 * * 1-5` / `0 19 * * 1-5` | `0 7 * * *` / `0 19 * * *` |
| research parameters | morning `{"incremental":"true"}` / evening `{}` | 两者均 `{"incremental":"true"}` |
| description | "Disabled offline example... Not part of any formal deployment selection" | 逐条标注对应 Hermes job id |
| git 跟踪 | 是 | **否** |
| 最后修改 | commit 8/3 04:28 | 文件 8/30 22:39 |

`live manifest` 与 `candidate manifest`（`.candidate.json`）逐字节一致。

### H. repo deploy/ 下的 unit 文件部署情况

| 文件 | 是否部署 |
|---|---|
| `deploy/qrp-atlas-api.service` | ✅ 已部署（enabled, running） |
| `deploy/qrp-atlas-jobs.service` | ⚠️ **不在 repo**，实际文件在 `/etc/systemd/system/`，未纳入 git |
| `deploy/qrp-atlas-system-b-daily.service` | ❌ 未部署（0 unit files） |
| `deploy/qrp-atlas-system-b-daily.timer` | ❌ 未部署 |
| `deploy/qrp-atlas-pipeline-scheduler.service.example` | ❌ 未部署 |
| `deploy/qrp-atlas-pipeline-scheduler.timer.example` | ❌ 未部署 |
| `deploy/qrp-atlas-pipeline-runner.service.example` | ❌ 未部署 |
| `deploy/qrp-atlas-pipeline-runner.timer.example` | ❌ 未部署 |
| `deploy/qrp-pit-backfill-20260714*` | ⚠️ 已部署为用户级 unit，状态 failed（一次性回填任务，历史遗留） |

---

## 5. 调度来源全量排查

| 来源 | QRP 相关结论 |
|---|---|
| systemd 系统级 service | `qrp-atlas-jobs.service` ✅ **authority**；`qrp-atlas-api.service` ✅ 运行中（市场数据 API，非调度器） |
| systemd 系统级 timer | 无 QRP 相关 |
| systemd 用户级 service | `hermes-gateway.service`、`qrp-atlas-web.service`（Caddy 前端）、`qrp-auth-postgres.service`（inactive dead）、`qrp-pit-backfill-20260714*`（failed，历史回填） |
| systemd 用户级 timer | 4 个，全为 OS 级（firmware-updater、launchpadlib-cache-clean、ubuntu-insights ×2），无 QRP |
| 用户 crontab | 仅 1 条 mihomo 配置转换，与 QRP 无关 |
| root crontab | **UNVERIFIED**（sudo 需交互认证，未提权）。已确认 `/etc/cron.d/` 仅 anacron + e2scrub_all，`/etc/cron.{daily,hourly,weekly,monthly}/` 仅系统标准项 |
| Hermes scheduler | 14 jobs，13 disabled；唯一 active 非 QRP |
| 其他 scheduler / wrapper | `~/.hermes/scripts/` 下 legacy shell（见 F），均未被活跃调度器引用 |

---

## 6. Evidence

| 结论 | 关键证据 |
|---|---|
| 单一 authority 且活跃 | `systemctl status qrp-atlas-jobs.service` → `enabled; active (running) since Sun 2026-08-30 22:53:16 CST`；`systemctl list-unit-files 'qrp-*' --type=service` → 仅 `qrp-atlas-api.service` + `qrp-atlas-jobs.service` enabled |
| manifest 29 jobs 全 enabled | `json.load` 解析 live manifest → enabled 29 / disabled 0 |
| 真实运行证据 | `/home/claire/data/qrp-atlas/runtime/job/job_runtime.sqlite3`，7,115 条 `job_run` |
| 服务真在跑 | `job_service_lease`: service `qrp-atlas-jobs`，heartbeat 2026-09-09T04:02:03Z，lease 到期 04:02:33Z；`scheduler_cursor`: `production` last_scanned 2026-09-09T04:01:00Z |
| ⚠️ 查询陷阱 | `qrp-atlas-jobs status` / `health` 在**不带** `--runtime-dir` 与 `--scheduler-id production` 时读的是 default 隔离库，返回 `status: STOPPED`、`scheduler_id: default`、pending 0——看起来像停了，实际 production 实例在跑。判断必须走 production store |
| SQLite 只读打开 | 原地 `sqlite3.connect('file:...?mode=ro')` 被拒（`unable to open database file`）；改法：先 `cp` 到 /tmp 再打开 |
| repo 对照 | `git diff deploy/pipeline/production-job-definitions.json <live>`；`git rev-list --left-right --count origin/develop/v1.1...HEAD` → `0  9` |
| contract 全集与依赖边 | `qrp-atlas-jobs --env-file <env> list-contracts` → 35 registered contracts，含 `dependencies` 数组 |
| Hermes 侧 | `hermes cron list`（1 active）、`~/.hermes/cron/jobs.json`（14 jobs，13 disabled）、`~/.hermes/cron/output/` 各 job 目录 mtime（7/31–8/2） |
| 失败明细 | `job_run` 表 `error_summary` 字段：`ETF_DAILY_API_PARTIAL`、`STOCK_BASIC_API_FAILED`、`IRM_PROVIDER_PARTIAL_PAGE_OVERLAP`、`CNINFO_PROVIDER_TOTAL_MISMATCH`、`RESEARCH_REPORT_PDF_PROVIDER_FAILED`、`RESEARCH_INDUSTRY_PDF_PROVIDER_FAILED` |
| UNVERIFIED | root 级 crontab 内容（无免密 sudo，未提权） |

---

## 7. Final Assessment

**Production orchestration inventory: COMPLETE**

唯一 UNVERIFIED 项为 root 级 crontab 内容（无免密 sudo，未提权修改系统），已用 `/etc/cron.d` 与 `/etc/cron.*` 文件清单佐证无 QRP 条目。

**Task09 Contract Graph Audit can start: NO**

尚缺 3 个事实，且都不是补数据能解决的：

1. **授权/决策链无 contract 形态。** authorization / score / strategy / target 只有 `src/qrp_atlas/strategies/` 下的策略代码，不存在 PipelineContract——契约图上没有这些节点可审。Task09 前需先决定：它们转成正式 Contract，还是作为 `system_b_asset_rank_daily` 的内部步骤。
2. **依赖图在生产结构上不可满足。** `system_b_asset_rank_daily` ← `dc_hot_ingest` + `ths_hot_ingest`，但两者未进 manifest；同理 `theme_m5_production`、`system_b_theme_rank_daily` ← `dc_hot/ths_hot`。这 10 个未调度 contract 是「等 Task09 决定」还是「已放弃」，需要明确定论。
3. **权威定义无版本化。** live manifest 未进 git，repo 自带版本与之冲突（29 enabled vs 2 disabled），且实际生效的 `qrp-atlas-jobs.service` unit 也不在 repo 内。Task09 若要在契约图上做审计，必须先确定哪一份是权威并纳入版本控制，否则审计基线无法固定。

另有一个非阻塞但会影响编排设计的现实约束：`etf-daily-close` 从未成功，`etf-adj-factor-close` 因此 25 次 BLOCK 从未执行。Task09 若要把 ETF 链纳入每日产品，需要先解掉这个上游，而不是只补 schedule。
