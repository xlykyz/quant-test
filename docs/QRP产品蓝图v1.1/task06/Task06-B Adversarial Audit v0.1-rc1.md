# Task06-B 对抗式设计审计 v0.1-rc1

结论：**FAIL**

代码基线：`8331c2e5520f916ddd92dcf341727ff97eb67f5e`，审计日期：2026-09-07。
范围：用户提供的 `Task06-B_System_B_Theme_Trend_Rank_Design_v0.1-rc1.md`；按配套 `Task06-B_Local_Adversarial_Audit_Prompt.md` 执行。
以下源码路径均相对于仓库根目录，行号对应上述基线。复现使用真实上游生产函数、内存 DuckDB 和临时数据库；Task06-B 尚未实现，关于其行为的推论均指按 rc1 规则实现后的结果。

## BLOCKER

### B1. Episode 的当前开闭状态不能直接作为 D 日资格；正常结束日也被误列为合同异常

**Evidence（事实）**

- 设计 §3.2 要求 D 日指向 Episode，且 `episode_end_date IS NULL`；三项不一致则合同失败。§11.4 又要求历史 replay 稳定。
- `src/qrp_atlas/contracts/schema.py:1415` 的 `THEME_CUSTOM_INDEX_STATE` 没有 `custom_index_episode_id`；正式关联字段在 `THEME_M4_OBSERVATION`，见同文件 `:1472`。不能按设计字面从 state 表取这个字段。
- `src/qrp_atlas/pipeline/theme/service.py:1019` 的 `run_m4_daily()` 先把已有 Episode ID 赋给当日 observation；`:1052` 在结束日原地更新 Episode 的 `episode_end_date/episode_return`；`:1126` 仍把这个 ID 写入结束日 M4。历史 M4 不随之回写。
- Episode 表主键只有 `episode_id`，没有按 D 留存的开闭版本，见 `deploy/duckdb/003_stock_collections_and_m4.sql:106`。

**Failure case（实测及推论）**

`lifecycle_case()` 实际逐日生产：Episode 于 2026-08-10 开始、08-11 确认；08-14 尚 open；08-18 正常结束。

1. 08-18 生产后，08-14 的 M4 行逐字段未变，但用 `end_date IS NULL` 回放 08-14，可排名数量由 1 变为 0。若按“三项不一致”处理则整日失败；若按不适格处理则历史 universe 改变。两者均不符合设计。
2. 08-18 当日 M4 合法地保留 Episode ID，同时 `end_date=08-18`。这不是坏数据，却会触发 rc1 的不一致规则，阻止正常的“不适格”snapshot 产出。

**Required change（建议）**

- 明确用 finalized `M4(D).custom_index_episode_id` 关联 Episode，并校验 theme/collection/唯一性。
- 将资格定义为 D 视角：`confirmed_date <= D AND (end_date IS NULL OR end_date > D)`。未来结束信息在 D 的计算与证据中投影为未知，不使用结束收益；D-aligned return 继续按 §5.1 从起点和 D 的 index 派生。
- `end_date=D` 是正常的 `NO_OPEN_EPISODE`，不能因指针非空而判合同失败。丢失对应 Episode、身份不匹配、日期不可能等才属于合同异常。
- 增加“先产 D、再结束 Episode、回放 D”及“结束日仍带 Episode ID”的验收用例；不修改上游 schema 或历史事实。

## MAJOR

### M1. M5 成功硬依赖会阻断设计要求的 UNAVAILABLE 物化路径

**Evidence（事实）**

- 设计 §11.1 顺序为 `M4 → M5 → availability → Theme Rank`，同时要求来源 `UNAVAILABLE` 时仍能写 `INCOMPLETE_INPUT`。
- availability 已由热榜采集写入，与该来源快照在同一事务提交：`src/qrp_atlas/pipeline/popularity_support.py:767` 的 `replace_popularity_batch()`。它不是 M5 完成后才生成的独立步骤。
- `src/qrp_atlas/pipeline/theme/m5_service.py:140` 的完整性检查拒绝空来源；`:240` 的 `read_m5_popularity_inputs()` 无条件要求两个来源。正式 M5 freshness 也执行该检查，见 `src/qrp_atlas/pipeline/theme_m5_contracts.py:253`。
- `src/qrp_atlas/orchestration/scheduler.py:182` 的依赖门禁要求上游 `SUCCESS`；`src/qrp_atlas/pipeline/contract_validation.py:356` 要求 `UPSTREAM_PIPELINE` 输入同时声明依赖。

**Failure case（实测及推论）**

`popularity_cases()` 用正式持久化函数写入东财完整快照及 THS 的 `(UNAVAILABLE, 0)`。正式 M5 Contract 返回 `FAILED / THEME_M5_THS_HOT_INPUT_INCOMPLETE`。

将该结果交给真实 scheduler 的依赖门禁，按设计顺序构造的 M5 硬依赖返回 `BLOCKED`。因此，即使纯计算层实现了 §8.2，生产执行仍到不了它。复现中的 Task06-B JobDefinition 和 store 读取是明确的测试替身，不代表已有 Task06-B 调度配置。

**Required change（建议）**

- 把 availability 明确归入两个热榜采集的输出；先检查 availability，再决定是否要求当日 M5。
- 为 `UNAVAILABLE` 路径移除对 M5 成功的无条件依赖。可使用现有 `TABLE` 输入和条件 freshness checker；来源全 `AVAILABLE` 时仍必须证明当日 M5 已完成且版本一致，否则失败并按生产策略重试。
- 冻结正常路径的生产顺序与不可用路径的门禁，并做完整 Contract/scheduler 验收。无需放宽 M5、修改上游 schema 或扩建通用调度框架。

### M2. 缺少 M5 与所消费热榜/availability 属于同一输入版本的校验

**Evidence（事实）**

- 设计 §5.7 将 M5 的 appearance 分子与 availability 的 snapshot 分母组合；§8.3 检查数量、比例和快照元数据，§12 要求记录两侧 provenance，但没有规定跨两侧的版本一致性断言。
- `src/qrp_atlas/pipeline/theme/m5_service.py:273` 的 `compute_m5_input_snapshot_id()` 对 themes、memberships、dc_hot、ths_hot 一起生成指纹；`:389` 计算该指纹，`:422` 将它存入 M5。
- `src/qrp_atlas/pipeline/popularity_support.py:742` 可按目标日重新替换热榜和 availability；该事务不重算 M5。每个来源的 `input_version` 见 `:790`，与 M5 的聚合输入指纹不是可直接比较的同一种 ID。

**Failure case（实测及推论）**

两个 Theme A/B 各有一个成员。东财、THS 各一个合法 Top100 快照，序号均为 `[1]`。

| 阶段 | A appearance rate | B appearance rate |
|---|---:|---:|
| V1：只有 A 成员上榜，完成 M5 | 0.5 | 0 |
| V2：东财同日重采，改为只有 B 成员上榜 | 0 | 0.5 |
| V2 availability 配旧 M5 | 0.5 | 0 |

两侧都 `AVAILABLE`，snapshot 数/序号仍为 `1/[1]`，旧 M5 自身 count/ratio 算术一致，所有比例在 `[0,1]`，当前热榜与当前 availability 也一致。这些检查不能发现旧 M5。

`popularity_cases()` 实测只读重算后的 M5 输入指纹与已存指纹不同。若其余五个 leaf 并列，两个 popularity leaf 都把 A/B 的最终顺序排反。仅记录旧 M5 ID 和新 availability 元数据仍会留下错误结果。

**Required change（建议）**

- 在同一受控输入快照中验证：M5 的 `input_snapshot_id` 对应本次实际消费的 Theme/Membership、热榜行及其 availability。
- 可复用现有 `compute_m5_input_snapshot_id()` / `calculate_m5_facts()` 做只读指纹验证；不一致时 fail-fast，保留旧的两张 Rank 结果表，待上游 M5 对齐后重跑。不可把不同种类的 run ID 或时间戳比较当作指纹校验。
- 增加“同日重采改变 Top100 成员、数量及序号不变、M5 尚未更新”的验收用例。该要求可利用已有字段实现，不要求给 M4/M5 加列。

## MINOR

### N1. 将“有效状态日”明确为字段判据，区分合法 NULL 与缺行

**Evidence（事实）**

设计 §5.4 已禁止缺行后缩分母；但正式 `is_above_or_equal_ma5` 可空（`src/qrp_atlas/contracts/schema.py:1427`）。`run_m4_daily()` 在无有效价格或 MA5 窗口不完整时写 NULL（`src/qrp_atlas/pipeline/theme/service.py:950`），已有 Episode 可以继续 open。

**Failure case（实测）**

`null_state_case()` 产生完整的五个正式状态日：`[true,true,NULL,NULL,NULL]`。最后一天 effective members 已恢复为 1，Episode 仍 open，符合 U_D；按可观测布尔值计数为 `2/2=1`，若误用完整行数则为 `2/5=0.4`。这里没有缺失状态行，不能用“缺行”规则代替 NULL 口径。

**Required change（建议）**

把“有效”明确为 `is_above_or_equal_ma5 IS NOT NULL`：先按交易日历校验应有行完整，再在已存在的合法状态中计数；不要把 NULL 转 false。metadata 记录 expected/valid/NULL 日数。这个补充落实原有“有效状态日”语义，不要求更改上游状态或 U_D。

### N2. 补齐退化状态的优先级和空维度输出

**Evidence（事实）**

设计 §6.4 的 `N=1` 给 final rank 1，§8.2 的 unavailable 给 final rank NULL；§7.3 未规定整个维度没有 active leaf 时的值。现有 `rank_component()` 只处理组件状态（`src/qrp_atlas/indicators/system_b/asset_ranking.py:211`），不会替 Task06-B 决定这些组合边界。

**Failure case（最小规则组合）**

- 唯一 eligible Theme 遇到 trusted popularity unavailable，同时命中两条 final rank 规则。
- 两个 Theme 的 Duration 和 Above-MA5 均相同，其他维度有变化：Persistence 的 active 权重和为 0，直接套展示分公式会出现 `0/0`。

**Required change（建议）**

明确 eligible row 的优先级为 `INCOMPLETE_INPUT > INSUFFICIENT_UNIVERSE > NO_VARIATION > OK`；合同异常始终先于物化。空维度分数为 NULL、对全局贡献为 0，不再次按固定维度预算重权。加入上述两个边界用例。

## 已验证无问题的关键点

- **C_D 的生产冻结能力已有实现。** `ThemePipelineService._fetch_all_canonical_themes()` 在已完成目标日读取 finalized index/M4 集合（`src/qrp_atlas/pipeline/theme/service.py:225`）；§3.5 的复用要求能避免另写 identity SQL。既有相关回归已通过。
- **open return 的 stale 风险已由设计明确防护，不重复报错。** 实测正式 open Episode 存值为 `0`，D-aligned 值为 `118/105-1=12.380952%`；§5.1 已要求重新派生，现有 index 起点及 D 日事实可支持。修正 B1 后不需要新增上游收益列。
- **缺历史状态行的防护已写明。** §5.4/§8.3 要求失败而非缩分母；实现必须按交易日历检查完整性。N1 仅补充合法 NULL 的明确判据。
- **Hot Appearance Rate 的同版本算术成立。** `calculate_m5_raw_observations()` 以 distinct PIT 成员为 member 分母、以映射行数为 appearance（`src/qrp_atlas/indicators/m5/observations.py:298`、`:332`、`:351`）；Top100 distinct-ticker 校验在 `src/qrp_atlas/pipeline/theme/m5_service.py:183`。实际构造 20 成员、3+2 snapshots、25 appearances，结果为 `0.25`。M2 是版本配对问题，不是否定这个分母公式。
- **M4/M5 的成员边界成立。** M4 limit-up count 仅计算 effective members（`src/qrp_atlas/indicators/m4/observations.py:105`）；M5 使用 PIT Membership，不应替换为 effective member 分母。
- **固定 U_D 的 helper 风险已被 §6.1 明确覆盖。** `rank_component([1,None,2])` 实测 N 为 2；Task06-B 在调用前验证全 U_D 有限值即可，不需要修改 Task06-A helper。
- **全局重权、维度展示与 exact tie key 数学一致。** 实测 160 组不同 N/业务并列输入，精确有理数计算均满足 `theme_raw_score = 100*K / (2*(N-1)*sum(active base weights))`。维度以各自实际 active 预算还原全局 composite 完全一致。`35` 对 `15+20` 的反向排名构造得到真并列、final average rank `1.5`。
- **同库两表原子替换可以沿用现有机制。** 实际调用 `src/qrp_atlas/pipeline/system_b_asset_rank/service.py:511` 的 `_persist()`，在 snapshot INSERT 后令 audit 主键冲突，确认两表旧行全部保留。当前 migration 最大编号为 `008`，候选 `009` 未占用。
- **没有找到需要扩大范围的重复计权反例。** 七个 leaf 的非负加权保持原始指标上的 Pareto 单调性；相关性本身不能证明错误排序。本次不据此否决权重，也不要求加入 Damage/M6/授权或新增上游 schema。

验证结果：相关 8 个测试文件共 **82 passed in 21.05s**；复现脚本输出 **AUDIT REPRODUCTIONS PASSED**，包含上述生命周期、NULL 状态、M5 unavailable、同日版本错配、160 组数学验证、25/100 分母样例和真实 `_persist()` 回滚验证。

```powershell
python -m pytest tests/indicators/test_theme_trend_and_episode.py tests/indicators/test_m4_observations.py tests/indicators/test_m5_observations.py tests/indicators/test_asset_ranking.py tests/pipeline/theme/test_m4_semantic_finalization.py tests/pipeline/theme/test_m5_production.py tests/pipeline/test_popularity_contracts.py tests/pipeline/system_b_asset_rank/test_production.py -q
python -X utf8 "docs/QRP产品蓝图v1.1/task06/audit_task06_b_rc1.py"
```

本次仅新增审计报告和复现脚本，未修改业务实现。未运行全仓测试或 Linux 真实数据验收：本次是设计审计，测试范围集中于引用的上游行为和数学反例；不据此声称 Task06-B 已实现或通过生产验收。

材料 SHA-256：

```text
Design: 549CB8240DEA9DEDC094A4281E0FB71AFFAA057FF3C741C83A1CEFCC6AC13849
Prompt: 99DF84EBE104015AA7B32DFAADD73322334471CDD50FD18B83325D53E525D127
```

## 最终建议

- 是否可进入实现：**NO**
- 必须先修：**B1、M1、M2**。
