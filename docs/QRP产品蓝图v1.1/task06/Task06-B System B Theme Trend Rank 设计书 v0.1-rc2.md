# Task06-B System B Theme Trend Rank 设计书 v0.1-rc2

> 状态：Adversarial Re-audit Candidate（针对 rc1 审计修订）  
> 日期：2026-09-07  
> 代码基线：`develop/v1.1@8331c2e5520f916ddd92dcf341727ff97eb67f5e`  
> 语义所有者：`SYSTEM_B`  
> 交付模式：`BUILTIN`

---

## 0. 文档目的

Task06-B 不建设新的 Theme 状态模型。Task04 已通过 Theme / Membership、Theme Equal-weight Index、Theme Trend State、Theme Episode、M4、M5 持续产出 Theme 状态事实。

Task06-B 的职责只有一个：

> **消费已经确定的 Theme 状态事实，把处于有效趋势中的 Theme 映射为同一交易日可比较的横截面相对强弱，并输出可审计、可回放、可版本化的 Theme Trend Rank。**

v0.1 目标是先建立一套稳定、解释性强、真正消费 M4/M5 成果的 Relative Rank baseline；Damage / Breakdown 后续再按真实案例独立演进。

---

# 1. 核心产品定义

Theme Rank v0.1 评价：

> **当前仍处于有效 Episode 中的 Theme，在已经建立的趋势强度、趋势持续性、当前板块结构与市场关注支持上的横截面相对强弱。**

它不是：

- 单日涨幅榜；
- Theme 状态机；
- 主线/非主线布尔判定；
- 未来收益预测；
- 开仓授权；
- 账户/仓位决策；
- ENTER/HOLD/EXIT 决策。

### 1.1 冻结的业务偏好

1. 趋势强度以一段时间内已经建立的趋势为核心，不以 D 日单日涨幅为核心。
2. 持续性越强，Theme 应获得更高的相对评价。
3. Theme 不因单纯时间流逝或高位横盘自动衰减。
4. v0.1 不设计趋势年龄衰减。
5. 批量跌停、核心高 M1/M2/M3 个股恶化等属于后续 Damage 能力，v0.1 不提前虚构。
6. Task06-B 只消费事实，不反向修改 M4/M5 业务语义。

---

# 2. 上下游与数据库边界

## 2.1 上游链路

```text
Theme / Membership
        ↓
PIT Resolver
        ↓
Theme Effective Members
        ↓
Theme Equal-weight Index
        ↓
Theme Trend State
        ↓
Theme Episode
        ↓
M4 Raw Observations
        ↓
M5 Raw Observations
        ↓
Task06-B Theme Relative Evaluation
```

## 2.2 不修改上游事实表

v0.1 不修改：

- `theme_effective_member_daily`
- `theme_custom_index_daily`
- `theme_custom_index_state`
- `theme_custom_index_episode`
- `theme_m4_observation`
- `theme_m5_observation`

Task06-B 派生出的 Episode Duration、Episode Above-MA5 Ratio、Limit-up Diffusion、Hot Appearance Rate、component rank/score、final Theme Rank 均属于评价层，不回写 M4/M5。

## 2.3 数据库边界

新增结果表继续注册到现有主 DuckDB，与 M4/M5/Theme 状态事实同库但逻辑分层；不新建独立 Theme Rank DB。

---

# 3. Theme State Universe 与 Rank Universe

## 3.1 C_D：Theme State Universe

```text
C_D = D 日合法存在的全部 canonical Theme
```

C_D 是 Theme 状态事实全集。是否参与 Rank 不影响 Membership、Effective Member、Theme Index、Trend State、Episode、M4、M5 的正常产出。

## 3.2 U_D：Theme Rank Universe

```text
U_D = {
  theme ∈ C_D
  |
  M4(D).effective_member_count > 0
  AND D 日存在已经确认、且在 D 视角尚未结束的 Theme Episode
}
```

### D 日 Episode 指针

必须以 finalized `theme_m4_observation(D).custom_index_episode_id` 作为 D 日 Episode 指针，不得假设 `theme_custom_index_state` 存在该字段。

非空 Episode 指针必须：

1. 唯一匹配 `theme_custom_index_episode`；
2. Episode 的 `theme_id / collection_id` 与 D 日 M4 / C_D 身份一致；
3. 满足：

```text
episode_confirmed_date <= D
AND (episode_end_date IS NULL OR episode_end_date > D)
```

只有满足上述条件才是 `OPEN_AT_D`。

### 历史回放投影

Episode 后续结束后会补全 `episode_end_date`。历史 D 回放禁止使用：

```text
episode_end_date IS NULL
```

作为历史资格条件。

历史 D 只能使用：

```text
OPEN_AT_D = confirmed_date <= D < end_date
```

后续已知 `end_date > D` 只用于证明 Episode 在 D 尚未结束，不得把未来 end 信息或最终 episode_return 暴露为 D 日评分证据。

### 结束日

若：

```text
M4(D).custom_index_episode_id != NULL
AND episode_end_date = D
```

这是合法结束日形态：

```text
rank_eligible = false
rank_eligibility_reason = NO_OPEN_EPISODE
```

不是合同异常。

真正的合同异常包括：指针找不到 Episode、重复 Episode、Theme/collection 身份不匹配、`confirmed_date > D` 却作为 D 日已确认 Episode、日期关系不可能等。

## 3.3 U_D 的作用边界

U_D 只决定谁参与 D 日横截面 Rank。

```text
Theme ∈ C_D but Theme ∉ U_D
```

意味着：

```text
上游状态数据：正常存在/更新
rank_eligible：false
theme_rank：NULL
theme_score：NULL
```

不意味着 Theme 没有状态数据。

## 3.4 Eligibility reason

首版：

- `ELIGIBLE`
- `NO_EFFECTIVE_MEMBERS`
- `NO_OPEN_EPISODE`

同时命中时：

```text
NO_EFFECTIVE_MEMBERS > NO_OPEN_EPISODE
```

不属于 C_D 的 Theme 不物化当日 Rank snapshot 行。

## 3.5 C_D 来源

Task06-B 不复制一套 Theme identity SQL。生产模式必须消费 D 日已经 finalized 的上游 Theme/M4 结果，优先复用现有 production universe / resolver / finalized M4 Theme 集合。

---

# 4. v0.1 评分结构

| 一级维度 | Leaf Component | Base Weight |
|---|---|---:|
| Trend Strength | Episode Return | 35% |
| Trend Strength | Theme Daily Return | 10% |
| Trend Persistence | Episode Duration | 15% |
| Trend Persistence | Episode Above-MA5 Ratio | 20% |
| Current Structure | Limit-up Diffusion | 10% |
| Popularity Support | Hot Stock Ratio | 6% |
| Popularity Support | Hot Appearance Rate | 4% |
| **Total** |  | **100%** |

一级预算：

```text
Trend Strength      45%
Trend Persistence   35%
Current Structure   10%
Popularity Support  10%
```

80% 权重用于回答“这一轮趋势已经建立得多强、持续得多好”。

---

# 5. Leaf Raw 定义

所有 leaf 在 U_D 内均为 `HIGHER_IS_BETTER`。

## 5.1 Episode Return — 35%

```text
episode_return_raw
= index_level(D) / index_level(episode_start_date) - 1
```

要求：

- Episode 身份来自 finalized `M4(D).custom_index_episode_id`；
- 先通过 `OPEN_AT_D`；
- 不得直接消费 Episode 表后来更新的最终 `episode_return` 作为历史 D raw；
- 必须由 `theme_custom_index_daily` 的起点与 D 日 `index_level` 派生；
- 不得读取 D 日之后的结束收益或结束证据。

## 5.2 Theme Daily Return — 10%

```text
theme_daily_return_raw = M4.theme_daily_return
```

只表达 D 日即时强化，不可支配中期趋势语义。

## 5.3 Episode Duration — 15%

```text
episode_duration_raw
= 从 episode_confirmed_date 到 D（含首尾）的实际 open trading days
```

要求：

- confirmed day = 1；
- 使用正式 `trading_calendar`；
- `confirmed_date > D` 为合同错误；
- 起点为 confirmed_date，不是 start_date；
- 不做年龄衰减或上限截断。

## 5.4 Episode Above-MA5 Ratio — 20%

```text
episode_above_ma5_ratio_raw
=
Episode 从 start_date 到 D 的有效状态日中
is_above_or_equal_ma5 = true 的日数
/
非 NULL 有效状态日数
```

要求：

1. 只能消费 D 及以前的正式 state；
2. 先以 `trading_calendar` 构造 `start_date..D` 的 expected open days；
3. 每个 expected day 必须恰有一条 Theme state；缺行 fail-fast，不得缩分母；
4. 行存在但 `is_above_or_equal_ma5 IS NULL` 是合法上游状态，不得转 false；
5. `valid_days = non-null days`；
6. metadata 记录 `expected_days / valid_days / null_days / true_days / false_days`；
7. confirmed Episode 若 `valid_days = 0`，合同失败；
8. raw 必须位于 `[0,1]`。

该比例用于表达“高位横盘/短暂跌破 MA5 不应一天把历史持续性归零”。

## 5.5 Limit-up Diffusion — 10%

```text
limit_up_diffusion_raw
= theme_limit_up_count / effective_member_count
```

U_D 已保证分母 > 0。不得用裸 `theme_limit_up_count` 排名。

## 5.6 Hot Stock Ratio — 6%

```text
hot_stock_ratio_raw = M5.theme_hot_stock_ratio
```

必须保留 M5 的 PIT Theme Membership 分母，不得换成 M4 Effective Members。

## 5.7 Hot Appearance Rate — 4%

```text
valid_snapshot_count_total
= Σ required popularity sources.valid_snapshot_count

hot_appearance_rate_raw
= theme_hot_list_appearance_count
  / (theme_member_count × valid_snapshot_count_total)
```

要求：

- snapshot 分母来自 `popularity_source_availability`；
- 不从 `theme_hot_source_count` 猜 completeness；
- 复用现有 canonical source mapping；
- `theme_member_count <= 0` 对 U_D 是合同不一致；
- raw 超出 `[0,1]` fail-fast，不 clip。

---

# 6. Cross-sectional Rank

## 6.1 固定 U_D

先完整构造 U_D，再计算任何 component rank。

禁止按 component 非空值重新定义 universe。

虽然 Task06-A `rank_component()` 会对 finite values 形成自身计算 universe，Task06-B wrapper 必须先证明正常可用 component 在完整 U_D 中 raw 全部合法；缺失不得通过 helper 自动缩 N 掩盖。

## 6.2 Average ties

直接复用 Task06-A：

- higher-is-better；
- 相同 raw 使用 average rank；
- `theme_id / collection_id` 不参与业务 tie-break；
- identity 排序只用于稳定输出。

## 6.3 NRS

```text
score = 100 * (N - average_rank) / (N - 1)
N = |U_D|
```

正常情况下所有 leaf 使用同一个 N。

## 6.4 N = 1

- raw 保留；
- raw_rank = 1；
- component score = NULL；
- final `theme_rank = 1`；
- final `theme_score = NULL`；
- status = `INSUFFICIENT_UNIVERSE`。

但若同时存在 trusted `UNAVAILABLE`，按 §8 的状态优先级输出 `INCOMPLETE_INPUT`，不得被 N=1 覆盖。

## 6.5 NO_VARIATION

完整 U_D 中某 component raw 全相同：

- raw 保留；
- average raw rank 保留；
- component score = NULL；
- status = `NO_VARIATION`；
- 整个 U_D 一致移除该 component 权重；
- 剩余 active leaf 按 base weight 比例统一重权。

`NO_VARIATION` 是唯一允许重权的场景。

禁止：

- 单个 Theme 缺失后重权；
- popularity unavailable 后改成 M4-only rank；
- per-theme available-only average。

若所有 leaf 均 `NO_VARIATION`：final raw/rank/score 均 NULL，status=`NO_VARIATION`。

---

# 7. Composite 与最终 Rank

## 7.1 Effective weight

```text
effective_weight_c
= base_weight_c / Σ(base_weight_j for j ∈ active leaves)
```

只有 `NO_VARIATION` leaf 可退出 active set。

## 7.2 Theme raw score

```text
theme_raw_score_i
= Σ(effective_weight_c × normalized_rank_score_i,c)
```

## 7.3 一级维度展示分

输出：

- `trend_strength_score`
- `trend_persistence_score`
- `current_structure_score`
- `popularity_support_score`

若维度至少一个 active leaf：

```text
dimension_score
= Σ(base_weight_c × component_score_c)
  / Σ(base_weight_c for active leaves in dimension)
```

若整个维度 leaf 均 `NO_VARIATION`：

```text
dimension_score = NULL
```

不得为该维度恢复固定预算后二次重权。全局 composite 始终只遵循 leaf-level effective weight。

## 7.4 Final rank

```text
theme_rank = average_descending_rank(theme_raw_score)
```

若 composite 有变化：

```text
theme_score = 100 * (N - theme_rank) / (N - 1)
```

若 active leaves 有变化但所有 Theme composite 恰好相同：common raw/rank 保留，`theme_score=NULL`，status=`NO_VARIATION`。

## 7.5 Exact final tie

最终业务并列不得依赖 DOUBLE equality。

推荐精确 key：

```text
q_i,c = 2N - 2*raw_rank_i,c
K_i   = Σ(base_weight_c * q_i,c)   # active leaves only
```

同一 run：

- K ordering 与 weighted NRS composite ordering 等价；
- K 完全相等才是真并列；
- `theme_raw_score` 可保存 DOUBLE 用于展示；
- final rank/tie 不由 DOUBLE equality 决定。

---

# 8. Missing / Unavailable / Contract Failure

## 8.1 固定 universe fail-closed

必需评分输入不能因缺失缩小 U_D，也不能做 Theme 级动态重权。

## 8.2 Trusted popularity UNAVAILABLE

若 D 日任一 required popularity source 在 `popularity_source_availability` 明确为 `UNAVAILABLE`：

- 不解释为零人气；
- C_D / U_D 仍由 M4/Theme facts 正常确定；
- 非 popularity component 可计算并写 audit；
- popularity leaf raw/score = NULL，status=`UNAVAILABLE`；
- 整个 U_D final composite/rank/score = NULL；
- eligible rows `theme_status=INCOMPLETE_INPUT`；
- 属于可信业务输入不完整，不等同代码异常。

## 8.3 AVAILABLE 但局部缺失

required popularity 全 `AVAILABLE` 时，以下任一情况均 fail-fast：

- M5 缺 U_D Theme 行；
- M5 ratio/count 不一致；
- source snapshot 与 availability 元数据不一致；
- Theme state / Episode 匹配不完整；
- expected state history 缺行；
- raw ratio 超范围；
- 重复身份结果行。

失败时不得替换 target D 的两张 Rank 结果表；保留之前已提交结果。

## 8.4 M5 / popularity 同版本一致性

全 required sources `AVAILABLE` 时，必须证明 persisted M5 与本次实际消费的 Theme/Membership、DC/THS 热榜行、availability 属于同一逻辑输入版本。

正常路径：

1. 读取 persisted `theme_m5_observation(D)`；
2. 验证 C_D 对应行完整唯一，并共享同一个 `input_snapshot_id`；
3. 基于当前 D 日 Theme/Membership 与两路完整 popularity rows 只读重算 M5 deterministic input fingerprint，优先复用现有 `compute_m5_input_snapshot_id()` / `calculate_m5_facts()`；
4. 必须满足：

```text
persisted_m5_input_snapshot_id
== recomputed_current_m5_input_snapshot_id
```

5. 验证 M5 calculation version 是允许的正式版本。

若同日热榜 replacement/correction 后 M5 尚未重算，即使 availability 数量/序号不变，也必须 fail-fast，等待 M5 对齐。

禁止用 run 时间、created_at 或不同种类 ID 字符串比较替代逻辑指纹验证。

## 8.5 普通 MISSING_INPUT

U_D 内某一个 Theme 单独缺 required leaf，且无 source-wide trusted unavailable 解释：contract failure，不参与最终排名降级。

## 8.6 C_D 非 eligible Theme

`C_D \ U_D`：

- snapshot row 仍存在；
- `rank_eligible=false`；
- eligibility reason 按规则；
- Rank/composite/dimension score 全 NULL；
- `theme_status=NOT_ELIGIBLE`。

## 8.7 Status priority

先区分合同失败：

```text
合同异常 → run FAILED → 不物化本次 target-date 新结果
```

对无合同异常的 eligible row：

```text
INCOMPLETE_INPUT
>
INSUFFICIENT_UNIVERSE
>
NO_VARIATION
>
OK
```

`NOT_ELIGIBLE` 只用于 `C_D \ U_D`。

---

# 9. 输出数据模型

新增两张表，继续位于主 DuckDB。

建议 migration：

```text
deploy/duckdb/009_system_b_theme_rank.sql
```

若实现时 009 已占用，顺延编号，禁止覆盖。

## 9.1 `system_b_theme_rank_snapshot`

主键：

```text
(trade_date, theme_id)
```

建议字段：

```text
trade_date DATE NOT NULL
theme_id VARCHAR NOT NULL
collection_id VARCHAR NOT NULL

rank_eligible BOOLEAN NOT NULL
rank_eligibility_reason VARCHAR NOT NULL

trend_strength_score DOUBLE
trend_persistence_score DOUBLE
current_structure_score DOUBLE
popularity_support_score DOUBLE

theme_raw_score DOUBLE
theme_rank DOUBLE
theme_score DOUBLE
theme_status VARCHAR NOT NULL
theme_universe_size INTEGER NOT NULL

input_provenance VARCHAR NOT NULL
diagnostics VARCHAR NOT NULL
evidence VARCHAR NOT NULL
production_run_id VARCHAR NOT NULL
calculation_version VARCHAR NOT NULL
created_at TIMESTAMP NOT NULL

PRIMARY KEY (trade_date, theme_id)
```

正常完成时物化整个 C_D，不是只写 U_D。

## 9.2 `system_b_theme_rank_component_audit`

主键：

```text
(trade_date, theme_id, dimension, component)
```

建议字段：

```text
trade_date DATE NOT NULL
theme_id VARCHAR NOT NULL
collection_id VARCHAR NOT NULL

dimension VARCHAR NOT NULL
component VARCHAR NOT NULL
raw_value DOUBLE
direction VARCHAR NOT NULL
raw_rank DOUBLE
normalized_rank_score DOUBLE
base_weight DOUBLE NOT NULL
effective_weight DOUBLE
weighted_contribution DOUBLE
universe_size INTEGER NOT NULL
tie_count INTEGER NOT NULL
status VARCHAR NOT NULL

source_provenance VARCHAR NOT NULL
metadata_json VARCHAR NOT NULL
production_run_id VARCHAR NOT NULL
calculation_version VARCHAR NOT NULL
created_at TIMESTAMP NOT NULL

PRIMARY KEY (trade_date, theme_id, dimension, component)
```

`metadata_json` 按 component 记录必要分子/分母和 lineage，避免扩展大量专用列。

---

# 10. Status Vocabulary

### Theme row

- `OK`
- `NOT_ELIGIBLE`
- `INCOMPLETE_INPUT`
- `INSUFFICIENT_UNIVERSE`
- `NO_VARIATION`

### Component

- `OK`
- `NO_VARIATION`
- `INSUFFICIENT_UNIVERSE`
- `UNAVAILABLE`
- `MISSING_INPUT`（正常 production 局部 required missing 应升级为 run failure）

### Run-level service result

- `COMPLETE`
- `INCOMPLETE_INPUT`
- `EMPTY_UNIVERSE`
- `FAILED`

不新增第三张 run 表，复用现有 pipeline/orchestration 的 `production_run_id` 与运行日志。

---

# 11. 生产、事务与 Replay

## 11.1 生产门禁

Task06-B **不得把 `theme_m5_production=SUCCESS` 声明为无条件硬依赖**，否则 trusted `UNAVAILABLE` 会被 scheduler 提前 BLOCKED。

固定判定顺序：

```text
1. Theme / M4(D) finalized
2. popularity_source_availability(D) 有正式状态
3. 条件分支
```

### Path A — 任一 required source = UNAVAILABLE

```text
M4 finalized
+ availability complete/trusted
+ any required source UNAVAILABLE
        ↓
Task06-B 允许执行
        ↓
不要求 M5(D) SUCCESS
        ↓
物化整个 C_D
eligible rows = INCOMPLETE_INPUT
final composite/rank/score = NULL
非 popularity component audit 可保留
```

### Path B — required sources 全 AVAILABLE

```text
M4 finalized
+ all required availability AVAILABLE
        ↓
必须证明 M5(D) 已完成
+ C_D 行完整
+ calculation_version 合法
+ persisted/recomputed M5 input_snapshot_id 一致
        ↓
Task06-B 正常计算
```

availability 缺行/未知不得猜成 UNAVAILABLE，按 not-ready / contract failure 处理。

不为此扩建通用动态 scheduler，也不放宽 M5 自身 fail-closed。

## 11.2 Read-only calculation core

```text
resolve/read facts
→ pure calculate_theme_ranking(...)
→ validate full result
→ materialize
```

计算核心不直接操作数据库事务。

## 11.3 Target-date atomic replace

两张表同事务按 target D 原子替换：

```text
BEGIN
  DELETE target D from component audit
  DELETE target D from snapshot
  INSERT complete validated snapshot
  INSERT complete validated component audit
COMMIT
```

异常则 ROLLBACK，不允许 partial publish。

## 11.4 Replay / idempotence

相同 target date、upstream business facts、calculation version、weight/rule version 必须产生相同业务输出。`production_run_id / created_at` 可变化，eligibility/raw/rank/score/weights/status 必须稳定。

## 11.5 Historical stability

Episode 在未来 `E>D` 正常结束不得改变过去 D 的 U_D 或 Rank：

```text
先生产 D
→ Episode 在 E>D 结束
→ replay D
```

D 的 eligibility、U_D、raw、component ranks、final rank/status 必须业务一致。

历史 D 使用 `OPEN_AT_D` 投影，不使用“当前数据库 end_date 是否 NULL”。

---

# 12. Calculation Version 与 Provenance

建议：

```text
system_b_theme_rank@0.1.0
```

`input_provenance` 至少覆盖：

- trade_date；
- C_D universe identity/provenance；
- M4 calculation_version / production_run_id / input_snapshot_id；
- M5 calculation_version / production_run_id / persisted input_snapshot_id；
- AVAILABLE 路径 current-source recomputed M5 input_snapshot_id；
- Theme index/state/episode rule versions；
- popularity availability source metadata；
- Task06-B calculation_version；
- base weights；
- active/effective weights。

不新增通用 provenance 平台。

---

# 13. Non-goals

v0.1 不纳入：

1. Task06-A M1/M2/M3 正向 Theme Core Score；
2. 高 M1/M2/M3 核心股恶化 penalty；
3. Theme 批量跌停 damage；
4. limit-down diffusion；
5. M6 market sentiment；
6. Task05 market authorization；
7. mainline boolean；
8. 账户、持仓、仓位、风险预算；
9. ENTER/HOLD/EXIT；
10. future return / predictive ML；
11. generic factor/scoring platform；
12. 修改 M4/M5 schema；
13. 新建独立 Theme Rank DB；
14. immutable multi-run event store。

---

# 14. 关键不变量

1. `C_D != U_D`；Rank eligibility 不控制上游状态产出。
2. Task06-B 不重新解释 Theme state，只做 relative evaluation。
3. 所有 leaf 使用同一个固定 U_D。
4. Missing 不得 shrink universe。
5. 只有 `NO_VARIATION` 允许统一重权。
6. Popularity unavailable 不得解释为零。
7. `theme_hot_source_count` 不能证明 completeness。
8. Theme Daily Return 只有 10%，不能退化成单日强度榜。
9. 高位横盘不因时间变长自动衰减。
10. Episode Above-MA5 Ratio 只消费 D 及以前 state。
11. D 日 Episode 资格 = finalized M4(D) pointer + D-aligned `confirmed_date <= D < end_date`；`end_date=D` 是 `NO_OPEN_EPISODE`。
12. M5 使用 PIT Membership，不用 Effective Members 偷换分母。
13. Limit-up Diffusion 用 ratio，不用裸 count。
14. Hot Appearance Rate 使用正式 valid snapshot count。
15. identity key 不参与业务 tie-break。
16. final tie 不由 DOUBLE equality 决定。
17. AVAILABLE 路径必须证明 persisted M5 fingerprint 与当前 source facts 重算 fingerprint 一致。
18. trusted UNAVAILABLE 路径不得被 M5 SUCCESS 硬依赖提前 BLOCKED。
19. Above-MA5 先校验 expected state 行完整；合法 NULL 不转 false。
20. snapshot + audit 同事务原子替换。
21. 正常完成时 snapshot 覆盖整个 C_D。
22. 上游 schema 不因 Task06-B 修改。
23. v0.1 无 Damage，不允许实现者顺手补充。

---

# 15. Acceptance Cases

至少覆盖：

### Universe / eligibility

- Open Episode + effective members → eligible。
- No open Episode → snapshot 存在、`NO_OPEN_EPISODE`、rank/score NULL。
- effective_member_count=0 → `NO_EFFECTIVE_MEMBERS`。
- `C_D \ U_D` 上游状态仍正常存在。
- Episode 未来结束后 replay D，D 的 U_D / eligibility / rank 不变。
- `end_date=D` 且 M4(D) 保留 Episode pointer → 合法 `NO_OPEN_EPISODE`。

### Raw

- Duration confirmed day=1；跨周末只计 open days。
- Above-MA5 10 valid days / 8 true → 0.8。
- Limit-up 3/30 → 0.1。
- M5 popularity 继续使用 PIT Membership。
- member_count=20、3+2 snapshots、appearance=25 → Hot Appearance Rate=0.25。
- `[true,true,NULL,NULL,NULL]` 且 5 行完整 → expected=5、valid=2、null=3、raw=1.0；不得算 2/5，也不得当成缺行。

### Ranking

- average ties；
- N=1；
- component NO_VARIATION；
- all NO_VARIATION；
- composite true tie；
- 整个一级维度 NO_VARIATION → dimension score NULL，无 0/0、无二次重权；
- N=1 + trusted UNAVAILABLE → `INCOMPLETE_INPUT` 优先。

### Missing / unavailable

- source-wide UNAVAILABLE → final rank/score 全 NULL，不解释为 0；
- AVAILABLE 但一条 M5 Theme 缺失 → fail-fast；
- component local NULL → fail-fast，不缩 universe；
- same-day popularity replacement、数量/序号不变但 M5 stale → fingerprint mismatch fail-fast；
- required source UNAVAILABLE、M5 自身 FAILED → Task06-B 仍能进入 `INCOMPLETE_INPUT` 物化路径，不被 M5 SUCCESS gate 阻断。

### Persistence

- 同输入 replay 业务字段一致；
- 写 audit 前注入异常 → snapshot 不得 partial commit；
- Episode 后续结束、Theme 后续身份变化不得自动改写已 finalized 历史 Rank。

---

# 16. 实现建议（非业务规则）

沿用 Task06-A 结构：

```text
src/qrp_atlas/contracts/system_b.py
  + Theme Rank constants / versions / statuses

src/qrp_atlas/indicators/system_b/theme_ranking.py
  + pure component derivation
  + fixed-universe validation
  + reuse normalized_rank_score / rank_component
  + composite + exact tie semantics

src/qrp_atlas/pipeline/system_b_theme_rank/service.py
  + upstream fact reads
  + C_D / U_D resolution
  + provenance
  + atomic materialization

src/qrp_atlas/pipeline/system_b_theme_rank_contracts.py
  + contract / registry integration

deploy/duckdb/009_system_b_theme_rank.sql
  + two tables
```

若现有扩展点更直接则复用，不为本任务建立 generic scoring framework。

---

# 17. rc2 对抗复审重点

1. Episode 在 `E>D` 结束后 replay D，`OPEN_AT_D` 是否保持 D 的 U_D 不变。
2. `end_date=D` + M4(D) pointer 是否得到 `NO_OPEN_EPISODE` 而非合同失败。
3. 历史 Episode Return 是否只由 D-aligned index 派生。
4. popularity `UNAVAILABLE` 且 M5 FAILED 时，Task06-B 是否仍能物化 `INCOMPLETE_INPUT`。
5. 全 AVAILABLE 时 same-day source replacement + stale M5 是否被 fingerprint 校验阻断。
6. Above-MA5 expected state 行完整与合法 NULL 是否正确区分。
7. 状态优先级是否覆盖组合退化场景。
8. 一级维度全部 leaf NO_VARIATION 是否输出 NULL 且无二次重权。
9. fixed U_D / average ties / exact final tie / atomic replace 不得回归。
10. 修订不得要求修改 M4/M5 schema、放宽 M5 fail-closed 或扩建通用 scheduler。

---

# 18. v0.1 结论

```text
C_D：全部合法 canonical Theme
        ↓
U_D：effective_member_count > 0
     + finalized M4(D) pointer
     + D-aligned open Episode
        ↓
7 个 leaf raw facts
        ↓
固定 U_D average-rank + NRS
        ↓
NO_VARIATION-only uniform reweight
        ↓
weighted Theme Trend composite
        ↓
average final rank + final NRS
        ↓
snapshot + component audit
```

业务权重：

```text
35% Episode Return
15% Episode Duration
20% Episode Above-MA5 Ratio
10% Theme Daily Return
10% Limit-up Diffusion
 6% Hot Stock Ratio
 4% Hot Appearance Rate
```

rc2 只修复 rc1 对抗审计确认的时间语义、生产门禁、版本一致性与退化状态边界；评分结构与权重保持不变。Damage / Breakdown 不进入 v0.1。
