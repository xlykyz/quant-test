# Task07-C — System B Portfolio Constraint Resolution & Final Target 设计书

> 状态：DESIGN DRAFT 2 / IMPLEMENTATION READY
>
> 分支基线：`develop/v1.1 @ fbc1865b4b64708372905602974ca5f659120710`
>
> 前置：Task07-A 已完成 native full-snapshot Portfolio Target 合同；Task07-B 已完成 System-B-local Holding / Entry / Exit Decision。
>
> 任务身份：Task07 的最后一个业务工作包。07-C 只负责把 07-B 的资产级业务判断，在 System B 的组合容量与仓位约束下解析成唯一、完整、可审计的 desired `StrategyPortfolioTarget`。
>
> DRAFT 2 修订冻结：补齐普通 distinct-slot 容量耗尽与 cutoff tie 的诊断语义；权重容差统一为 `1e-12`；明确合同/不变量破损必须 fail-closed，而合法业务候选因组合约束未选中才进入 diagnostics；comparison score 同分采用 exact equality，禁止 `isclose`；正式挂载仅允许最小显式 System B input-normalizer 路由。

---

## 1. 任务目标

Task07-C 回答：

> **当 Task07-B 已经判断出哪些资产应该 EXIT / HOLD / ENTER(new) / ENTER(add) 后，在固定 1/8、最多 6 只、单票最多两次、单票新增风险不越 30%、组合总 desired weight 不越 100% 等约束下，最终 System B 想持有什么、各持有多少？**

输出必须是：

```text
StrategyPortfolioTarget(full snapshot)
```

不是增量信号，也不是订单计划。

---

## 2. 业务规则来源

System B 业务 SSOT：

```text
xlykyz/MyTradingSystem
source_commit = 82369650d16914e42c03da7635f410b12a38220e
source_document = docs/15_交易系统2.0(初稿).md
```

本任务直接使用的已确认规则：

1. 每次买入固定使用账户总资产的 `1/8 = 12.5%`；
2. 同一股票最多买入两次；
3. 第二次买入仍是固定 1/8；
4. 单票计划路径为第一次 12.5%、第二次累计约 25%，新增风险不得使目标仓位超过 30%；
5. 同时持有不同股票不超过 6 只；
6. 不另设每日开仓次数上限；
7. 同一股票加仓不增加 distinct holding count；
8. 已有 6 只时禁止新增不同股票；
9. 新候选不得通过卖出已有持仓来“腾名额”；
10. 已有持仓只有正式 EXIT 规则才能退出；
11. BUY 决策与现实成交分层，整数手、价格、停牌/涨跌停和实际成交失败不属于策略 desired target。

---

## 3. 与 07-A / 07-B 的边界

### 3.1 07-A 已解决“目标合同”

07-C 直接复用：

```text
StrategyHoldingState
StrategyPortfolioTargetPosition
StrategyPortfolioTarget
StrategyRunResult.portfolio_targets
```

并继续遵守：

- target 是 signal/strategy date，不是 execution date；
- full snapshot；
- omitted current holding = desired weight 0；
- native target 是最高 portfolio authority；
- target 不表达 quantity / order / fill。

### 3.2 07-B 已解决“该不该”

07-C 不重新判断：

- MA5 两日 EXIT；
- authorization；
- eligibility；
- severe abnormal supervision；
- relative-score threshold；
- ADD 是否处于最高评分层；
- comparison-score provenance 是否可用。

07-C 消费 07-B 已验证的：

```text
StrategyDecision.action
StrategyDecision.score
StrategyDecision.reason_code
StrategyDecision.evidence.entry_kind
```

因此 canonical `07-B → 07-C` 链路中的 07-B 决策不变量属于 07-C 输入合同的一部分。若 07-C 收到一个按冻结 07-B 规则不可能产生的 ENTER intent，不得将其伪装成正常组合约束拒绝后继续运行，应 fail-closed。

### 3.3 07-C 只解决“约束后最终多少、最终选谁”

07-C 负责：

```text
EXIT first
retained exposure preservation
fixed 1/8 increments
single-asset add cap
max 6 distinct holdings
multiple ENTER candidate competition
desired gross-weight capacity
score-tie fail-closed
full-snapshot target construction
constraint-resolution audit
```

---

## 4. Strategy 与 Execution 边界再次冻结

07-C 结束于：

```text
desired portfolio target
```

### 07-C 负责的“容量”

只包括可由策略权重直接确定的容量：

- target gross weight `<= 1.0`；
- fixed 1/8 increment 是否还能容纳；
- NEW 后 distinct holdings 是否 `<= 6`；
- ADD 后该资产 desired weight 是否 `<= 0.30`。

### 下游 Portfolio / Backtest 负责

- 次一交易日执行；
- 100 股整数手；
- 买入价格；
- 停牌；
- 涨跌停；
- 现实现金余额；
- 价格变化导致的可买数量；
- 手续费、滑点；
- 未成交 / 部分成交；
- realized holdings。

因此路线图中“现金不足 / 不可成交 / 整数手”的现实执行语义，不在 07-C 内伪装成 strategy target 逻辑；07-C 只处理 **desired weight budget**。

---

## 5. 实现形态

07-C 仍采用 System-B-local 最小实现，不建设新的通用 Portfolio Framework。

建议核心纯函数：

```python
resolve_system_b_portfolio_target(
    *,
    trade_date: str,
    holdings: Mapping[str, StrategyHoldingState],
    decisions: Sequence[StrategyDecision],
    strategy_code: str,
    strategy_version: str,
) -> StrategyPortfolioTarget
```

允许再提供一个薄的 System B strategy orchestration wrapper，把：

```text
07-B normalize
→ 07-B evaluate
→ 07-C resolve
→ StrategyRunResult(decisions + portfolio_targets)
```

组合成正式 System B strategy result。

冻结：

- 不新增 Common `PortfolioIntent`；
- 不新增 Common `ADD` action；
- 不修改 generic equal-weight semantics 来硬适配 System B；
- 不把 System B 1/8 / 30% / max6 写进 generic `weights.py`；
- 如需正式挂载 Registry，最多增加最小、显式的 System B input-normalizer 路由，不建设 validator plugin registry。

---

## 6. 输入合同

### 6.1 单日

一个 resolver invocation 只允许一个 canonical：

```text
trade_date = YYYY-MM-DD
```

所有 decisions 必须同日。

### 6.2 Holdings

使用 Task07-A / 07-B 已验证的：

```text
Mapping[str, StrategyHoldingState]
```

至少要求：

- key == state.asset_id；
- current_weight positive finite；
- entry_count integer >=1；
- entry dates 合法。

07-C 进一步要求初始持仓总权重：

```text
sum(current_weight) <= 1 + PORTFOLIO_WEIGHT_TOLERANCE
```

若初始快照本身超过 100%，属于无效 portfolio state，resolver fail-closed，不通过偷偷卖出修复。

### 6.3 Decisions 完整覆盖

Task07-B decisions 的资产域必须完整覆盖：

```text
initial holding asset ids
UNION
entry-side candidate asset ids
```

同一 `trade_date + asset_id` 唯一。

07-C 不接受缺少已有持仓 decision 的输入。

### 6.4 Strategy identity

所有 decisions 必须与 resolver 的：

```text
strategy_code
strategy_version
```

一致。

### 6.5 ENTER decision

任何 `ENTER` 必须满足：

```text
score = finite numeric
entry_kind = NEW | ADD
```

明确拒绝：

```text
None
NaN
+inf
-inf
```

且 evidence 必须能证明 07-B 已完成必要业务 gate。

07-C 不重新推导 comparison score。

### 6.6 Weight tolerance SSOT

Task07-C 所有**权重边界**统一使用：

```python
PORTFOLIO_WEIGHT_TOLERANCE: float = 1e-12
```

用途仅限：

```text
portfolio gross-weight bound
single-asset target-weight bound
final target weight validation
```

其值必须与 Task07-A native portfolio-target validation 的 `1e-12` 保持严格一致，避免 07-C 自身接受、统一 checked validation 随后拒绝的双重口径。

不得把该常量泛化成 `FLOAT_TOLERANCE`，也不得用于 comparison-score 同分判定。

### 6.7 Failure taxonomy：合同破损 vs 业务约束拒绝

07-C 必须区分两类失败：

#### A. Contract / invariant violation → fail-closed

属于系统级坏输入或上游冻结不变量被破坏，必须抛出统一策略校验异常（沿用现有 `StrategyValidationError` 边界），不得正常返回一个“看似合法”的降级 target。

至少包括：

```text
trade_date mismatch
strategy_code / strategy_version mismatch
decisions 未完整覆盖要求的资产域
同一 trade_date + asset_id 重复 decision
initial portfolio gross > 1 + PORTFOLIO_WEIGHT_TOLERANCE
retained_count > 6
ENTER score 非 finite
entry_kind 与持仓身份矛盾
ADD 对应资产不在 retained holdings
ADD 的 entry_count != 1
canonical 07-B 链路中出现按冻结 07-B 规则不可能产生的 ENTER intent
```

特别冻结：

```text
entry_count >= 2 + ENTER(ADD)
→ StrategyValidationError
```

该情形意味着 07-B / orchestration / input contract 回归，不得新增 `ADD_ENTRY_LIMIT_EXCEEDED` diagnostic 将其吞掉。

#### B. Business constraint rejection → diagnostics

候选 intent 本身合同合法、业务上允许参与组合解析，但最终因 07-C 负责的组合约束未被选中。此时不得 crash，原持仓按 full-snapshot 规则保留，并输出稳定 diagnostics。

至少包括：

```text
ADD_SINGLE_ASSET_CAP_EXCEEDED
NEW_DISTINCT_CAPACITY_INSUFFICIENT
NEW_DISTINCT_CAPACITY_TIE_UNRESOLVED
PORTFOLIO_WEIGHT_CAPACITY_INSUFFICIENT
ENTER_WEIGHT_CAPACITY_TIE_UNRESOLVED
```

判定原则：

```text
坏合同 / 不可能状态 → exception
合法 intent + 当前组合装不下 → diagnostic rejection
```

---

## 7. Base Target：先处理现有持仓

07-C 首先从 initial holdings 建立 base desired portfolio。

### 7.1 EXIT

```text
held + action=EXIT
→ final desired weight = 0
```

在 full snapshot 中优先通过“省略该资产”表达 0。

EXIT 是终态；该资产不得在同日再次被任何 ENTER 恢复。

### 7.2 HOLD

```text
held + action=HOLD
→ base desired weight = current_weight
```

不把持仓主动重平衡回 12.5% 或 25%。

### 7.3 Held + NO_ACTION

07-B 的 held `NO_ACTION` 主要表示 EXIT status 等输入不足。

冻结：

```text
held + NO_ACTION
→ preserve current_weight
```

因为 unavailable 不能被解释成 desired exit。

### 7.4 Held + ENTER(ADD)

在进入约束解析前：

```text
base desired weight = current_weight
```

只有 ADD 最终被 07-C 选中后才增加 0.125。

---

## 8. Fixed 1/8 Increment

冻结：

```text
ENTRY_INCREMENT = 0.125
```

### NEW

选中：

```text
target_weight = 0.125
```

不允许：

- 缩成 0.10；
- 按剩余容量比例缩放；
- 多候选 pro-rata；
- 用 cash_buffer 自动改写。

### ADD

选中：

```text
target_weight = current_weight + 0.125
```

即 1/8 是**新增风险增量**，而不是把已有持仓强制重平衡到固定锚点。

---

## 9. 25% planned path 与 30% hard cap

### 9.1 25% 不是日常 rebalance anchor

源规则中的：

```text
第一次 12.5%
第二次累计 25%
```

描述的是固定两次 1/8 买入的计划路径。

已有仓位会因价格变化产生 current_weight drift，因此 07-C 不应：

```text
HOLD → 强制改成 12.5% / 25%
```

否则等价于引入未经批准的每日再平衡交易。

### 9.2 30% 是新增风险 hard gate

对于**合同合法的 ADD candidate**：

```text
current_weight + 0.125 <= 0.30 + PORTFOLIO_WEIGHT_TOLERANCE
```

才允许最终选中。

若已有持仓因价格上涨被动超过 30%：

- 不因该事实自动 EXIT；
- 不因该事实主动减仓至 30%；
- 当日不得 ADD；
- 保留 current_weight。

reason / diagnostic：

```text
ADD_SINGLE_ASSET_CAP_EXCEEDED
```

注意：`entry_count != 1` 不属于本节业务 cap rejection，而属于 §6.7 的 contract / invariant violation。

---

## 10. Distinct Holding Capacity

### 10.1 Retained count

```text
retained holdings
=
initial holdings
-
confirmed EXIT assets
```

### 10.2 基础不变量

正常输入必须：

```text
retained_count <= 6
```

若 retained_count > 6：

- 不能按 ticker 或 score 强制卖出；
- 不能生成违反 System B max6 的新 target；
- resolver fail-closed，要求上游/人工修复异常状态。

### 10.3 NEW

每个最终选中的 NEW：

```text
consumes 1 distinct slot
```

### 10.4 ADD

ADD：

```text
consumes 0 distinct slots
```

### 10.5 Available slots

```text
available_new_slots = 6 - retained_count
```

EXIT 先执行，因此同日 EXIT 可以为**其他新股票**释放 slot。

---

## 11. Desired Gross-Weight Capacity

07-C 不读取现实 cash balance；只读取 target 权重预算。

### 11.1 Base gross

```text
base_gross
=
sum(base desired weight of retained holdings)
```

必须：

```text
0 <= base_gross <= 1 + PORTFOLIO_WEIGHT_TOLERANCE
```

### 11.2 每个 ENTER 的增量

无论 NEW / ADD：

```text
increment = 0.125
```

### 11.3 固定增量不可拆分

若剩余 desired weight budget 不足 0.125：

```text
该 ENTER 不可选中
```

不得缩仓补齐。

诊断：

```text
PORTFOLIO_WEIGHT_CAPACITY_INSUFFICIENT
```

### 11.4 与现实现金的区别

即使 desired target 有 12.5% weight budget，次日因价格、整数手、手续费等仍可能买不到完整计划金额。

该现实差异由 Portfolio / Backtest 负责，不反向改变 07-C target。

---

## 12. Multiple ENTER Candidate Resolution

源规则明确：

- 不另设每日开仓次数上限；
- 允许多个新增，只要约束允许；
- 评分用于选择更优合格标的；
- 不得为新候选替换已有持仓。

因此 07-C 不应“每天只买一个”。

### 12.1 基本排序 authority 与 score grouping

只使用：

```text
comparison_score DESC
```

不得使用 ticker 作为业务 priority。

同分定义冻结为：

```text
score_a == score_b
```

即 exact numeric equality。

禁止：

```text
math.isclose
abs(score_a - score_b) <= epsilon
round(score, n) 后再分组
```

07-C 的 weight tolerance 与 comparison-score tie semantics 完全独立。ENTER score 必须在进入排序/分组前通过 finite validation，避免 NaN 破坏 equality / ordering。

### 12.2 NEW 的 slot resolution

先只看 NEW candidates。

按 score 从高到低按**同分组**处理。

#### Case A：普通容量已耗尽

若：

```text
remaining_new_slots == 0
```

则当前及更低分 NEW 均没有 distinct slot 可用。

诊断：

```text
NEW_DISTINCT_CAPACITY_INSUFFICIENT
```

这不是 tie，不得误记为 `NEW_DISTINCT_CAPACITY_TIE_UNRESOLVED`。

#### Case B：整组可容纳

若：

```text
0 < group_size <= remaining_new_slots
```

则整个 group 进入下一步 gross-capacity resolution。

#### Case C：真正 cutoff tie

若：

```text
0 < remaining_new_slots < group_size
```

则：

- 不允许用 ticker 从同分组中挑部分；
- 该同分 NEW group 全部不选；
- 更低分 NEW 也不得越级获得 slot；
- NEW slot resolution 在该 cutoff 处停止继续向低分选择；
- 记录 unresolved capacity tie。

诊断：

```text
NEW_DISTINCT_CAPACITY_TIE_UNRESOLVED
```

不新增 `LEAPFROG_PREVENTED` 作为必需业务码；“高分 unresolved tie 后低分不得越级”属于 resolution 控制流不变量。只有现有 diagnostics 合同明确要求为每个后续资产逐一给出拒绝 reason 时，才允许在不改变业务语义的前提下补充审计表达。

ADD 不消耗 distinct slot，因此不因该 NEW-only tie 自动被拒绝。

### 12.3 Gross-capacity resolution

把：

```text
slot-admitted NEW
UNION
individually-feasible ADD
```

放入统一 ENTER pool。

仍按 `comparison_score DESC` 的 exact-equality 同分组处理。

若整个 score group 的固定 1/8 increments 都能装入剩余 target gross budget：

```text
select whole group
```

否则：

- 不允许在同分组内用 ticker 选部分；
- 该同分组全部不选；
- 更低分 ENTER 不得越级；
- 记录容量 unresolved。

若 group 只有一个资产且仍装不下，则 reason 为普通 weight capacity insufficient；若 group >1 且只能部分容纳，则为 tie unresolved。

诊断：

```text
PORTFOLIO_WEIGHT_CAPACITY_INSUFFICIENT
ENTER_WEIGHT_CAPACITY_TIE_UNRESOLVED
```

---

## 13. Candidate Validation Before Group Resolution

在 score-group 竞争前，07-C 先区分**输入不变量校验**与**组合业务可行性**。

### 13.1 NEW structural invariants

必须：

- asset not in initial holdings；
- 07-B `entry_kind=NEW`；
- target increment=0.125；
- score finite；
- 非 same-day EXIT asset。

违反这些条件表示输入身份/07-B 冻结不变量矛盾，按 §6.7 fail-closed，而不是写一个普通 capacity rejection diagnostic。

### 13.2 ADD structural invariants

必须：

- asset in retained holdings；
- 07-B `entry_kind=ADD`；
- `entry_count == 1`；
- score finite。

其中：

```text
entry_count != 1
ADD asset 不在 retained holdings
entry_kind / holding identity 矛盾
```

均属于 contract / invariant violation，必须抛 `StrategyValidationError`。

### 13.3 ADD business feasibility

在 structural invariants 已通过后，再检查：

```text
current_weight + 0.125 <= 0.30 + PORTFOLIO_WEIGHT_TOLERANCE
```

若仅该条件失败：

- 不增加 0.125；
- 原持仓继续 `POSITION_PRESERVED`；
- 07-B ENTER decision 保留；
- 记录 `ADD_SINGLE_ASSET_CAP_EXCEEDED` diagnostic；
- resolver 继续处理其他合法 candidates。

07-C 不重写 07-B 的原业务 decision。

---

## 14. Tie Semantics

### 14.1 不使用技术字段伪造业务优先级

禁止：

```text
ticker ASC
asset_id ASC
input row order
hash order
```

决定谁获得稀缺 slot / weight budget。

### 14.2 Exact equality 是业务同分定义

同分只认：

```text
comparison_score_a == comparison_score_b
```

不使用任何 tolerance / `math.isclose`。

该规则定义的是 System B 的业务比较语义，而不是对浮点底层字节表示作额外架构保证。

### 14.3 asset_id 只用于 canonical serialization

最终 target positions 仍按：

```text
asset_id ASC
```

稳定序列化。

这只是输出顺序，不是业务 selection priority。

### 14.4 Fail-closed 原则

当业务 score 无法区分、但容量又不足以容纳整个同分组时：

```text
不猜
不随机
不按 ticker
```

通过 unresolved diagnostic 显式保留未裁决状态。

---

## 15. Final Target Construction

最终正权重资产只来自：

```text
retained holdings
+
selected NEW
```

其中 retained holding 若 selected ADD，则 weight 增加 0.125。

### 15.1 Position target weight

```text
EXIT                       → omitted / 0
HOLD                       → current_weight
held NO_ACTION             → current_weight
ENTER(ADD) not selected    → current_weight
ENTER(ADD) selected        → current_weight + 0.125
ENTER(NEW) not selected    → omitted / 0
ENTER(NEW) selected        → 0.125
```

### 15.2 Full snapshot

`positions` 只需要包含最终 desired positive holdings。

因此：

- EXIT asset 可省略；
- rejected NEW 可省略；
- `positions=()` 表示 desired 全现金。

### 15.3 Final invariants

正常返回 target 必须满足：

```text
all weights finite
0 < each position weight <= 1
sum(weights) <= 1 + PORTFOLIO_WEIGHT_TOLERANCE
positive distinct positions <= 6
```

并且：

- 同日 EXIT asset 不得重新出现；
- 没有业务 EXIT 的 existing holding 不得被无故省略；
- ADD 不得使该资产从新增风险角度越过 30% hard cap。

---

## 16. Target Position Reason / Evidence

建议 position reason：

```text
POSITION_PRESERVED
NEW_ENTRY_SELECTED
ADD_ENTRY_SELECTED
```

position evidence 至少保存：

```text
source_decision_action
source_decision_reason_code
source_entry_kind
comparison_score
prior_weight
entry_increment
final_target_weight
entry_count_before
entry_count_after_if_selected
was_initially_held
was_retained
```

并透传必要 comparison-score provenance：

```text
score_calculation_version
rule_version_set_id
parameter_set_id
input_snapshot_id
```

07-C 不重算 provenance。

---

## 17. Constraint-Rejection Audit

07-B 的 ENTER decision 表示“业务上允许成为进入候选”；07-C 可能因组合约束最终不选。

不得改写 07-B decision 为 NO_ACTION。

最终 `StrategyRunResult` 应同时保留：

```text
07-B decisions
+
07-C native portfolio target
```

07-C 使用 `StrategyPortfolioTarget.diagnostics` 或上层 `StrategyRunResult.diagnostics` 记录**业务组合约束拒绝**。

建议稳定诊断格式：

```text
SYSTEM_B_TARGET_REJECTION|asset_id=<id>|reason=<CODE>
```

至少 reason：

```text
ADD_SINGLE_ASSET_CAP_EXCEEDED
NEW_DISTINCT_CAPACITY_INSUFFICIENT
NEW_DISTINCT_CAPACITY_TIE_UNRESOLVED
PORTFOLIO_WEIGHT_CAPACITY_INSUFFICIENT
ENTER_WEIGHT_CAPACITY_TIE_UNRESOLVED
```

明确不新增：

```text
ADD_ENTRY_LIMIT_EXCEEDED
```

因为 `entry_count >= 2 + ADD` 已冻结为合同/不变量破损，应 fail-closed，不属于可继续运行的业务 constraint rejection。

不为此新增新的 Common rejection model。

---

## 18. 决策顺序

固定：

```text
1. validate holdings + decisions contract
2. validate 07-B ENTER structural invariants + finite scores
3. identify same-day EXIT terminal assets
4. build retained holdings / base desired weights
5. validate retained_count and base_gross
6. extract ENTER NEW / ADD intents
7. reject contract-valid but business-infeasible ADD by single-asset cap
8. resolve NEW distinct slots by exact-score groups
9. resolve shared desired gross-weight budget by exact-score groups
10. apply selected 1/8 increments
11. build full-snapshot target
12. validate final invariants with PORTFOLIO_WEIGHT_TOLERANCE
13. canonical sort + diagnostics
```

任何输入顺序不得改变结果。

任何 contract / invariant violation 在进入正常 score-group capacity resolution 前 fail-closed。

---

## 19. 与 generic weights.py 的关系

现有 generic `equal_weight_targets()` / `selection_to_target_weights()` 适合 Top-N equal-weight 场景。

System B 不是 equal-weight rebalance：

- existing HOLD 保留 current weight；
- ADD 是固定增量；
- NEW 是固定 1/8；
- EXIT 才清零；
- 不能每天把所有持仓重新均分。

因此 07-C **不得直接套 equal_weight_targets() 生成 System B 最终 target**。

可以复用的仅是 Common 已有：

- target schema；
- validation；
- full-snapshot converter；
- deterministic result routing。

不应为了“代码复用”把 System B 业务语义塞进 generic equal-weight helper。

---

## 20. 正式 System B Strategy Integration

07-C 完成后，Task07 应具备一个正式 System B portfolio result 路径：

```text
prepared System B facts
+ typed holdings
+ Task05 authorization
+ comparison_score + provenance

→ 07-B local normalize
→ 07-B decisions
→ 07-C target resolution
→ StrategyRunResult(
     decisions=...,
     portfolio_targets=(full_target,),
     diagnostics=...
   )
```

建议正式 strategy code：

```text
system_b_portfolio
```

但不得破坏：

```text
system_b_basic
system_b_authorization
```

现有 legacy/basic strategy 继续保持原语义。

### 20.1 Checked runner normalizer routing

如果正式 Registry 挂载要求 `run_strategy_checked` 识别 System B local input contract，只允许增加与现有 special-case 模式一致的**最小显式 System B normalizer route**，使 System B 的 NA/None 与私有事实字段继续由现有 System B normalization 语义处理。

允许：

```text
strategy_code == system_b_portfolio
→ explicit System B input normalizer
→ registered strategy runner
```

禁止借 Task07-C 新建：

```text
generic validator plugin registry
StrategyInputNormalizerRegistry
Strategy Framework v2
```

该接入只解决 System B 正式 checked-runner 挂载，不扩大 common abstraction surface。

---

## 21. 测试矩阵

### 21.1 Base target / EXIT

1. HOLD → preserve current_weight；
2. held NO_ACTION → preserve current_weight；
3. EXIT → omitted；
4. same-day EXIT terminal asset 不得重新出现在 target；
5. EXIT 后释放 NEW slot 给其他资产。

### 21.2 Fixed 1/8

1. NEW selected → 0.125；
2. ADD selected → current + 0.125；
3. 不允许 partial increment；
4. 不允许 pro-rata；
5. 0.1249 remaining budget 不允许 0.1249 entry。

### 21.3 Single asset cap / ADD invariant

1. current=0.125 + ADD → 0.25；
2. current=0.175 + ADD → 0.30；
3. current>0.175 + contract-valid ADD → rejected，`ADD_SINGLE_ASSET_CAP_EXCEEDED`，原持仓保留；
4. current>0.30 + HOLD → preserve，不自动 trim；
5. `entry_count>=2 + ADD` → `StrategyValidationError` fail-closed，不生成 `ADD_ENTRY_LIMIT_EXCEEDED` diagnostic。

### 21.4 Distinct holdings

1. retained=0，多 NEW 可进入直到约束上限；
2. retained=5，score=95 NEW 成功占用唯一 slot，随后 score=80 NEW → `NEW_DISTINCT_CAPACITY_INSUFFICIENT`，不是 tie；
3. retained=5，两个同为 score=95 的 NEW 竞争唯一 slot → 整组不选，`NEW_DISTINCT_CAPACITY_TIE_UNRESOLVED`；
4. retained=6，NEW 因普通 slot exhaustion 拒绝，ADD仍可评估；
5. initial=6 EXIT 1 → retained=5，可新增 1；
6. retained>6 → fail-closed，不强制卖出修复。

### 21.5 Score priority

1. 多 NEW distinct scores，按高分优先；
2. capacity 足够时不人为限制每日只买一个；
3. lower score 不得越过 higher score 获取稀缺 slot；
4. ADD 与 NEW 一起竞争 gross budget 时按 score authority；
5. asset_id 不影响业务选择。

### 21.6 Tie / exact score semantics

1. 2 个 exact-equal score NEW、2 slots → 两个都选；
2. 2 个 exact-equal score NEW、1 slot → 两个都不选，diagnostic；
3. 上述情况下更低分 NEW 不得越级；
4. exact-equal score group gross budget 只够部分 → 全组不选；
5. 改变输入行顺序结果不变；
6. 两个仅“非常接近”但不 exact equal 的 finite scores 不得因 `isclose` 被合并成 tie group；
7. `NaN` / `+inf` / `-inf` ENTER score → fail-closed。

### 21.7 Gross target capacity

1. base_gross=0.75，可容纳两个 0.125；
2. base_gross=0.875，只能容纳一个 0.125；
3. base_gross>0.875，不能容纳任何 fixed entry；
4. 若 score 产生明确优先级，最高者先获得 budget；
5. cutoff tie 不按 ticker 拆分。

### 21.8 Floating boundary alignment

1. `sum(weights) == 1.0` → valid；
2. `sum(weights) <= 1.0 + 1e-12` 的统一边界与 Task07-A checked validation 一致；
3. `sum(weights) > 1.0 + 1e-12` → fail-closed；
4. ADD 30% gate 使用同一 `PORTFOLIO_WEIGHT_TOLERANCE=1e-12`；
5. comparison-score equality 不读取该 tolerance。

### 21.9 Full snapshot

1. final positive holdings 完整；
2. EXIT omitted 意味 0；
3. unselected NEW omitted；
4. existing non-EXIT 不得丢失；
5. positions asset_id ASC；
6. sum<=1 + `1e-12`；
7. distinct<=6。

### 21.10 Checked runner / orchestration

1. `system_b_portfolio` 正式路径使用 System B local normalization；
2. System B 合法 NA/None 事实不会被错误送入 generic strict normalizer 后误拒；
3. `run_strategy_checked` 最终仍通过统一 result validation；
4. 不引入 generic validator plugin registry；
5. legacy `system_b_basic` / `system_b_authorization` 行为不变。

### 21.11 Regression

必须覆盖：

- Task05；
- Task06；
- Task07-A；
- Task07-B；
- `system_b_basic`；
- generic weights / native target adapter；
- full regression。

---

## 22. Definition of Done

Task07-C 完成必须满足：

1. 07-B decisions 可确定性解析为 native full-snapshot target；
2. EXIT-first / same-day terminal 不回归；
3. HOLD / held NO_ACTION preserve current exposure；
4. NEW / ADD 固定 0.125 increment；
5. 不主动把持仓重平衡到 12.5% / 25%；
6. contract-valid ADD 新增风险不得使 target 超过 30%，否则保留原仓并诊断拒绝；
7. `entry_count>=2 + ADD` 等 07-B 不变量破损必须 fail-closed，不降级成 business diagnostic；
8. retained holdings / NEW 后 distinct <=6；
9. ordinary distinct-slot exhaustion 与 cutoff tie 使用不同诊断语义；
10. target gross <=1，权重边界统一使用 `PORTFOLIO_WEIGHT_TOLERANCE=1e-12`；
11. 容量不足时不缩放 fixed entry；
12. 多候选按 comparison score authority 解析；
13. comparison-score tie 使用 exact equality，不使用 `math.isclose` / weight tolerance；
14. non-finite ENTER score fail-closed；
15. cutoff score ties 不使用 ticker 伪优先级，且更低分不得 leapfrog；
16. 07-B decisions 与 07-C business rejection audit 同时保留；
17. target full snapshot 通过 Task07-A canonical validation；
18. 不修改 comparison-score 算法；
19. 不把整数手/价格/停牌/涨跌停/现实 cash feasibility 拉回 strategy 层；
20. checked runner 仅增加最小显式 System B normalizer route（若正式 Registry 挂载需要）；
21. 不扩张 Strategy Framework；
22. legacy `system_b_basic` / `system_b_authorization` 不回归；
23. targeted + full regression 通过。

---

## 23. Task07 完成后的边界

Task07-C 完成后，Task07 的产品输出应稳定为：

```text
System B facts / authorization / score inputs
→ Holding / Entry / Exit decisions
→ Portfolio constraint resolution
→ complete desired portfolio target
```

到此为止，System B 的策略业务闭环成立。

下一步 Task08 才进入：

```text
historical replay
validation
dry-run
cost / execution realism evaluation
```

---

## 24. 当前仍显式未冻结的业务项

仍然不在 07-C 内解决：

```text
comparison_score 的具体公式
```

07-C 继续假设 Task07-B 输入中的 comparison score 已由上游按同版本、同快照解析好。

真实 production 若 approved comparison-score model 缺失，07-B 会阻断 NEW/ADD，因此 07-C 仍可生成仅包含现有 HOLD / EXIT 结果的合法 target，不得自行补评分公式。
