# Task07-B — System B Holding / Entry / Exit Decision 设计书

> 状态：DESIGN REVISION 1 / 待最终审查
>
> 设计基线：`develop/v1.1 @ 11554e02618da1a901c423ed02bc99c290946c41`
>
> 前置：Task07-A 已完成并合并；typed holdings、checked runner、native full-snapshot portfolio target 与 canonical `StrategyRunResult` 已可用。
>
> Revision 1：修正同日 EXIT 后误重新 ENTER 的生命周期漏洞；冻结 System-B-local prepared-input normalizer 与显式三态输入；补齐 `comparison_score` 同版本、同快照 provenance 合同。

---

## 1. 任务目标

Task07-B 回答：

> **在给定当日新增仓授权、候选资格、统一比较分、严重异动状态和初始持仓的前提下，System B 对每只股票应当 HOLD、ENTER、EXIT 还是 NO_ACTION？**

本任务只实现 System B 的 Holding / Entry / Exit Decision，不计算最终组合权重。

必须落实：

- 已有持仓每日继续评分，但评分、排名、M 身份和判断层变化本身不触发卖出；
- 连续两个实际交易日收盘低于 MA5 → EXIT 全部剩余持仓；
- 严重异动监管期禁止开仓和加仓，但监管期本身不自动清仓；
- 新候选必须通过相对已有持仓评分门槛；
- 已有持仓满足条件时可成为第二次买入候选；
- 同一股票最多买入两次；
- 同日先确认 EXIT，再用 retained holdings 参与其他资产的新增判断；
- **当日确认 EXIT 的资产，EXIT 是该资产当日终态，不得同日重新 ENTER。**

---

## 2. 业务规则来源

System B 业务 SSOT：

```text
xlykyz/MyTradingSystem
source_commit = 82369650d16914e42c03da7635f410b12a38220e
source_document = docs/15_交易系统2.0(初稿).md
```

Task07-B 不重新解释评分模型，也不发明未批准规则。

当前已批准规则：

1. 候选与已有持仓每日使用同一评分口径；
2. 评分下降、排名下降、失去 M 身份、授权变化均不触发已有持仓退出；
3. 连续两个实际交易日收盘低于 MA5 才确认 EXIT；
4. 严重异动监管期禁止开仓和加仓；
5. 新候选相对评分门槛：
   - retained=0：无持仓比较门槛；
   - retained=1—3：严格高于最低持仓分；
   - retained=4—5：严格高于全部持仓，即严格高于最高持仓分；
   - retained>=6：禁止新增不同股票；
6. 高分候选不得替换或强制卖出已有持仓；
7. 最高评分层中的合格已有持仓可成为第二次买入候选；
8. 同一股票最多买入两次。

---

## 3. `comparison_score` 暂存假设

### 3.1 未冻结内容

以下仍未批准，07-B 不得实现：

- 最终评分指标集合；
- 指标归一化；
- 权重；
- 综合分公式；
- M1/M2/M3 映射阈值。

### 3.2 输入语义

07-B 暂时消费：

```text
comparison_score = finite float | UNAVAILABLE
```

仅表示：在同一 `trade_date`、同一评分规则/计算版本、同一输入快照下，对候选和持仓使用同一口径得到、可直接比较的统一分数。

不得：

- 用 rank 替代；
- 用 M 身份反推；
- 缺失值按 0；
- 混用不同版本或不同 snapshot 的分数。

### 3.3 Score fail-closed

新增 / 加仓所需 score 或 provenance 不完整时：

```text
entry side = blocked
```

但：

- MA5 EXIT 继续正常判断；
- 既有持仓不得因 score 问题被清零；
- HOLD / EXIT 不依赖 comparison-score 完整性。

---

## 4. Task07-B 与 Task07-C 边界

### 4.1 07-B 负责

```text
initial holdings
+ normalized exit status
+ authorization status
+ entry eligibility status
+ comparison score
+ severe-abnormal supervision status

→ per-asset StrategyDecision
```

输出：

```text
ENTER
HOLD
EXIT
NO_ACTION
```

### 4.2 07-C 负责

- 每次 1/8 总资产；
- 第二次买入后的计划 25%；
- 单票 hard max <=30%；
- 多个 ENTER 候选的容量竞争；
- 最多 6 只的最终组合约束；
- 现金不足；
- 最终完整 `StrategyPortfolioTarget`。

07-B 不生成 `target_weight`。

### 4.3 Execution 下沉

07-B 不处理 T+1、集合竞价、停牌/涨跌停、整数手、成交价、手续费、订单或 Broker。

---

## 5. 实现形态

07-B 实现为 **System-B-local deterministic policy component**，不是独立 Product Strategy。

冻结：

1. 输出复用现有 `StrategyDecision`；
2. 不新增 Common `ADD` action；
3. 加仓仍使用 `StrategyAction.ENTER`，通过 reason/evidence 区分 `NEW` 与 `ADD`；
4. 不生成 `StrategyRunResult.portfolio_targets`；
5. 不改造 `system_b_basic`；
6. 不建设 Strategy Framework v2。

---

## 6. System-B-local 输入合同

### 6.1 为什么不能直接使用 Common strict validator

现有标准 `validate_strategy_input()` 会对 required fields / indicators 的 NA 直接 fail。07-B 又必须区分：

```text
NOT_TRIGGERED
vs
UNAVAILABLE
```

因此本任务冻结一个 **System-B-local prepared-input normalizer / immutable envelope**。

它只服务 System B，不注册成 Common validator plugin，不新增 `StrategyInputScope`。

逻辑形态：

```text
raw prepared facts
+ initial holdings
+ candidate asset ids
+ authorization
+ comparison-score provenance

→ normalize_system_b_decision_input(...)

→ immutable SystemBDecisionInput

→ evaluate_system_b_holding_entry_exit(...)
```

### 6.2 单日 envelope

一个 `SystemBDecisionInput` 只允许一个 canonical `trade_date`。

至少包含：

```text
trade_date
initial_holdings
candidate_asset_ids
authorization_status
comparison_score_provenance
asset_facts
```

输入资产域必须完整等于：

```text
initial_holding_asset_ids
UNION
candidate_asset_ids
```

每个 asset 必须恰好一条 normalized fact；不得 duplicate；不得因缺事实静默删除资产。

### 6.3 显式状态合同

#### Exit

```text
exit_status = TRIGGERED | NOT_TRIGGERED | UNAVAILABLE
```

映射规则：

```text
system_b_exit_triggered == True  → TRIGGERED
system_b_exit_triggered == False → NOT_TRIGGERED
上游事实缺失 / nullable unknown → UNAVAILABLE
```

禁止把缺失隐式转为 `False`。

整个必需字段列不存在属于输入 schema error；已知资产的某日退出事实无法确定，则必须规范化为 `UNAVAILABLE`。

#### Entry eligibility

```text
entry_eligibility_status = ELIGIBLE | INELIGIBLE | UNAVAILABLE
```

`UNAVAILABLE` 阻断该资产 NEW / ADD，不影响该资产既有持仓 EXIT。

#### Authorization

日级：

```text
authorization_status = AUTHORIZED | DENIED | UNAVAILABLE
```

`DENIED` / `UNAVAILABLE` 均阻断所有 NEW / ADD；不触发已有持仓 EXIT。

#### Severe abnormal supervision

```text
supervision_status = ACTIVE | INACTIVE | UNAVAILABLE
```

只有 `INACTIVE` 允许 NEW / ADD。

#### Comparison score

```text
comparison_score = finite float | None
```

`None` 唯一表示 unavailable；NaN / Inf 不允许进入 normalized envelope。

### 6.4 Current holdings

继续复用 Task07-A：

```python
StrategyInput.holdings: Mapping[str, StrategyHoldingState]
```

07-B 实际需要：

```text
asset_id
current_weight
entry_count
first_entry_date
last_entry_date
```

不新增 quantity / cost / cash / account / PnL。

---

## 7. Comparison-score provenance 合同

### 7.1 Run-level provenance

所有参与相对门槛或 top-score 判断的分数必须属于同一 provenance envelope，至少包含：

```text
trade_date
score_calculation_version
rule_version_set_id
parameter_set_id
input_snapshot_id
```

其中 `trade_date` 必须与 `SystemBDecisionInput.trade_date` 一致。

如果上游原始数据携带 row-level provenance，normalizer 必须验证参与比较的所有有效 score provenance 完全一致，再提升为 run-level provenance；不得混合后继续比较。

### 7.2 缺失或不一致

发生任一情况：

```text
provenance missing
provenance mismatch
score trade_date mismatch
```

则：

```text
所有依赖 comparison_score 的 NEW / ADD fail-closed
```

reason 至少区分：

```text
COMPARISON_SCORE_PROVENANCE_UNAVAILABLE
COMPARISON_SCORE_PROVENANCE_MISMATCH
```

HOLD / EXIT 仍独立执行。

### 7.3 审计

凡 decision 使用了 comparison score，evidence 至少保存：

```text
comparison_score
score_calculation_version
rule_version_set_id
parameter_set_id
input_snapshot_id
```

保证可重放地证明“为什么这些分数当日可比较”。

---

## 8. 固定决策顺序

```text
1. normalize + validate local input envelope
2. evaluate initial-holding EXIT
3. mark same-day EXIT assets terminal
4. build retained holdings
5. preserve HOLD / unresolved exposure state
6. evaluate entry-side global gates
7. evaluate NEW candidates against retained holdings
8. evaluate ADD candidates among retained holdings
9. emit deterministic decisions
```

EXIT 必须先于所有新增判断。

---

## 9. Initial Holdings / Retained Holdings / NEW 的严格定义

### 9.1 Initial holdings

```text
initial_holdings = 本交易日策略评估开始前的持仓集合
```

该集合决定资产是否属于“原持仓”。

### 9.2 Retained holdings

```text
retained_holdings
=
initial_holdings
-
当日 exit_status=TRIGGERED 的资产
```

retained holdings 用于：

- 当日相对评分门槛；
- ADD 判断；
- 07-C distinct-stock capacity 的业务输入。

### 9.3 NEW candidate

NEW 必须定义为：

```text
asset_id in candidate_asset_ids
AND asset_id not in initial_holdings
```

**不是** `asset_id not in retained_holdings`。

因此：

```text
initially held
+ today EXIT
→ remains an initial-holding asset identity for this trade_date
→ cannot become NEW on the same day
```

### 9.4 EXIT 当日终态

对同一资产：

```text
initially held
AND exit_status=TRIGGERED
→ EXIT
```

冻结：

- 当日不得 ADD；
- 当日不得重新 ENTER；
- 即使 `entry_eligible=ELIGIBLE`、authorization=AUTHORIZED、score 全市场最高，也只能输出 EXIT；
- EXIT 仍从 retained holdings 移除，因此可为**其他资产**释放 distinct-stock capacity。

---

## 10. Holding / Exit Decision

### 10.1 EXIT

```text
asset in initial_holdings
AND exit_status == TRIGGERED
→ EXIT
reason = MA5_TWO_ACTUAL_TRADING_DAYS_EXIT
```

07-B 不重新计算 MA5，消费 canonical exit fact。

### 10.2 HOLD

```text
asset in retained_holdings
AND exit_status == NOT_TRIGGERED
AND 未产生 ADD ENTER
→ HOLD
reason = POSITION_CONTINUES
```

评分/排名/M身份/市场授权变化不能单独覆盖 HOLD。

### 10.3 EXIT unavailable

```text
asset in initial_holdings
AND exit_status == UNAVAILABLE
→ NO_ACTION
reason = EXIT_STATUS_UNAVAILABLE
```

语义：

- 不确认 EXIT；
- 不把 unavailable 伪造为正常 HOLD；
- 当日不得 ADD；
- 07-C 必须保留当前 desired exposure；
- 该资产仍计入 retained holdings，防止虚假释放容量。

---

## 11. Entry-side global gates

任何增加风险敞口的 NEW / ADD 必须同时满足：

```text
authorization_status == AUTHORIZED
entry_eligibility_status == ELIGIBLE
supervision_status == INACTIVE
comparison_score available
comparison_score provenance valid
```

任一 gate 不满足即阻断 entry side。

建议 reason：

```text
NEW_POSITION_AUTHORIZATION_DENIED
NEW_POSITION_AUTHORIZATION_UNAVAILABLE
ENTRY_ELIGIBILITY_DENIED
ENTRY_ELIGIBILITY_UNAVAILABLE
SEVERE_ABNORMAL_SUPERVISION_BLOCKED
SEVERE_ABNORMAL_STATUS_UNAVAILABLE
COMPARISON_SCORE_UNAVAILABLE
COMPARISON_SCORE_PROVENANCE_UNAVAILABLE
COMPARISON_SCORE_PROVENANCE_MISMATCH
```

这些 gate 不产生 EXIT。

---

## 12. NEW Entry 相对评分门槛

仅对 §9.3 定义的 NEW candidate 执行。

### 12.1 retained = 0

无持仓比较门槛；global gates 通过即可成为 ENTER candidate。

```text
reason = NEW_ENTRY_ELIGIBLE_NO_HOLDING_THRESHOLD
```

### 12.2 retained = 1—3

所有 retained holdings 的有效 comparison score 必须齐全且 provenance 一致：

```text
candidate_score > min(retained_holding_scores)
```

严格 `>`，相等不通过。

### 12.3 retained = 4—5

```text
candidate_score > max(retained_holding_scores)
```

即严格高于全部 retained holdings；相等不通过。

### 12.4 retained >= 6

禁止新增不同股票：

```text
NO_ACTION
reason = DISTINCT_HOLDING_CAP_REACHED
```

### 12.5 Retained score coverage 不完整

retained=1—5 且任一 retained holding score / provenance 不可用：

```text
所有依赖相对门槛的 NEW fail-closed
reason = RETAINED_HOLDING_SCORE_UNAVAILABLE
```

不得只比较可用子集。

### 12.6 EXIT 释放容量示例

日初 6 只持仓，其中 1 只确认 EXIT：

```text
initial_holdings = 6
retained_holdings = 5
```

其他从未持有的新候选按 retained=5 的“严格高于全部 retained holdings”规则判断。

被 EXIT 的原持仓自身不得重新 NEW。

---

## 13. ADD / Second Entry

### 13.1 Action 表达

不新增 Common `ADD`：

```text
StrategyAction.ENTER
entry_kind = ADD
```

### 13.2 ADD 基础条件

```text
asset in retained_holdings
entry_count == 1
exit_status == NOT_TRIGGERED
authorization_status == AUTHORIZED
entry_eligibility_status == ELIGIBLE
supervision_status == INACTIVE
comparison_score available
comparison-score provenance valid
```

`entry_count >= 2` 不得再次 ENTER，只能 HOLD（若无 EXIT）。

### 13.3 最高评分层

entry-side qualified comparison universe：

```text
所有通过 global gates 的 NEW candidates
UNION
所有通过 ADD 基础条件的 retained holdings
```

```text
top_score = max(comparison_score)
```

可加仓持仓满足：

```text
holding_score == top_score
```

即可输出：

```text
ENTER
entry_kind = ADD
reason = ADD_ENTRY_ELIGIBLE_TOP_SCORE
```

### 13.4 同分

业务规则未批准技术性 tie-breaker。

多个资产同为 top score 时：

- 07-B 保留 co-top 事实；
- 不用 ticker 打破业务同分；
- 符合条件的 co-top holding 均可成为 ADD candidate；
- 最终容量冲突留给 07-C；
- 07-C 如必须二选一且仍无业务 tie-breaker，应显式 unresolved/fail-closed，不得静默按 ticker 排业务优先级。

---

## 14. 严重异动

当前 v1.1 自动范围只有：

```text
supervision ACTIVE
→ 禁止 NEW
→ 禁止 ADD
```

监管期本身不自动 EXIT。

I 类减仓 50%、II 类清仓当前仍属人工处置，不由 07-B 自动生成交易动作。

允许在 evidence 中保留：

```text
manual_handling_required
manual_handling_status
manual_handling_record_id
```

不借本任务建设 OMS / manual order system。

---

## 15. Decision 输出合同

每个 `trade_date + asset_id` 至多一个主 `StrategyDecision`。

### 15.1 Action

```text
EXIT      初始持仓确认正式退出
ENTER     NEW 或 ADD 候选
HOLD      retained holding 正常继续持有
NO_ACTION 未持有且不新增，或持仓输入不足而保守保持 exposure
```

### 15.2 score / weight

```text
StrategyDecision.score = comparison_score or None
StrategyDecision.weight = None
```

### 15.3 Evidence

至少保存：

```text
was_initially_held
was_retained
entry_count
exit_status
entry_eligibility_status
authorization_status
supervision_status
comparison_score
comparison_score_provenance_valid
score_calculation_version
rule_version_set_id
parameter_set_id
input_snapshot_id
retained_holding_count
relative_score_threshold
relative_score_passed
entry_kind = NEW | ADD | NONE
same_day_exit_terminal
```

---

## 16. Reason Code 基线

```text
MA5_TWO_ACTUAL_TRADING_DAYS_EXIT
POSITION_CONTINUES
EXIT_STATUS_UNAVAILABLE

NEW_ENTRY_ELIGIBLE_NO_HOLDING_THRESHOLD
NEW_ENTRY_ABOVE_MIN_HOLDING_SCORE
NEW_ENTRY_ABOVE_ALL_HOLDING_SCORES
NEW_ENTRY_SCORE_THRESHOLD_NOT_MET
DISTINCT_HOLDING_CAP_REACHED
SAME_DAY_EXIT_TERMINAL

ADD_ENTRY_ELIGIBLE_TOP_SCORE
ADD_ENTRY_NOT_TOP_SCORE
ADD_ENTRY_LIMIT_REACHED

NEW_POSITION_AUTHORIZATION_DENIED
NEW_POSITION_AUTHORIZATION_UNAVAILABLE
ENTRY_ELIGIBILITY_DENIED
ENTRY_ELIGIBILITY_UNAVAILABLE
SEVERE_ABNORMAL_SUPERVISION_BLOCKED
SEVERE_ABNORMAL_STATUS_UNAVAILABLE
COMPARISON_SCORE_UNAVAILABLE
COMPARISON_SCORE_PROVENANCE_UNAVAILABLE
COMPARISON_SCORE_PROVENANCE_MISMATCH
RETAINED_HOLDING_SCORE_UNAVAILABLE
```

实现命名可轻微调整，但不同业务原因不得被折叠。

---

## 17. Determinism

冻结：

- 单个 canonical `trade_date`；
- canonical `asset_id`；
- input domain 完整覆盖 initial holdings ∪ candidates；
- 同日同资产唯一；
- raw 输入顺序不影响结果；
- decisions 按 `trade_date ASC, asset_id ASC` 稳定输出；
- strict threshold 一律 `>`；
- NaN / Inf 不作为有效 score；
- 不用 ticker 破坏业务同分；
- 相同 normalized envelope 必须得到完全一致结果。

---

## 18. 与既有 `system_b_basic` 的关系

`system_b_basic@1.0.0` 继续作为 legacy/basic architecture fixture，不改造成完整 System B。

07-B 新增 System-B-local policy；07-C 的正式 System B portfolio strategy 再组合 Task05/06/07-B 输出并生成 native target。

---

## 19. 明确非范围

07-B 禁止实现：

- comparison-score 公式；
- 新评分权重 / 阈值；
- 1/8 target weight；
- 25% / 30% 最终组合计算；
- multi-ENTER 最终容量 resolution；
- full portfolio target；
- quantity / integer lot / cash feasibility；
- execution / order / broker；
- 严重异动自动减仓/清仓；
- Common validator plugin registry；
- 新 `StrategyInputScope`；
- Strategy Framework v2；
- Task08 / Task09。

---

## 20. 测试矩阵

### 20.1 EXIT / HOLD

1. held + TRIGGERED → EXIT；
2. held + NOT_TRIGGERED → HOLD；
3. held + UNAVAILABLE → NO_ACTION + preserve exposure；
4. score / rank / M身份下降不触发 EXIT；
5. authorization denied 不触发 EXIT；
6. supervision ACTIVE 本身不触发 EXIT；
7. supervision ACTIVE + exit TRIGGERED → EXIT。

### 20.2 Same-day EXIT terminal

1. `held + exit=TRIGGERED + entry eligible + authorization + supervision inactive + highest score` → **只输出 EXIT**；
2. 上述资产不得出现 NEW 或 ADD；
3. 6 只初始持仓退出 1 只后，其他新资产按 retained=5 判断；
4. 被退出资产即使仍在 candidate set，也不得同日重新 ENTER。

### 20.3 NEW threshold

1. retained=0 → 无相对门槛；
2. retained=1 → `candidate > holding` passes，`==` fails；
3. retained=3 → 与最低分比较；
4. retained=4/5 → 严格高于最高分；
5. retained>=6 → 新不同股票阻断；
6. retained score coverage 不完整 → NEW fail-closed。

### 20.4 ADD

1. entry_count=1 + top qualified score → ENTER/ADD；
2. entry_count=2 → HOLD；
3. top score 为新候选 → 非 top held 不 ADD；
4. authorization denied/unavailable → 不 ADD；
5. supervision ACTIVE/UNAVAILABLE → 不 ADD；
6. exit UNAVAILABLE → 不 ADD；
7. co-top 不用 ticker tie-break。

### 20.5 Local input normalization

1. exit bool True → TRIGGERED；
2. exit bool False → NOT_TRIGGERED；
3. exit fact unknown → UNAVAILABLE；
4. unknown 不得隐式转 False；
5. invalid enum → fail；
6. candidate / holding domain coverage 缺失 → fail；
7. duplicate asset row → fail；
8. comparison score NaN/Inf → unavailable/fail-normalization，不得作为有效值。

### 20.6 Provenance

1. 同日同 version/snapshot score 可比较；
2. provenance missing → NEW/ADD blocked，HOLD/EXIT unaffected；
3. mixed `score_calculation_version` → NEW/ADD blocked；
4. mixed `input_snapshot_id` → NEW/ADD blocked；
5. trade_date mismatch → NEW/ADD blocked；
6. decision evidence 保存 canonical provenance。

### 20.7 Regression

必须保证：

- Task05 authorization；
- Task06 Asset/Theme Rank；
- Task07-A contract / checked runner；
- `system_b_basic` legacy；
- full regression。

---

## 21. Definition of Done

Task07-B 完成必须满足：

1. System-B-local Holding / Entry / Exit policy 已实现；
2. local input normalizer / immutable envelope 已实现，未扩张 Common Framework；
3. exit/eligibility/authorization/supervision 的 unavailable 语义显式可执行；
4. `system_b_exit_triggered` bool 正确映射到 exit tri-state；
5. input domain 完整覆盖 initial holdings ∪ candidate universe；
6. **同日 EXIT 为该资产终态，不得重新 ENTER/ADD**；
7. EXIT-first + retained-holdings 语义有测试；
8. 0 / 1—3 / 4—5 / 6 相对门槛实现；
9. ADD 最高评分层与 max-two-buy 规则实现；
10. comparison score provenance 缺失/混用 fail-closed；
11. score/provenance 问题不影响 HOLD / EXIT；
12. 不修改 comparison-score 算法；
13. 不生成 target weight / portfolio target；
14. `system_b_basic` 行为不被隐式改写；
15. targeted + full regression 通过。

---

## 22. Task07-C 接口预期

Task07-C 消费：

```text
StrategyInput.holdings
+ Task07-B StrategyDecision
+ normalized comparison score / provenance
```

并把：

```text
EXIT
HOLD
ENTER(new)
ENTER(add)
```

解析成完整 `StrategyPortfolioTarget`。

07-C 必须继续遵守：

- EXIT first；
- same-day EXIT terminal；
- 不替换 principle；
- 1/8 increment；
- max two buys；
- planned 25%；
- hard max <=30%；
- <=6 distinct stocks；
- deterministic conflict resolution；
- full snapshot authority。

---

## 23. 当前唯一业务悬而未决项

```text
comparison_score 的具体计算公式
```

07-B 通过明确的 score + provenance 输入合同隔离该未决项。

因此：

> **07-B 可以使用测试桩 / prepared upstream score 完整实现和验证；真实生产路径在 approved score model 缺失时必须阻断 NEW / ADD，但 Holding / Exit 仍可确定运行。**
