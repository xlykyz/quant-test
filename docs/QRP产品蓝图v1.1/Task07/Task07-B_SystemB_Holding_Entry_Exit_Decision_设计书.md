# Task07-B — System B Holding / Entry / Exit Decision 设计书

> 状态：DESIGN DRAFT 1
>
> 分支基线：`develop/v1.1 @ ba302ae343ce95d52633d84456318238c572fa99`
>
> 前置：Task07-A 已完成并合并；typed holdings、checked runner、native full-snapshot portfolio target 与 canonical `StrategyRunResult` 已可用。
>
> 任务身份：Task07 的 System B 业务核心工作包之一。07-B 只冻结并实现 **持有 / 新增 / 加仓 / 退出的业务判断**；最终仓位、容量竞争、资金约束与完整 Portfolio Target 由 Task07-C 负责。

---

## 1. 任务目标

Task07-B 回答一个问题：

> **在给定当日市场授权、候选资格、统一比较分、严重异动状态和当前持仓的前提下，System B 对每只股票业务上应当 HOLD、ENTER、EXIT 还是 NO_ACTION？**

本任务必须把以下已批准规则转成确定性策略判断：

- 已有持仓每日继续评分，但评分、排名、M 身份和判断层变化本身不触发卖出；
- 连续两个实际交易日收盘低于 MA5 → 全部退出该标的剩余持仓；
- 严重异动监管期禁止开仓和加仓，但不因“处于监管期”本身自动清仓；
- 新候选相对已有持仓存在严格评分门槛；
- 已有持仓若仍为合格标的且处于最高评分层，可成为一次加仓候选；
- 同一股票最多买入两次；
- 同日先确认 EXIT，再用 retained holdings 参与新增判断；
- 评分只控制新增资金，不替换、不挤出、不强制卖出已有持仓。

07-B 不计算最终目标权重，也不处理实际成交可实现性。

---

## 2. 业务规则来源与优先级

System B 业务 SSOT 仍为：

```text
xlykyz/MyTradingSystem
source_commit = 82369650d16914e42c03da7635f410b12a38220e
source_document = docs/15_交易系统2.0(初稿).md
```

qrp-atlas 只做工程映射，不重新解释业务规则。

Task07-B 使用以下已确认规则：

1. 每日收盘后，候选与已有持仓使用同一评分口径；
2. 评分下降、排名下降、失去 M1/M2/M3、题材授权变化、市场阶段变化、V 规则撤销新增仓授权，均不触发已有持仓卖出；
3. 连续两个实际交易日收盘低于 MA5 才确认全部退出；
4. 严重异动监管期禁止开仓与加仓；当前自动执行范围不包含严重异动减仓/清仓；
5. 新候选相对评分门槛：
   - retained holdings = 0：不与持仓比较；
   - retained holdings = 1—3：候选分数必须严格高于 retained holdings 最低分；
   - retained holdings = 4—5：候选分数必须严格高于 retained holdings 全部分数，即严格高于最高分；
   - retained holdings = 6：禁止新增不同股票；
6. 高分新候选不得替换已有持仓；
7. 当最高评分的合格标的是已有持仓，且该票仍允许加仓时，可作为第二次买入候选；
8. 同一股票最多买入两次。

---

## 3. `comparison_score` 暂存假设（本任务明确保留）

### 3.1 当前事实

Task06 已完成 Asset Rank / Theme Rank 等结果能力，但 System B 的最终统一综合评分公式、权重和 M1—M3 映射仍未批准。

因此 Task07-B **不得**：

- 发明 M1/M2/M3 综合公式；
- 设默认权重；
- 用 rank 代替 score；
- 用 M 身份映射出隐藏分数；
- 将缺失分数静默视为 0。

### 3.2 本任务输入假设

Task07-B 暂时消费一个上游已解析字段：

```text
comparison_score: finite numeric | unavailable
```

其语义仅为：

> 同一交易日、同一 System B 规则版本下，对候选与已有持仓使用同一口径生成、可直接进行严格大小比较的统一分数。

07-B 只消费，不解释其算法。

### 3.3 Fail-closed

只要新增 / 加仓判断所需的比较分无法完整确定：

```text
entry side = blocked
reason = COMPARISON_SCORE_UNAVAILABLE
```

但：

- 已有持仓的 MA5 EXIT 仍必须正常判断；
- 已有持仓若没有退出触发，不得因为 score 缺失自动卖出；
- score 缺失不得改写历史持仓状态。

---

## 4. Task07-B 与 07-C 的边界

### 4.1 07-B 负责“该不该”

07-B 负责：

```text
current holdings
+ exit facts
+ authorization
+ eligibility / veto
+ comparison_score
+ severe-abnormal supervision

→ per-asset Holding / Entry / Exit Decision
```

输出只表达业务判断：

```text
ENTER
HOLD
EXIT
NO_ACTION
```

### 4.2 07-C 负责“最终多少、最终选谁”

07-C 才负责：

- 每次 1/8 总资产；
- 第一次 / 第二次买入的最终 target weight；
- 计划 25%；
- 单票 hard max <= 30%；
- 多个 ENTER 候选同时竞争容量时的确定性 resolution；
- 最多 6 只股票的最终组合约束；
- 现金不足；
- 组合级优先级；
- 完整 `StrategyPortfolioTarget` full snapshot。

07-B **不得生成最终 target_weight**。

### 4.3 Execution 仍在下游

以下仍不属于 07-B：

- T+1 / 次日集合竞价；
- 停牌 / 涨跌停可成交性；
- 100 股整数手；
- 实际价格导致的现金可实现性；
- 手续费 / 滑点；
- OMS / broker / order / fill。

---

## 5. 实现形态：System B 私有策略组件，不独立注册成 Product Strategy

Task07-B 是 Task07-C 的业务判断组件，不应成为一个可被通用 legacy decisions adapter 独立执行的半成品策略。

冻结：

1. 07-B 实现为 **System B-local deterministic policy component**；
2. 其输出使用既有 `StrategyDecision`；
3. 07-B 本身不注册成独立 Product Strategy；
4. 07-B 不单独生成 `StrategyRunResult.portfolio_targets`；
5. Task07-C 的最终 System B strategy 调用 07-B，保留其 decisions，并生成 native full-snapshot portfolio target；
6. 不新增 Common `BusinessIntent` / `PortfolioIntent` / `ADD` action 类型。

这样避免：

```text
07-B intermediate ENTER/HOLD/EXIT
→ 被 generic legacy adapter 误认为完整组合
```

也避免为了一个 System B 工作包扩张 Strategy Framework。

---

## 6. 输入合同

### 6.1 评估单位

Task07-B 按一个 canonical `trade_date` 做单日决策。

输入资产域至少是：

```text
current holdings
UNION
当日 System B entry-side candidate universe
```

同一 `trade_date + asset_id` 必须唯一。

### 6.2 Current holdings

直接使用 Task07-A 已落地：

```python
StrategyInput.holdings: Mapping[str, StrategyHoldingState]
```

关键字段：

```text
asset_id
current_weight
entry_count
first_entry_date
last_entry_date
```

07-B 不新增 Account / quantity / cost / PnL 模型。

### 6.3 每资产 prepared facts

逻辑上至少需要：

```text
trade_date
asset_id
comparison_score
entry_eligible
exit_triggered
severe_abnormal_supervision_status
```

其中：

- `comparison_score`：上游已解析统一比较分；
- `entry_eligible`：上游 eligibility + hard veto 后的最终 entry-side 资格，不在 07-B 重算股票池/M身份；
- `exit_triggered`：canonical System B MA5 两日退出事实；
- `severe_abnormal_supervision_status`：至少能区分 `ACTIVE / INACTIVE / UNAVAILABLE`。

允许携带额外审计字段，但不得改变以上 authority。

### 6.4 日级 authorization

消费 Task05 已解析的新增仓授权：

```text
new_position_authorized: bool
```

及对应 reason / evidence。

冻结：

> **任何会增加风险敞口的操作——第一次买入和第二次加仓——都必须通过同一 entry-side authorization gate。**

判断层撤销授权只阻断 ENTER，不触发 EXIT。

### 6.5 严重异动状态未知

如果候选 / 可加仓持仓的严重异动监管状态无法确定：

```text
不得 ENTER
reason = SEVERE_ABNORMAL_STATUS_UNAVAILABLE
```

对于已有持仓：

- 不因状态未知自动卖出；
- MA5 EXIT 仍独立生效；
- 必须保留 diagnostics / evidence，供人工审查。

---

## 7. 固定决策顺序

Task07-B 单日计算顺序冻结为：

```text
1. validate input
2. evaluate current-holding EXIT
3. build retained holdings
4. preserve HOLD / unresolved-holding state
5. evaluate entry-side global gates
6. evaluate new-stock relative score threshold
7. evaluate existing-stock ADD eligibility
8. emit deterministic per-asset decisions
```

**EXIT 必须先于所有新增判断。**

原因：同日已确认退出的股票不得继续占用：

- retained holding count；
- relative-score comparison set；
- 后续 07-C 的 distinct-stock capacity。

但退出只释放“业务目标容量”；现实成交是否成功仍由下游 Portfolio / Backtest 处理。

---

## 8. 已有持仓决策

### 8.1 EXIT authority

07-B 不自行从价格重新计算 MA5。

应优先复用既有 canonical fact：

```text
system_b_exit_triggered
```

冻结：

```text
held
AND exit_triggered == true
→ EXIT
reason = MA5_TWO_ACTUAL_TRADING_DAYS_EXIT
```

退出为全部剩余持仓，具体 target=0 由 07-C full target 表达。

### 8.2 HOLD

若：

```text
held
AND exit_triggered == false
AND 未产生 ADD decision
```

则：

```text
HOLD
```

以下变化不能覆盖 HOLD：

- comparison score 下降；
- rank 下降；
- M1/M2/M3 身份消失；
- 题材失去主线身份；
- phase B → A/C；
- V rule 撤销新增仓授权；
- 新候选分数更高。

### 8.3 Exit fact unavailable

如果 held asset 的 exit fact 无法确定：

```text
NO_ACTION
reason = EXIT_STATUS_UNAVAILABLE
```

语义为：

- 不确认退出；
- 不确认正常 HOLD；
- 07-C 必须保留当前 desired exposure，不得因缺数据清零；
- 该票当日不得加仓；
- 记录 diagnostics。

该路径属于 fail-closed / no-fabrication，不得把 unavailable 当作 `exit_triggered=false`。

---

## 9. Retained Holdings

定义：

```text
retained_holdings
=
initial holdings
-
当日已确认 EXIT 的 holdings
```

后续全部 entry comparison 都使用 retained holdings，而不是日初 holdings。

冻结：

- 被 EXIT 的资产不参与新候选门槛；
- 被 EXIT 的资产当日不得同时 ADD；
- identity / score 变化本身不从 retained holdings 中移除资产；
- `EXIT_STATUS_UNAVAILABLE` 的资产仍视为 retained，以避免虚假释放容量。

---

## 10. New Entry 决策

“新候选”定义：

```text
asset_id not in retained_holdings
AND entry_eligible == true
```

### 10.1 Global gates

任一条件不满足即不得 ENTER：

```text
new_position_authorized == true
entry_eligible == true
severe_abnormal_supervision_status == INACTIVE
comparison_score available
```

其中 hard veto 已包含在 `entry_eligible` authority 中，高分不得覆盖 veto。

### 10.2 Retained holdings = 0

无相对持仓门槛。

满足 global gates 的新候选：

```text
ENTER
reason = NEW_ENTRY_ELIGIBLE_NO_HOLDING_THRESHOLD
```

多个候选同时通过时，07-B 不擅自只留一个；07-C 处理最终组合容量。

### 10.3 Retained holdings = 1—3

要求全部 retained holdings 的 `comparison_score` 可用。

门槛：

```text
candidate_score > min(retained_holding_scores)
```

严格 `>`；相等视为不通过。

通过：

```text
ENTER
reason = NEW_ENTRY_ABOVE_MIN_HOLDING_SCORE
```

否则 `NO_ACTION`。

### 10.4 Retained holdings = 4—5

要求全部 retained holdings 的 `comparison_score` 可用。

门槛：

```text
candidate_score > max(retained_holding_scores)
```

即严格高于全部已有持仓；相等不通过。

通过：

```text
ENTER
reason = NEW_ENTRY_ABOVE_ALL_HOLDING_SCORES
```

### 10.5 Retained holdings >= 6

禁止新增不同股票：

```text
NO_ACTION
reason = DISTINCT_HOLDING_CAP_REACHED
```

注意：该规则只阻断“新增不同股票”，不阻断对已有持仓的第二次买入候选。

### 10.6 Retained score coverage 不完整

当 retained holdings = 1—5 且任一 retained holding 缺少 comparison score：

```text
所有 NEW ENTRY fail-closed
reason = RETAINED_HOLDING_SCORE_UNAVAILABLE
```

不得只比较有分数的持仓子集。

---

## 11. Add / Second Entry 决策

### 11.1 不新增 Common `ADD` Action

07-B 继续使用：

```text
StrategyAction.ENTER
```

区分方式：

```text
asset already in retained_holdings
+ reason_code = ADD_ENTRY_ELIGIBLE_TOP_SCORE
```

07-C 可通过 holdings + `entry_count` 确定这是第二次买入。

### 11.2 基础条件

已有持仓只有同时满足以下条件才进入 ADD 评估：

```text
asset in retained_holdings
entry_count == 1
entry_eligible == true
new_position_authorized == true
severe_abnormal_supervision_status == INACTIVE
exit status confirmed false
comparison_score available
```

`entry_count >= 2`：

```text
不得再次 ENTER
```

但正常继续 HOLD。

### 11.3 最高评分规则

MyTradingSystem 当前规则为：

> 当评分最高的合格标的是已有持仓股票时，可以再次执行一次固定 1/8 买入。

07-B 定义 entry-side qualified comparison universe：

```text
所有满足 authorization / eligibility / veto / supervision / score-ready 的
new candidates
UNION
可加仓 retained holdings
```

计算：

```text
top_score = max(comparison_score)
```

若可加仓持仓：

```text
holding_score == top_score
```

则：

```text
ENTER
reason = ADD_ENTRY_ELIGIBLE_TOP_SCORE
```

否则维持 `HOLD`。

### 11.4 同分

当前业务规则未给出“最高分同分时只允许哪一只”的进一步裁决。

因此 07-B **不得用 ticker 等技术字段伪造业务 tie-breaker**。

如果多只资产同为最高分：

- 07-B 保留同分业务事实；
- 符合加仓基础条件的 co-top holding 均可输出 ENTER candidate；
- 最终是否同时进入目标组合、如何占用组合容量，交给 07-C 的 deterministic constraint resolution；
- 07-C 若发现必须在同分候选中二选一而业务规则仍未批准，不得静默使用 ticker 作为业务优先级，应显式 fail-closed 或记录 unresolved tie。

---

## 12. 严重异动

### 12.1 自动化边界

当前 qrp-atlas v1.1 只自动实现：

```text
监管期 ACTIVE
→ 禁止开仓
→ 禁止加仓
```

不自动实现：

- I 类严重异动自动减仓 50%；
- II 类严重异动自动清仓；
- 任何等价自动执行。

上述自动交易仍属于明确暂缓范围。

### 12.2 已有持仓

监管期本身：

```text
held + supervision ACTIVE
→ 不新增风险敞口
→ 不因此自动 EXIT
```

MA5 EXIT 若同时触发，仍由 MA5 EXIT authority 正常产生 EXIT。

### 12.3 Manual handling audit

07-B 必须允许把上游严重异动人工处置状态/引用保留进 decision evidence，例如：

```text
severe_abnormal_supervision_status
manual_handling_required
manual_handling_status
manual_handling_record_id
```

若当前工程尚无正式人工处置持久化对象：

- 07-B 只保留最小审计 hook / evidence；
- 不借本任务建设 OMS / manual order system；
- 不用“缺 record”解释为“已完成处置”。

---

## 13. Decision 输出合同

07-B 对输入域每个 `trade_date + asset_id` 至多输出一个主 `StrategyDecision`。

### 13.1 Action

```text
EXIT      已有持仓确认触发正式退出
ENTER     新股票新增候选，或已有持仓第二次买入候选
HOLD      已有持仓继续持有且当日不增加敞口
NO_ACTION 未持有且不应新增，或输入不足导致 fail-closed
```

### 13.2 `score`

```text
StrategyDecision.score = comparison_score
```

若 unavailable：`None`。

不得将 Asset Rank / M identity 偷换为该字段。

### 13.3 `weight`

07-B：

```text
StrategyDecision.weight = None
```

最终 weight 只由 07-C native portfolio target 决定。

### 13.4 Evidence

至少保留能够解释决策的事实子集：

```text
was_held
entry_count
exit_triggered
entry_eligible
new_position_authorized
severe_abnormal_supervision_status
comparison_score
retained_holding_count
relative_score_threshold
relative_score_passed
entry_kind = NEW | ADD | NONE
```

Evidence 仅记录已存在事实和已执行规则，不重新计算上游 score / rank / identity。

---

## 14. Reason Code 基线

建议冻结以下 System B reason codes：

```text
MA5_TWO_ACTUAL_TRADING_DAYS_EXIT
POSITION_CONTINUES
EXIT_STATUS_UNAVAILABLE

NEW_ENTRY_ELIGIBLE_NO_HOLDING_THRESHOLD
NEW_ENTRY_ABOVE_MIN_HOLDING_SCORE
NEW_ENTRY_ABOVE_ALL_HOLDING_SCORES
NEW_ENTRY_SCORE_THRESHOLD_NOT_MET
DISTINCT_HOLDING_CAP_REACHED

ADD_ENTRY_ELIGIBLE_TOP_SCORE
ADD_ENTRY_NOT_TOP_SCORE
ADD_ENTRY_LIMIT_REACHED

NEW_POSITION_AUTHORIZATION_DENIED
ENTRY_ELIGIBILITY_DENIED
SEVERE_ABNORMAL_SUPERVISION_BLOCKED
SEVERE_ABNORMAL_STATUS_UNAVAILABLE
COMPARISON_SCORE_UNAVAILABLE
RETAINED_HOLDING_SCORE_UNAVAILABLE
```

实现允许按现有命名规范轻微调整，但不得合并掉不同业务原因。

---

## 15. Determinism

冻结：

- canonical `trade_date`；
- canonical `asset_id`；
- 同日同资产输入唯一；
- 输入排序不得影响决策；
- 最终 decisions 按 `trade_date ASC, asset_id ASC` 稳定输出；
- score 比较使用原值，不使用 ticker 破坏业务同分；
- 严格门槛均使用 `>`，不是 `>=`；
- 不允许 NaN / Inf 作为有效 comparison score；
- 相同输入 + holdings + rule/config version 必须得到完全一致结果。

---

## 16. 与既有 `system_b_basic` 的关系

当前 `system_b_basic@1.0.0` 是早期架构验证策略，只基于：

```text
system_b_trend_valid
system_b_exit_triggered
```

直接生成简单 ENTER/HOLD/EXIT。

Task07-B 不应把完整 System B 规则硬塞回该验证策略并改变其既有兼容语义。

建议：

- 保留 `system_b_basic` 作为 legacy/basic architecture fixture；
- 新增 System B-local holding/entry/exit policy component；
- Task07-C 再由正式 System B portfolio strategy 组合 Task05/06/07-B 结果并生成 native target。

---

## 17. 明确非范围

Task07-B 禁止实现：

- `comparison_score` 综合公式；
- M1/M2/M3 新权重或新阈值；
- 新评分模型；
- 1/8 最终 target weight；
- 25% / 30% 最终组合计算；
- 多 ENTER 候选最终容量排序；
- full portfolio target；
- quantity / 100 股整数手；
- cash feasibility；
- next-open execution；
- suspension / limit execution rules；
- 严重异动自动减仓/清仓；
- Account / OMS / order / broker；
- Strategy Framework v2；
- 新 Common `ADD` action；
- Task08 replay / Task09 daily product。

---

## 18. 测试矩阵

### 18.1 Holding / Exit

至少覆盖：

1. held + exit=true → EXIT；
2. held + exit=false → HOLD；
3. held + exit unavailable → NO_ACTION + preserve exposure semantics；
4. score下降不触发 EXIT；
5. M身份变化不触发 EXIT；
6. authorization撤销不触发 EXIT；
7. severe supervision ACTIVE 不因监管期本身自动 EXIT；
8. severe supervision ACTIVE + MA5 exit=true → EXIT。

### 18.2 New Entry Threshold

1. retained=0 → 无相对门槛；
2. retained=1 → candidate > min passes；
3. retained=1 → candidate == holding fails；
4. retained=3 → 与最低分比较；
5. retained=4 → 必须严格高于最高分；
6. retained=5 → 必须严格高于最高分；
7. retained=6 → 新不同股票被阻断；
8. 当日先 EXIT 使 6→5 后，新候选按 retained=5 规则判断；
9. retained score 缺失 → 所有依赖相对门槛的 new entry fail-closed。

### 18.3 Add

1. held entry_count=1 + top qualified score → ENTER / ADD；
2. held entry_count=2 → HOLD，不再加仓；
3. top score 为新候选 → held 不加仓；
4. authorization=false → 不加仓；
5. supervision ACTIVE → 不加仓；
6. supervision unavailable → 不加仓；
7. exit=true → EXIT 优先，绝不同时 ADD；
8. 6只 retained holdings 时，对已有持仓 ADD 仍可进入候选；
9. co-top holdings 不使用 ticker 伪业务 tie-breaker。

### 18.4 Missing / Invalid

1. comparison_score NaN/Inf → unavailable；
2. candidate score unavailable → candidate blocked；
3. relevant score coverage incomplete → entry side fail-closed；
4. duplicate trade_date+asset_id → fail；
5. holdings asset 缺少当日 required exit fact → explicit unavailable path；
6. invalid supervision status → fail。

### 18.5 Regression

必须保证：

- Task05 authorization regression；
- Task06 Asset/Theme Rank regression；
- Task07-A contract / checked runner regression；
- `system_b_basic` legacy tests；
- full regression。

---

## 19. Definition of Done

Task07-B 完成必须同时满足：

1. System B-local deterministic Holding / Entry / Exit policy 已实现；
2. 不修改 `comparison_score` 业务算法；
3. EXIT-first / retained-holdings 语义明确且有测试；
4. 0 / 1—3 / 4—5 / 6 持仓相对评分门槛全部实现；
5. 严格 `>` tie semantics 有测试；
6. 允许已有持仓在最高评分层成为第二次买入候选；
7. entry_count >= 2 不得继续加仓；
8. authorization / eligibility / severe-abnormal supervision 能阻断 NEW 与 ADD；
9. 上述 gate 不会误伤已有持仓 HOLD / MA5 EXIT；
10. score / supervision / exit 输入不足均显式 fail-closed，不伪造事实；
11. 不增加 Common `ADD` action，不扩大 Strategy Framework；
12. 07-B 不生成最终 target weight / portfolio target；
13. legacy `system_b_basic` 行为不被隐式改写；
14. targeted regression + full regression 通过。

---

## 20. Task07-C 接口预期

Task07-C 将消费：

```text
StrategyInput.holdings
+ Task07-B per-asset StrategyDecision
+ comparison_score / priority evidence
```

并负责把：

```text
EXIT
HOLD
ENTER(new)
ENTER(add)
```

确定性解析成：

```text
StrategyPortfolioTarget(full snapshot)
```

07-C 必须继续遵守：

- EXIT first；
- 不替换 principle；
- 1/8 entry increment；
- max two buys；
- planned 25%；
- hard max <=30%；
- <=6 distinct stocks；
- multi-candidate conflict resolution；
- full snapshot authority。

---

## 21. 当前唯一显式悬而未决项

Task07-B 不再绕开比较接口，但仍保留以下正式未冻结项：

```text
comparison_score 的具体计算公式
```

本设计通过明确输入合同隔离该未决项。

因此：

> **Task07-B 可以在 comparison_score 由测试桩 / prepared upstream value 提供的条件下完整实现和验证；在真实生产路径没有 approved score model 时，entry-side 必须继续 fail-closed。**

这不会阻塞 Holding / Exit 规则、相对门槛机制、加仓机制和后续 07-C 组合约束的工程实现。
