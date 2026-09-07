# Task07-A — System B Portfolio Target Contract Integration 设计书

> 状态：DESIGN DRAFT / 待对抗审计
>
> 分支基线：`develop/v1.1`
>
> 任务身份：Task07 的 enabling work package。Task07 仍以 **System B 业务闭环** 为唯一主目标；本任务只补齐 System B 为输出完整目标组合而实际暴露出的最小 QRP Common 能力缺口。

---

## 1. 背景与任务定位

QRP v1.1 当前核心链路已经收敛为：

```text
pipeline
→ contracts
→ stock_collections
→ indicators
→ strategies
→ portfolio target
→ production run / replay / result
```

Task05 已经完成 System B 新增仓授权结果的正式挂载；Task06 已经完成 Asset Rank 与 Theme Rank。进入 Task07 后，System B 首次需要把市场判断、横截面排名、当前持仓状态与组合规则汇合成一个**完整目标组合**。

现有 Strategy Framework 已具备：

- `StrategyDefinition` / `StrategyProtocol` / `StrategyRegistry`；
- ASSET / MARKET 两种 `StrategyInputScope`；
- 参数、required fields、required indicators、indicator requests；
- `StrategyDecision`；
- `StrategyAuthorization`；
- `StrategyRunResult`；
- declarative strategy；
- strategy catalog / product backtest；
- `StrategyDecision -> target_weights -> PortfolioBacktestEngine` 的通用适配链。

但对于 System B 这类复杂策略，当前仍存在一个关键断点：

```text
StrategyRunResult
  ├─ decisions
  └─ authorizations

        ↓  缺少策略原生、typed、完整的 portfolio target 结果

Portfolio / Backtest
```

简单 ENTER/HOLD/EXIT 策略可以由下游 Adapter 推导 target；System B 的最终组合却包含持仓生命周期、加仓次数、容量竞争、权重约束等业务语义，因此 **Portfolio Target 本身属于 System B 策略结果的一部分**，不能依赖通用 Adapter 在策略外部猜测。

Task07-A 的作用，就是为后续 Task07-B / 07-C 提供这个最小、稳定的输入输出边界。

---

## 2. Task07 总原则（冻结）

### 2.1 唯一主目标

> **Task07 以 System B 业务闭环为唯一主目标；为实现该目标暴露出的 QRP Common 能力缺口，仅进行最小、通用、向后兼容扩展，不将 Task07 扩张为独立的 Strategy Framework 重构任务。**

该原则对 Task07-A / B / C 全部生效。

### 2.2 “被动补齐 Common”，不是“主动建设平台”

Task07 不以“做一个万能策略平台”为目标。

正确顺序：

```text
System B 业务需求
→ 暴露 Common 缺口
→ 补最薄、可复用的一层
→ 返回 System B 主线
```

禁止顺序：

```text
发现一个抽象机会
→ 先设计 Strategy Framework v2
→ 扩建 plugin / external / account / OMS 等能力
→ 再回来实现 System B
```

### 2.3 最小、通用、向后兼容

任何 Common 扩展必须同时满足：

1. **最小**：只覆盖 Task07 已经真实需要的语义；
2. **通用**：命名与类型不得硬编码 System B 业务知识；
3. **向后兼容**：现有 built-in / declarative strategy、现有 `StrategyDecision` 与现有回测产品路径默认不受破坏。

### 2.4 策略语义与执行语义严格分层

Task07 的稳定边界停在：

```text
strategy result / portfolio target
```

不得重新把已退出 v1.1 Core 的 Execution / OMS / broker order planning 拉回 Task07。

通用 Portfolio / Backtest 可以模拟：

- T+1；
- 停牌；
- 涨跌停；
- 整数手；
- 成交成本；
- 现实成交失败；
- 持仓资金变化。

System B 策略负责：

- 谁应该持有；
- 是否继续持有；
- 是否退出；
- 是否允许新增 / 加仓；
- 组合容量竞争；
- 业务目标权重 / 目标状态。

**通用 Portfolio Engine 不得包含 System B、Theme Rank、Asset Rank、MA5 两日退出、第二次加仓等业务知识。**

---

## 3. Task07 三包关系

Task07 继续采用三包结构：

```text
07-A：System B Portfolio Target Contract Integration
     定义“输入输出长什么样”

             ↓

07-B：System B Holding / Entry / Exit Policy
     决定“业务上想持有什么”

             ↓

07-C：System B Portfolio Constraint Resolution & Final Target
     决定“约束后最终持有什么”
```

Task07-A 是薄 enabling task，不承担完整组合业务规则实现。

预期工程重量原则：

```text
07-A：薄
07-B：主业务核心
07-C：组合收口核心
```

---

## 4. Task07-A 目标

Task07-A 仅完成以下目标：

1. 给 QRP Strategy Result 增加一个可表达**完整目标组合**的 typed contract；
2. 给 System B 策略提供后续 07-B / 07-C 所需的**最小持仓状态输入**；
3. 明确 Strategy Portfolio Target 与现有 `StrategyDecision -> target_weights` Adapter 的兼容关系；
4. 保持现有简单策略与 declarative strategy 行为不变；
5. 不提前实现 System B 的完整 entry / hold / exit / sizing / constraint policy。

Task07-A 完成后，应允许后续 System B 策略在统一 `StrategyRunResult` 中原生返回完整 Portfolio Target，而不是要求 Portfolio Adapter 从增量动作反推。

---

## 5. 现有能力基线

### 5.1 Strategy Common 已有能力

当前 `StrategyDefinition` 已表达：

- code / name / version；
- strategy type；
- input scope；
- required fields；
- required indicators；
- indicator requests；
- parameter schema。

当前 `StrategyInput` 已表达：

```text
prepared_data
parameters
initial_positions: Mapping[str, bool]
runtime_context
```

当前 `StrategyRunResult` 已表达：

```text
definition
parameters
decisions
authorizations
diagnostics
```

### 5.2 Portfolio / Backtest 已有能力

当前已有通用：

```text
StrategyDecision
→ strategy_decisions_to_target_weights()
→ target_weight snapshots
→ PortfolioBacktestEngine
```

并支持：

- ENTER / HOLD / EXIT；
- rank / score priority；
- max positions；
- max weight；
- equal weight；
- cash buffer；
- zero target for exited holdings；
- full target snapshots。

因此 Task07-A **不得重新实现 Portfolio Engine 或第二套 target-weight adapter**。

---

## 6. 需要冻结的最小 Common Contract

> 本节定义的是设计目标。具体字段命名允许在对抗审计后做最小调整，但不得改变边界与职责。

### 6.1 Portfolio Target 必须成为 StrategyRunResult 的一等结果

建议最小新增：

```python
@dataclass(frozen=True)
class StrategyPortfolioTargetPosition:
    asset_id: str
    target_weight: float
    reason_code: str | None = None
    evidence: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class StrategyPortfolioTarget:
    trade_date: str
    strategy_code: str
    strategy_version: str
    positions: tuple[StrategyPortfolioTargetPosition, ...]
    diagnostics: tuple[str, ...] = ()
```

并在 `StrategyRunResult` 中增加：

```python
portfolio_targets: tuple[StrategyPortfolioTarget, ...] = ()
```

#### 必须满足的语义

- 一个 target 描述某一 `trade_date` 的**完整目标组合状态**；
- `positions` 代表目标组合，而不是 BUY / SELL 增量动作；
- 同一 target 内 `asset_id` 唯一；
- `target_weight >= 0`；
- 总目标权重不得超过 1（允许残余 cash）；
- 已退出资产是否需要显式 `target_weight=0`，由兼容层 / 持久化层设计审计确定；**不得同时出现“完整快照”与“增量 patch”双重语义**；
- 结果必须 deterministic serialization；
- Strategy Target 不记录 broker order、fill、slippage 等 execution facts。

### 6.2 权威字段：v1.1 优先采用 target_weight

Task07-A 默认建议：

> **Strategy Portfolio Target 的业务权威采用 `target_weight`，不在 Common Strategy Contract 中同时引入 target_quantity。**

原因：

- QRP 当前 Portfolio Engine 已以 `target_weight` 为正式输入；
- quantity / integer lot / price-dependent cash feasibility 属于 downstream portfolio realization / backtest execution；
- 同时把 `target_weight` 与 `target_quantity` 设为权威会制造双 SSOT；
- System B 的 1/8、25%、30% 等规则天然是组合比例语义。

若对抗审计发现原 Task07 规则明确要求 Strategy 层直接产出 share quantity，必须给出规则来源和不可下沉理由；否则禁止在 07-A 提前扩张。

### 6.3 Portfolio Target 不替代 StrategyDecision / Authorization

三类结果承担不同职责：

```text
StrategyAuthorization
= 市场级 / 策略级许可或否决

StrategyDecision
= 资产级判断与解释

StrategyPortfolioTarget
= 组合级最终目标状态
```

Task07-A 不删除、不重解释已有字段。

简单策略可继续只输出 `StrategyDecision`，由现有 Adapter 生成 target；复杂策略可以直接输出 `StrategyPortfolioTarget`。

---

## 7. 最小持仓状态输入

### 7.1 当前 bool initial_positions 不足

System B Task07 后续至少需要区分：

```text
未持有
已持有且只建仓一次
已持有且已完成第二次加仓
```

因此 `Mapping[str, bool]` 无法完整表达 System B 的持仓业务状态。

### 7.2 设计原则

不得借此建设完整 Account / Broker Position Model。

Task07-A 只需要提供一个**策略可消费的、typed、最小 holdings snapshot**。

建议方向：

```python
@dataclass(frozen=True)
class StrategyHoldingState:
    asset_id: str
    current_weight: float
    entry_count: int
    first_entry_date: str | None = None
    last_entry_date: str | None = None
```

并通过 `StrategyInput` 增加类似：

```python
holdings: Mapping[str, StrategyHoldingState] = field(default_factory=dict)
```

### 7.3 字段准入原则

仅允许 Task07-B / C 已明确需要的状态进入 Common typed model。

默认不进入 07-A：

- broker account id；
- order id；
- fill id；
- available cash；
- realized/unrealized PnL；
- average broker cost；
- frozen quantity；
- multi-currency；
- margin；
- arbitrary OMS state。

如后续发现某字段是 System B 组合判断不可缺少的事实，应在对抗审计中给出业务规则证据后再加入。

### 7.4 向后兼容策略

`initial_positions: Mapping[str, bool]` 当前已被现有策略使用。

07-A 不得直接破坏它。

优先方案：

- 保留 `initial_positions`；
- 新增 typed `holdings`；
- 对同时提供两者时定义确定性一致性校验；
- 后续新复杂策略优先消费 `holdings`；
- 旧策略保持零改动运行。

是否需要把 `initial_positions` 标记 legacy / compatibility，应由审计决定，不作为本任务 DoD。

---

## 8. Portfolio Target 与现有 Adapter 的兼容关系

Task07-A 必须避免“双路径结果不一致”。

建议冻结以下优先级：

```text
若 StrategyRunResult.portfolio_targets 非空：
    Portfolio Product / Backtest 直接消费 strategy-native targets
    不再从 decisions 二次推导 target

若 portfolio_targets 为空：
    保持现有 behavior
    StrategyDecision -> strategy_decisions_to_target_weights()
```

即：

```text
complex strategy
→ native portfolio target

simple / legacy strategy
→ decisions
→ existing adapter
→ portfolio target
```

禁止：

```text
同一 run 同时生成 native target
又根据 decisions 生成第二份 target
再通过隐式规则决定听谁的
```

若需要一致性审计，可以在测试中比较，但生产路径必须只有一个 authority。

---

## 9. Task07-A 不负责的业务规则

以下内容明确属于 07-B / 07-C，不在 07-A 实现：

### 07-B

- Task05 authorization 如何约束新增仓；
- Theme Rank 如何参与候选选择；
- Asset Rank 如何参与候选 / 已有持仓比较；
- 新候选相对现有持仓评分门槛；
- 已有持仓每日重评；
- 身份变化不得机械触发卖出；
- 两个连续实际交易日收盘低于 MA5 的退出；
- 第二次加仓资格；
- 监管严重异动等业务判断。

### 07-C

- 单次 1/8；
- 单票最多两次；
- 计划 25%；
- 单票不超过 30%；
- 最多 6 只不同股票；
- 组合容量竞争；
- 退出释放容量；
- 同日 exit / add 的策略级确定顺序；
- 最终完整 target snapshot。

07-A 只提供这些规则所需要的标准接口，不预实现规则。

---

## 10. Execution / Backtest 边界

以下问题不得因 Task07-A 被错误提升为 Strategy Common：

- 实际是否成交；
- 涨跌停无法成交；
- 停牌；
- T+1；
- 委托与成交先后；
- 部分成交；
- 滑点；
- 手续费；
- broker quantity rounding；
- OMS 幂等与恢复。

这些属于通用 Backtest / Future Execution Extension。

### 10.1 “整数手 / 现金不足”特别说明

原 Task07 路线图要求相关语义必须 deterministic，但这不意味着全部进入 Strategy Target Contract。

07-A 默认冻结：

```text
Strategy target = desired business portfolio state
Realized portfolio = downstream price / lot / cash constraint resolution
```

07-C 设计时必须进一步确认：

- 哪些“现金不足”属于 System B 的**组合容量选择**；
- 哪些属于 Portfolio Engine 的**现实成交可实现性**。

不得在 07-A 提前混合两层。

---

## 11. 数据、PIT 与可重放要求

Task07-A 本身不实现 Task08 Replay，但新增 contract 必须从一开始满足 Replay 前提：

- target 结果包含稳定 `trade_date`；
- strategy code/version 明确；
- serialization deterministic；
- evidence 不依赖运行时对象地址、随机顺序或数据库 handle；
- holdings 输入必须可由历史时点快照重建；
- 不允许策略 `run()` 内部主动查询“当前数据库最新状态”；
- 同一 prepared input + parameters + holdings 必须得到同一 portfolio target。

即：

```text
Strategy = pure deterministic decision component
```

继续保持 QRP 已有 StrategyProtocol 的数据库隔离原则。

---

## 12. Validation

至少需要增加：

### 12.1 Portfolio Target contract validation

检查：

- trade_date 合法；
- strategy code/version 与 result definition 一致；
- position asset_id 非空且唯一；
- target_weight finite；
- target_weight >= 0；
- target sum <= 1 + tolerance；
- deterministic position ordering；
- evidence 可序列化；
- 不包含重复 target date。

### 12.2 Holding State validation

检查：

- key 与 `asset_id` 一致；
- current_weight finite 且非负；
- entry_count 为非负整数；
- 日期 canonical；
- bool `initial_positions` 与 typed holdings 同时存在时无矛盾。

---

## 13. 测试要求

Task07-A 至少覆盖：

1. 旧 built-in strategy 在不提供 holdings / targets 时行为不变；
2. declarative strategy 行为不变；
3. `StrategyRunResult.to_dict()` 对新增 targets 稳定序列化；
4. native target 可被 Portfolio 产品路径直接消费；
5. native target 存在时不重复执行 decisions-to-target adapter；
6. native target 为空时旧 adapter 路径保持不变；
7. invalid duplicate asset target fail closed；
8. total target weight > 1 fail closed；
9. negative / NaN / inf target fail closed；
10. typed holdings validation；
11. holdings + legacy initial_positions 冲突 fail closed；
12. 同输入重复 run 输出完全一致。

完成 targeted tests 后运行全量 regression。

---

## 14. 允许修改的典型区域

实际文件由实现 Agent 基于代码审计确定，但预期主要落在：

```text
src/qrp_atlas/strategies/models.py
src/qrp_atlas/strategies/validation.py
src/qrp_atlas/backtest/portfolio/strategy.py
src/qrp_atlas/backtest/product/service.py
相关 __init__.py
相关 tests
```

必要时可以新增一个很薄的 common target adapter / validator 模块。

不得为了目录“更漂亮”进行大范围搬迁。

---

## 15. 明确禁止的 Scope Creep

Task07-A 禁止实现：

- Strategy Framework v2 重构；
- dynamic plugin loader；
- external strategy RPC / protocol；
- `PLUGIN / EXTERNAL` runtime；
- 多账户模型；
- Broker Position Model；
- OMS；
- order plan；
- execution extension；
- 任意资产类别的通用 portfolio domain；
- 多币种；
- margin / short；
- full account ledger；
- System B 07-B / 07-C 业务规则；
- Task08 replay orchestration；
- Task09 production orchestration。

如实现过程中发现“顺手可以做”，默认结论是：**不做，除非它阻塞本任务 DoD。**

---

## 16. DoD

Task07-A 完成条件：

1. `StrategyRunResult` 能正式表达 typed、完整 Portfolio Target；
2. System B 后续 Task07-B/C 所需的最小 typed holding state 已有稳定输入边界；
3. native target 与 legacy decisions-to-target 路径存在唯一、确定的 authority 规则；
4. 现有 built-in / declarative strategy 默认行为不变；
5. 通用 Portfolio / Backtest 无 System B 业务知识；
6. 没有引入 Execution / OMS / broker scope；
7. targeted tests 通过；
8. full regression 通过；
9. 文档与代码边界一致；
10. 对抗审计无 BLOCKER / MAJOR 后才允许进入实现。

---

## 17. 对抗审计重点

审计 Agent 不要评价“设计是否漂亮”，而要主动攻击以下问题：

1. **是否真的需要新增 native Portfolio Target，还是现有 Decision Adapter 足够？** 如果认为足够，必须用 System B Task07 具体规则证明它不会把业务语义泄漏到通用 Adapter。
2. `target_weight` 是否应该是唯一 Strategy Target authority？是否存在原规则必须 target_quantity 的证据？
3. typed holdings 的最小字段是否过多或不足？
4. 是否存在把 Portfolio Engine / Backtest responsibility 错塞回 Strategy 的地方？
5. legacy `initial_positions` 与新 holdings 如何兼容最安全？
6. native target 与 decision-derived target 是否可能出现双 SSOT？
7. declarative strategy / existing built-ins 是否会被破坏？
8. 是否有 PIT / replay determinism 隐患？
9. 是否有未经业务需求证明的抽象与 scope creep？
10. 设计是否真正为 07-B / 07-C 提供足够接口，又没有提前实现它们？

审计输出按：

```text
BLOCKER
MAJOR
MINOR
NIT
```

分级；每项必须给：

- evidence；
- impact；
- recommended minimal fix。

没有证据的问题不要脑补。

---

## 18. Task07-A 完成后的架构结果

Task07-A 完成不代表 QRP 策略挂载闭环完成。

它只意味着：

```text
QRP Strategy Runtime
已能承载一个复杂策略原生输出完整 Portfolio Target
```

真正的验证要等：

```text
07-A contract integration
        ↓
07-B System B business policy
        ↓
07-C final constrained target
        ↓
System B 业务闭环
+
QRP 复杂策略挂载路径第一次完整验证
```

其中“QRP 复杂策略挂载能力闭环”是 Task07 完成后的**架构成果**，不是 Task07 额外建设一个独立平台的目标。
