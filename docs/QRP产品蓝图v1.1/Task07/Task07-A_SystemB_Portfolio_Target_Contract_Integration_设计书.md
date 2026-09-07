# Task07-A — System B Portfolio Target Contract Integration 设计书

> 状态：DESIGN REVISION 1 / 待二次对抗审计
>
> 分支基线：`develop/v1.1`
>
> 任务身份：Task07 的薄 enabling work package。Task07 仍以 **System B 业务闭环** 为唯一主目标；本任务只补齐 System B 为输出完整目标组合而实际暴露出的最小 QRP Common 能力缺口。
>
> Revision 1：吸收首轮对抗审计 `NEEDS_REVISION` 结论，冻结完整快照、统一结果校验、native target 唯一转换入口、holdings as-of 语义、legacy runtime fail-closed 与 deterministic serialization 等合同语义；不扩大 Task07-A 产品范围。

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

Task05 已完成 System B 新增仓授权结果的正式挂载；Task06 已完成 Asset Rank 与 Theme Rank。进入 Task07 后，System B 首次需要把市场判断、横截面排名、当前持仓状态与组合规则汇合成一个**完整目标组合**。

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

但对于 System B 这类复杂策略，当前仍存在关键断点：

```text
StrategyRunResult
  ├─ decisions
  └─ authorizations

        ↓  缺少策略原生、typed、完整的 portfolio target 结果

Portfolio / Backtest
```

简单 ENTER/HOLD/EXIT 策略可以由下游 Adapter 推导 target；System B 的最终组合包含持仓生命周期、加仓次数、容量竞争、权重约束等业务语义，因此 **Portfolio Target 本身属于 System B 策略结果的一部分**，不能依赖通用 Adapter 在策略外部猜测。

Task07-A 的作用仅是为后续 Task07-B / 07-C 提供最小、稳定的输入输出边界。

---

## 2. Task07 总原则（冻结）

### 2.1 唯一主目标

> **Task07 以 System B 业务闭环为唯一主目标；为实现该目标暴露出的 QRP Common 能力缺口，仅进行最小、通用、向后兼容扩展，不将 Task07 扩张为独立的 Strategy Framework 重构任务。**

该原则对 Task07-A / B / C 全部生效。

### 2.2 “被动补齐 Common”，不是“主动建设平台”

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

1. **最小**：只覆盖 Task07 已真实需要的语义；
2. **通用**：命名与类型不得硬编码 System B 业务知识；
3. **向后兼容**：现有 built-in / declarative strategy、现有 `StrategyDecision` 与现有回测产品路径默认不受破坏。

### 2.4 策略语义与执行语义严格分层

Task07 的稳定边界停在：

```text
strategy result / desired portfolio target
```

不得重新把已退出 v1.1 Core 的 Execution / OMS / broker order planning 拉回 Task07。

通用 Portfolio / Backtest 负责模拟或解析：

- T+1；
- 停牌；
- 涨跌停；
- 整数手；
- 成交成本；
- 现实成交失败；
- 价格相关现金可实现性；
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

---

## 4. Task07-A 目标

Task07-A 仅完成：

1. 给 QRP Strategy Result 增加可表达**完整目标组合**的 typed contract；
2. 给后续 System B 07-B / 07-C 提供**最小 typed initial holding state**；
3. 建立 `StrategyRunResult -> portfolio target frame` 的唯一 Common 路由，避免 native target 与 legacy decisions 双 SSOT；
4. 建立统一 `validate_strategy_result()` fail-closed 边界；
5. 保持现有简单策略、declarative strategy 与旧 decisions 路径默认行为不变；
6. 不提前实现 System B entry / hold / exit / sizing / constraint policy；
7. 不为了 seeded holdings 改造现有 PortfolioBacktestEngine / Account 模型。

Task07-A 完成后，后续 System B 策略可以在统一 `StrategyRunResult` 中原生返回完整 Portfolio Target。

---

## 5. 现有能力基线

### 5.1 Strategy Common

当前 `StrategyInput`：

```text
prepared_data
parameters
initial_positions: Mapping[str, bool]
runtime_context
```

当前 `StrategyRunResult`：

```text
definition
parameters
decisions
authorizations
diagnostics
```

### 5.2 Portfolio / Backtest

当前已有：

```text
StrategyDecision
→ strategy_decisions_to_target_weights()
→ target_weight snapshots
→ PortfolioBacktestEngine
```

支持 rank / score priority、max positions、max weight、equal weight、cash buffer、zero target 与 full target snapshots。

Task07-A **不得重新实现 Portfolio Engine**。允许新增的只是：

- native target 的结构化转换；
- native/legacy 两条输入路径的唯一选择路由；
- 统一结果校验。

---

## 6. 最小 Common Contract（冻结）

### 6.1 Portfolio Target 是 StrategyRunResult 一等结果

新增公共类型：

```python
@dataclass(frozen=True)
class StrategyPortfolioTargetPosition:
    asset_id: str
    target_weight: float
    reason_code: str | None = None
    evidence: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]: ...


@dataclass(frozen=True)
class StrategyPortfolioTarget:
    trade_date: str
    strategy_code: str
    strategy_version: str
    positions: tuple[StrategyPortfolioTargetPosition, ...]
    diagnostics: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]: ...
```

`StrategyRunResult` 增加：

```python
portfolio_targets: tuple[StrategyPortfolioTarget, ...] = ()
```

公共导出必须从 `qrp_atlas.strategies` 暴露，调用方不得依赖内部模块路径。

### 6.2 Full Snapshot 唯一语义

首轮审计 B-1 后冻结：

> **每个 `StrategyPortfolioTarget` 都是该 `trade_date` 的完整 desired portfolio state，不是增量 patch。**

唯一解释：

- `positions` 中列出的资产具有对应正/零目标权重；
- **未出现在 `positions` 的任何当前持仓资产，其目标权重语义均为 0**；
- `positions=()` 明确表示该目标日 desired portfolio 为**全现金**；
- Adapter / persistence / replay 不得把省略资产解释为“保持原仓位”；
- 不再保留“退出资产是否显式输出 0 由兼容层决定”的开放语义。

为了 canonical snapshot 最小化，策略不要求为所有已退出资产显式保留 `target_weight=0` 行；**省略即 0** 是合同语义。转换成现有 Engine target frame 时，如执行引擎需要显式 zero row，由转换层基于前一完整 snapshot 确定性补齐；这只是结构转换，不改变业务含义。

### 6.3 target_weight 是唯一 Strategy Target authority

冻结：

> **v1.1 Strategy Portfolio Target 只以 `target_weight` 为业务权威，不引入并列权威 `target_quantity`。**

要求：

- finite；
- `0 <= target_weight <= 1`；
- 单 target 总权重 `<= 1 + tolerance`；
- residual 是 cash；
- quantity / integer lot / price-dependent feasibility 下沉 Portfolio / Backtest。

### 6.4 Date 语义冻结

`StrategyPortfolioTarget.trade_date` 是：

> **策略目标/信号日期（strategy target date），不是最终成交执行日期。**

Canonical 规则：

- 只接受 date-only `YYYY-MM-DD`；
- 不接受带时间部分或 timezone 的 target date；
- 同一 `StrategyRunResult` 内 target date 唯一；
- target date ASC 排序。

**native target 转换层绝不做日期平移。**

每条产品/回测调用链继续由其既有 timing 层负责 signal/target date → execution date，且只能发生一次。特别是 Product Service 已有 execution-date shift 时，native target 进入该路径后不得再次 next-day shift。

### 6.5 Strategy Target / Decision / Authorization 职责

```text
StrategyAuthorization
= 市场级 / 策略级许可或否决

StrategyDecision
= 资产级判断与解释

StrategyPortfolioTarget
= 组合级最终 desired state
```

三者允许同时存在用于解释，但 **portfolio authority 只有一个**：

- `portfolio_targets` 非空 → target authority = native targets；
- `portfolio_targets` 为空 → target authority = legacy decisions adapter。

`decisions` 在 native target run 中只能承担 explanation / audit，不得再次生成第二份生产 target。

---

## 7. Deterministic Serialization（冻结）

### 7.1 Canonical ordering

固定：

```text
portfolio_targets: trade_date ASC
positions within target: asset_id ASC
```

禁止按输入顺序、权重或 reason 排序。

### 7.2 新 target evidence 类型域

为避免破坏旧 StrategyDecision evidence，07-A **只对新增 Portfolio Target evidence 收紧合同**，不追溯重构所有 legacy evidence。

`StrategyPortfolioTargetPosition.evidence` 只允许 JSON-compatible tree：

- `null`；
- bool；
- string；
- integer；
- finite float；
- list / tuple（序列化为 JSON list）；
- string-keyed Mapping，递归校验。

明确拒绝：

- NaN / `+/-Inf`；
- set / frozenset；
- pandas / NumPy / datetime 等未显式转换的运行时对象；
- 非字符串 Mapping key；
- 任意不可 JSON 序列化对象。

Mapping canonical serialization 递归按 key 排序。

### 7.3 to_dict / canonical JSON

新增两个 dataclass 必须提供稳定 `to_dict()`；`StrategyRunResult.to_dict()` 必须包含 `portfolio_targets`。

Canonical JSON 采用：

```text
UTF-8
sort_keys=True
allow_nan=False
stable separators
```

是否复用现有 `deterministic_json()` 实现由实现审计决定；不得建立业务语义重复的第二套序列化规则。

---

## 8. 最小 typed holdings 输入（冻结）

### 8.1 为什么 bool 不足

System B 后续至少需要区分：

```text
未持有
当前持有，已完成一次建仓
当前持有，已发生多次建仓/加仓
```

`Mapping[str, bool]` 无法表达 entry count 与必要日期。

### 8.2 最小类型

新增：

```python
@dataclass(frozen=True)
class StrategyHoldingState:
    asset_id: str
    current_weight: float
    entry_count: int
    first_entry_date: str | None = None
    last_entry_date: str | None = None

    def to_dict(self) -> dict[str, Any]: ...
```

`StrategyInput` 增加：

```python
holdings: Mapping[str, StrategyHoldingState] = field(default_factory=dict)
holdings_as_of_date: str | None = None
```

公共类型从 `qrp_atlas.strategies` 导出。

### 8.3 holdings 唯一时间语义

首轮审计 M-2 后冻结：

> **`holdings` 是在本次 `prepared_data` 第一个交易日开始评估之前的初始持仓快照。**

规则：

- `holdings` 非空时，`holdings_as_of_date` 必填；
- `holdings_as_of_date` 只接受 `YYYY-MM-DD`；
- `holdings_as_of_date` 必须严格早于 `prepared_data` 的最早 `trade_date`；
- Common validator 不负责判断它是否为“上一合法交易日”，因为 Strategy Common 不查询交易日历；历史/生产输入准备层负责提供正确 PIT snapshot；
- 策略在多日 `prepared_data` 内部如需要推进 holding state，由策略自身的确定性业务逻辑按日期顺序推进；07-A 不设计 date-keyed account ledger。

这使 replay 语义固定为：

```text
(initial holdings as-of D-1-ish snapshot)
+ prepared facts from D...
+ parameters
→ deterministic strategy results/targets
```

### 8.4 holdings 只包含“当前持仓”

为避免零仓历史状态歧义：

- `holdings` Mapping 中出现的资产必须 `current_weight > 0`；
- `entry_count >= 1`；
- 未出现的 asset 即当前未持有；
- 退出后的历史 entry count 不通过 `holdings` 保存；若未来业务需要独立历史 position lifecycle，应另行有证据设计，不在 07-A 提前建设。

日期约束：

- `first_entry_date` / `last_entry_date` 若非空，必须为 `YYYY-MM-DD`；
- 日期不得晚于 `holdings_as_of_date`；
- 两者都存在时 `first_entry_date <= last_entry_date`。

### 8.5 与 legacy initial_positions 的精确兼容规则

保留：

```python
initial_positions: Mapping[str, bool]
```

兼容规则：

1. `holdings` 为空：完全沿用 legacy `initial_positions` 行为；
2. `holdings` 非空、`initial_positions` 为空：新复杂策略消费 typed holdings；
3. 两者均非空：对 key union 做严格一致性校验：

```text
legacy_held = initial_positions.get(asset_id, False)
typed_held  = asset_id in holdings   # holdings 内均要求 current_weight > 0

legacy_held must equal typed_held
```

4. `initial_positions` 缺失 key 按 `False`；
5. 任一冲突在**策略执行前** fail-closed；
6. 07-A 不移除、不 deprecate `initial_positions`。

---

## 9. 统一 Strategy Result Validation（冻结）

### 9.1 单一 validator

新增唯一公共结果校验入口：

```python
validate_strategy_result(
    definition: StrategyDefinition,
    result: StrategyRunResult,
) -> StrategyRunResult
```

职责仅是验证并 canonicalize 新增/已有结果合同所需的稳定结构，不执行业务策略。

### 9.2 必须校验

至少：

- result definition code/version 与被执行 strategy definition 一致；
- target strategy_code/version 与 result definition 一致；
- target date canonical、唯一、ASC；
- position asset_id 非空、唯一、ASC；
- target_weight finite、范围合法、总和合法；
- target evidence 为 07-A JSON-compatible tree；
- target diagnostics 为稳定字符串序列；
- 不允许 native target contract 违反 full snapshot / deterministic 规则。

### 9.3 唯一执行边界

所有 **QRP framework-owned** `strategy.run()` 调用点，必须在任何 Adapter、持久化或 Engine 之前立即调用 `validate_strategy_result()`。

至少包括：

- `StrategyRegistry.run()`；
- `StrategyBacktestRuntime.run()`；
- `run_strategy_portfolio_backtest()`；
- Product Service 中直接调用 strategy instance 的路径；
- 未来 Task07-B/C 新增的 QRP-owned runner。

不要求通过 Python Protocol 技术上拦截外部调用者直接执行任意对象的 `.run()`；但 QRP 自己的正式运行路径不得绕过 validator。

非法 result 一律 fail-closed，不允许依赖下游“碰巧报错”。

---

## 10. Strategy Result → Target Frame 唯一路由（冻结）

### 10.1 唯一 Common 入口

新增/收敛一个唯一入口，例如：

```python
strategy_result_to_target_weights(
    strategy_result: StrategyRunResult,
    *,
    legacy portfolio args...
) -> pd.DataFrame
```

该函数先假定 result 已统一校验；必要时可 defensive validate，但不得形成第二套规则。

唯一分支：

```text
if strategy_result.portfolio_targets:
    convert native full snapshots → canonical target frame
else:
    call existing strategy_decisions_to_target_weights(...)
```

现有 `strategy_decisions_to_target_weights()` 保留为 legacy decisions 专用实现，不再作为 Product / Portfolio 的最高层入口。

### 10.2 native target → Engine frame 字段

转换后的 Engine-facing frame 继续使用现有最小结构：

```text
trade_date
asset_id
target_weight
priority
```

规则：

- `trade_date` 原样保持 strategy target/signal date；
- `asset_id` ASC；
- `target_weight` 直接来自 native target；
- native target 不依赖 `priority` 再做容量决策；容量竞争已属于 07-C 策略输出，因此 `priority` 只能使用稳定 neutral value（如 `0.0`）满足现有 Engine frame schema；
- rich `reason_code` / `evidence` / `diagnostics` **不塞进 Engine frame**，它们保留在 canonical `StrategyRunResult` 中。

### 10.3 full snapshot 到显式 zero rows

如果现有 Portfolio Engine 对目标日期“省略资产=0”已有同样语义，则转换层不得重复猜测。

若某调用链需要显式 zero rows，必须仅依据：

```text
previous full target snapshot - current positions
```

确定性补 0；不得从 `StrategyDecision` 或外部 current holdings 推导第二份业务意图。

### 10.4 禁止双 SSOT

native targets 非空时：

- 不调用 `strategy_decisions_to_target_weights()`；
- decisions 只作为解释；
- 不比较后再“选择更合理的一份”；
- 不让 Product Service 与 Portfolio helper 分别各写一套 native conversion。

---

## 11. Backtest / Product 接入边界（冻结）

### 11.1 Portfolio Product path

现有 Product Service 当前在 strategy.run() 后无条件走 decisions adapter。07-A 必须改为：

```text
strategy.run()
→ validate_strategy_result()
→ strategy_result_to_target_weights()
→ existing timing shift exactly once
→ PortfolioBacktestEngine
```

native target 与 legacy decisions 最终复用同一个 Portfolio Engine。

### 11.2 Legacy StrategyBacktestRuntime

`StrategyBacktestRuntime` 是 ENTER/HOLD/EXIT trade-level runtime，不是完整 portfolio target runtime。

07-A 采用最小兼容策略：

> **若 validated StrategyRunResult 含 native `portfolio_targets`，Legacy StrategyBacktestRuntime 必须立即抛出明确 unsupported-path 错误，禁止继续遍历空/旧 decisions 并静默返回“无交易”。**

需要 native target 回测的策略必须走 Portfolio Backtest path。

07-A 不把 legacy trade runtime 重构成第二个 Portfolio Engine。

### 11.3 typed holdings 在现有 Portfolio/Product API 的范围

首轮审计 M-6 后明确：

- 07-A **保证 `StrategyInput` 底层正式支持 typed initial holdings**；
- 现有 `PortfolioBacktestEngine` 仍保持 cash-only initial account，不在 07-A 增加 seeded broker holdings；
- `run_strategy_portfolio_backtest()` / Product Service 如没有正式 seeded-holdings execution semantics，**不得静默丢弃调用方提供的非空 holdings**；必须 fail-closed 或保持 API 不暴露该参数；
- System B 07-B/C 的策略单元/集成验证可以直接构造 `StrategyInput(holdings=...)`；
- Task08 历史验证可以从空初始组合开始，让策略按时间轴确定性推进；
- 每日生产如何从真实/人工持仓快照构建 typed holdings 并调用策略，属于 Task09 production orchestration；
- 若 07-B/C 实际证明在 Task07 内就必须由某 QRP-owned runner 接收非空 holdings，则只增加**传递到 StrategyInput 的薄 runner 参数**，仍不得因此改造 Portfolio Engine seeded-account semantics。

这一定义解决“模型存在但被现有产品静默丢弃”的问题，同时避免 07-A scope creep 到 Account/Execution。

---

## 12. Target rich evidence / diagnostics 的审计位置

`StrategyPortfolioTargetPosition.reason_code/evidence` 与 target `diagnostics` 属于 **strategy result facts**，不是 execution facts。

冻结：

- canonical authority 是 `StrategyRunResult.to_dict()` 中的 `portfolio_targets`；
- Engine-facing target frame 是有意的 lossy projection，只用于 portfolio realization；
- `StrategyPortfolioBacktestRun` 等详细运行结果必须继续携带原始 `strategy_result`；
- Product / Replay 若持久化 strategy result snapshot，必须持久化完整 canonical target rich data，不得只保存 Engine frame 后宣称可解释；
- 07-A 不新增 broker/execution 表，也不为该 rich evidence 单独建设数据库 schema；Task08/09 复用既有 result/reproducibility 基础设施时必须保留该 canonical snapshot。

---

## 13. Task07-A 不负责的 System B 业务规则

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

## 14. Execution / Backtest 边界

不得提升为 Strategy Common：

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

特别冻结：

```text
Strategy target = desired business portfolio state
Realized portfolio = downstream price / lot / cash constraint resolution
```

07-C 只处理 System B 业务组合约束；价格、整手和现实成交导致的 realizability 仍属于 Portfolio/Backtest。

---

## 15. PIT / Replay Determinism

新增 contract 必须满足：

- target date = canonical `YYYY-MM-DD`；
- strategy code/version 明确；
- target / position 排序固定；
- target evidence JSON-compatible + canonical；
- holdings 有明确 `holdings_as_of_date`；
- holdings 可由历史时点快照重建；
- strategy `run()` 不主动查询当前数据库最新状态；
- 同一 prepared input + parameters + initial holdings 必须得到同一 result；
- timing shift 只由调用链下游现有 timing 层执行一次。

---

## 16. Validation 细则

### 16.1 StrategyPortfolioTarget

检查：

- `trade_date` exact `YYYY-MM-DD`；
- target dates 唯一、ASC；
- strategy code/version 与 result definition 一致；
- position `asset_id` 非空、唯一、ASC；
- `target_weight` finite 且 `[0,1]`；
- target sum `<= 1 + tolerance`；
- position evidence JSON-compatible；
- diagnostics string-only；
- canonical serialization 可重复。

### 16.2 StrategyHoldingState / StrategyInput

检查：

- Mapping key 与 `state.asset_id` 一致；
- holdings 内 `current_weight` finite 且 `> 0`；
- `entry_count` 为非 bool 正整数；
- holdings 非空时 `holdings_as_of_date` 必填；
- as-of date / entry dates exact `YYYY-MM-DD`；
- `holdings_as_of_date < min(prepared_data.trade_date)`；
- entry dates 不晚于 as-of；
- first <= last；
- legacy initial_positions 与 holdings key union 严格一致。

冲突必须在 strategy execution 前 fail-closed。

---

## 17. 测试要求

Task07-A 至少覆盖：

1. 旧 built-in strategy 无 holdings/targets 时行为不变；
2. declarative strategy 行为不变；
3. 新 target types 公共导出稳定；
4. `StrategyRunResult.to_dict()` 包含 canonical targets；
5. `positions=()` 表示全现金；
6. omitted asset 的完整快照语义被唯一转换，不存在 patch interpretation；
7. duplicate asset / duplicate target date fail-closed；
8. target sum > 1 fail-closed；
9. negative / NaN / inf target fail-closed；
10. target evidence 非 JSON 类型 fail-closed；
11. target ordering 固定为 date ASC + asset ASC；
12. strategy code/version mismatch fail-closed；
13. QRP-owned runners 在 Adapter/Engine 前统一调用 result validator；
14. native target 存在时最高层路由不调用 decisions adapter；
15. native target 为空时 legacy decisions path 行为不变；
16. Product path native target 只发生一次 execution-date shift；
17. Legacy StrategyBacktestRuntime 遇到 native target 明确 fail，而不是空交易成功；
18. holdings as-of 与字段 validation；
19. holdings + initial_positions key union conflict fail-closed；
20. 现有 Portfolio/Product 路径不得静默丢弃非空 holdings；
21. 同输入重复 run / serialization 输出一致。

完成 targeted tests 后运行全量 regression。

---

## 18. 允许修改的典型区域

预期主要落在：

```text
src/qrp_atlas/strategies/models.py
src/qrp_atlas/strategies/validation.py
src/qrp_atlas/strategies/__init__.py
src/qrp_atlas/strategies/registry.py
src/qrp_atlas/backtest/runtime/strategy.py
src/qrp_atlas/backtest/portfolio/strategy.py
src/qrp_atlas/backtest/product/service.py
相关 tests
```

允许新增一个很薄的 common result→target adapter / canonical serialization helper；不得为了目录整洁大范围搬迁。

---

## 19. 明确禁止的 Scope Creep

Task07-A 禁止实现：

- Strategy Framework v2 重构；
- dynamic plugin loader；
- external strategy RPC / protocol；
- `PLUGIN / EXTERNAL` runtime；
- 多账户模型；
- Broker Position Model；
- seeded broker account / full account ledger；
- OMS；
- order plan；
- execution extension；
- 任意资产类别的通用 portfolio domain；
- 多币种；
- margin / short；
- System B 07-B / 07-C 业务规则；
- Task08 replay orchestration；
- Task09 production orchestration。

如实现过程中发现“顺手可以做”，默认结论是：**不做，除非它阻塞本任务 DoD。**

---

## 20. DoD

Task07-A 完成条件：

1. `StrategyRunResult` 能表达 typed、完整、full-snapshot Portfolio Target；
2. `positions=()` / omitted asset / residual cash 语义唯一；
3. target date、排序、serialization 已 canonical；
4. System B 后续 07-B/C 所需最小 typed initial holding state 有明确 as-of 边界；
5. legacy `initial_positions` 兼容规则确定且 fail-closed；
6. 有唯一 `validate_strategy_result()`，QRP-owned run paths 不绕过；
7. 有唯一 `StrategyRunResult -> target frame` 最高层路由；
8. native target 与 legacy decisions target 不形成双 SSOT；
9. Product Portfolio path 可消费 native target，且日期 shift 只发生一次；
10. legacy StrategyBacktestRuntime 不会静默忽略 native target；
11. 非空 holdings 不会被 QRP-owned wrapper 静默丢弃；
12. 现有 built-in / declarative strategy 默认行为不变；
13. 通用 Portfolio / Backtest 无 System B 业务知识；
14. 未引入 Execution / OMS / Broker Account scope；
15. targeted tests + full regression 通过；
16. 二次对抗审计无 BLOCKER / MAJOR 后才允许进入实现。

---

## 21. 首轮对抗审计处置记录

### BLOCKER

- **B-1 完整快照与省略资产语义未冻结** → 已冻结 full snapshot：omitted asset = 0，`positions=()` = all cash。
- **B-2 结果校验无统一执行入口** → 已冻结唯一 `validate_strategy_result()` 与所有 QRP-owned `strategy.run()` 后立即校验。

### MAJOR

- **M-1 native target 转换 API / 日期语义未定义** → 已冻结唯一 `strategy_result_to_target_weights()` 路由；target date 为 strategy signal/target date；converter 不平移日期。
- **M-2 holdings 缺 as-of 生命周期语义** → 已增加 `holdings_as_of_date`，定义为首个 prepared trade date 之前的 initial snapshot。
- **M-3 initial_positions / holdings 冲突规则不完整** → 已冻结 key union bool 等价规则；holdings 只包含 current held assets。
- **M-4 deterministic serialization 类型域未定义** → 已限定新增 target evidence JSON-compatible tree + canonical ordering/JSON。
- **M-5 legacy StrategyBacktestRuntime 静默忽略 native target** → 已冻结 fail-fast unsupported-path。
- **M-6 Portfolio/Product API 无法传入 holdings** → 已明确 07-A 底层 StrategyInput 支持；现有 cash-only Portfolio Engine 不扩 seeded account；任何 QRP-owned wrapper 禁止静默丢弃非空 holdings。

### MINOR / NIT

- 排序键 → date ASC / asset_id ASC；
- 日期 → exact `YYYY-MM-DD`；
- rich target evidence/diagnostics → canonical StrategyRunResult authority，Engine frame 仅 lossy projection；
- 新 dataclass → public export + `to_dict()`；
- version consistency → 统一 result validator 在任何 Adapter/persistence/Engine 前校验。

---

## 22. Task07-A 完成后的架构结果

Task07-A 完成不代表 QRP 策略挂载闭环完成。

它只意味着：

```text
QRP Strategy Runtime
已能承载一个复杂策略原生输出完整 Portfolio Target
```

真正的闭环仍是：

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

“QRP 复杂策略挂载能力闭环”是 Task07 完成后的**架构成果**，不是 Task07 额外建设独立平台的目标。
